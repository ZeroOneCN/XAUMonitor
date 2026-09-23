#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Binance PAXG/USDT 实时行情旁路采集器

用途
----
独立于现有监控（DPB 信号系统）运行的「旁路」采集进程，只负责采集，
不参与任何信号计算，不影响也不依赖 dpb_monitor.py。

目标：持续采集 Binance PAXG/USDT（1 盎司实物黄金背书代币，与 XAU/USD
实测价差约 0.03%）的实时成交与 1 分钟 K 线，用于：

  1. 与 Twelve Data 轮询数据做对比，评估是否值得切换数据源；
  2. 为「盘中价格触发」类逻辑做数据准备。

数据落库：SQLite（独立库 paxg_stream.db，WAL 模式，可并发读）

用法
----
  python paxg_collector.py                 # 常驻采集
  python paxg_collector.py --symbol paxgusdt
  python paxg_collector.py --db /path/to.db
"""
import argparse
import asyncio
import csv
import gzip
import json
import logging
import signal
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import websockets
import requests

# ============================================================
# 配置
# ============================================================
BASE = Path(__file__).parent
DEFAULT_SYMBOL = "paxgusdt"                 # Binance 交易对（小写）
DEFAULT_DB = BASE / "paxg_stream.db"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="
CST = timezone(timedelta(hours=8))          # 北京时间

TRADE_FLUSH_ROWS = 200                      # 成交攒够多少行落库
TRADE_FLUSH_SECS = 5                         # 或最多等多少秒
STAT_INTERVAL = 60                           # 统计日志间隔（秒）
PING_INTERVAL = 20                           # 心跳（Binance 20 分钟发 ping，这里主动保活）
PING_TIMEOUT = 60

# 做单数据推送
CONFIG_FILE = BASE / "dpb_config.json"       # 复用监控的配置(取 webhook)
SIGNALS_DB = BASE / "signals.db"             # 复用监控的信号库
FIRED_FILE = BASE / "paxg_fired.json"        # 已触发价位记录（持久化，避免重启后重发）
LEVEL_RELOAD_SECS = 60                       # 重新加载监控价位的间隔
MAINTAIN_SECS = 86400                        # 数据库归档自维护间隔（每日一次）
WAL_TRUNCATE_SECS = 3600                     # WAL 收缩间隔（每小时）

# 为什么需要单独做 WAL 收缩：
# SQLite 的 wal_autocheckpoint 是 PASSIVE 模式 —— 它把已提交页写回主库，
# 但**从不收缩 WAL 文件本身**。采集器每秒写成交，WAL 会一路涨到历史最高水位
# 并停在那里（实测涨到 10.4MB，与主库同大），既占磁盘又拖慢崩溃恢复。
# PASSIVE 之外只有 TRUNCATE/RESTART 会把文件截回 0，必须显式、周期性执行。
# PAXG↔XAU 价差修正（见 _paxg_basis 说明：两个市场的价格不能直接比）
BASIS_TTL_SEC = 600                          # 价差基准缓存 10 分钟（价差移动很慢，省额度）
_basis_cache = {"t": 0.0, "key": "", "val": None}
LEVEL_MAX_SIGNALS = 20                       # 最多盯最近多少条信号
DEFAULT_ALERT_MAX_AGE_H = 24                 # 信号超过多少小时不再盯（兜底）

# 各周期信号的「提醒有效期」（分钟）。
# 5分钟周期的入场机会几小时后早已完全变味，切成 24 小时一刀切是错的：
# 会拿着 3 小时前、甚至已经止损了的信号去提醒用户。
_LEVEL_TTL_MINUTES = {
    "5m": 60,       # 1 小时
    "15m": 180,     # 3 小时
    "1h": 720,      # 12 小时
    "4h": 2880,     # 2 天
    "1d": 7200,     # 5 天
}


# ------------------------------------------------------------
# 盘中触发的行情复核
# ------------------------------------------------------------
# 为什么必须有：信号是用 Twelve Data 的 XAU/USD 算的，但盘中触发盯的是
# Binance PAXG/USDT —— 两个不同市场，PAXG 流动性差得多，会独立插针。
# 实测事故：17:01:18 报「触发止损 4388.89」，当时 PAXG=4388.78，而真实
# XAU/USD 最低 4392.32，离止损还差 3.24 美元；那一分钟的 PAXG 只有 3 笔成交。
# 结果是用户收到假止损提醒（可能因此平掉好单）。
# 【注意】曾试过「PAXG 穿越后要 XAU 也确认」的二元门 —— 那会造成【漏报】：
#   PAXG 因价差先穿越并被跳过，之后它一直在价位下方、不再产生「穿越事件」，
#   真实 XAU 后续到达该位时就永远不会报警。实测 #17 的止损：
#   PAXG 17:00 就跌破 4388.89，XAU 直到 17:28 才到 —— 领先 28 分钟，
#   于是 17:28 的真止损被永久静默。
# 正确解法是连续修正价差（见 _paxg_basis），而不是二元拦截。


def _paxg_basis(cfg: dict, lookback_min: int = 30, db_path=None):
    """PAXG 与真实 XAU/USD 的价差（XAU − PAXG），取近期中位数。

    【为什么必须修正价差，而不是加「确认门」】
    我们两次都栽在这里，是两个反向的错：
      ① 最初直接用 PAXG 价格去比「用 XAU 算出来的止损位」
         → PAXG 系统性低于 XAU（实测常态 ~$2-3，稀薄时插针到 $6+），
           价格跌向止损时 PAXG 永远先穿越 → 误报止损（17:01 那次）。
      ② 我改成「PAXG 穿越后要 XAU 也确认才报」→ 变成漏报：
         PAXG 在 17:00 就跌破 4388.89，XAU 直到 17:28 才到（领先 28 分钟）。
         17:00 那次被正确跳过，但 PAXG 此后一直在价位下方、
         不再产生「穿越事件」→ 17:28 的真止损被永久静默。

    两次的同一个病根都是：**拿不同市场的价格当同一把尺子**。
    正确解法是先把 PAXG 换算成「XAU 等价价」，再比较 —— 连续修正而非二元拦截，
    既不会因价差而提前触发，也不会因「已经穿过」而丢失后续的真实穿越。

    返回价差（美元）；数据不足时返回 None。
    """
    now = time.time()
    key = f"basis:{lookback_min}"
    if (_basis_cache["key"] == key and _basis_cache["val"] is not None
            and now - _basis_cache["t"] < BASIS_TTL_SEC):
        return _basis_cache["val"]
    try:
        import dpb_monitor as mon
        mcfg = mon.load_config()
        xau = mon.fetch_data(mcfg["ticker_td"], "1m", "1min")
        if xau is None or len(xau) < 5:
            return None
    except Exception as e:
        log.warning(f"[做单] 价差基准取 XAU 失败: {e}")
        return None
    db = Path(db_path) if db_path else DEFAULT_DB
    if not db.exists():
        return None
    try:
        with sqlite3.connect(str(db)) as conn:
            rows = conn.execute(
                "SELECT open_ms, close FROM klines_1m WHERE closed = 1"
                " ORDER BY open_ms DESC LIMIT ?", (lookback_min + 5,)).fetchall()
    except Exception:
        return None
    if len(rows) < 5:
        return None
    # 【易错点】必须用「本地时间字符串」对齐，不能用 Timestamp.timestamp()。
    # Twelve Data 返回的索引是【无时区的本地时间】，而 timestamp() 会把它当 UTC，
    # 于是整体错位 8 小时 —— 实测会算出 -26 美元的荒谬价差（拿 8 小时前的 PAXG 配对）。
    # 两边都用 "%Y-%m-%d %H:%M" 字符串做键，天然绕开时区换算。
    paxg = {}
    for ms, c in rows:
        if c:
            paxg[datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M")] = float(c)
    diffs = []
    for ts, row in xau.iterrows():
        try:
            base = ts
            for off in (0, -1, 1):          # 容忍 ±1 分钟的边界偏差
                k = (base + timedelta(minutes=off)).strftime("%Y-%m-%d %H:%M")
                if k in paxg:
                    v = float(row["Close"]) - paxg[k]
                    if -50 < v < 50:        # 明显异常值丢弃
                        diffs.append(v)
                    break
        except Exception:
            continue
    if len(diffs) < 5:
        return None
    diffs.sort()
    basis = diffs[len(diffs) // 2]        # 中位数
    _basis_cache.update({"t": now, "key": key, "val": basis})
    log.info(f"[做单] PAXG 价差基准 = {basis:+.2f} 美元"
             f"（XAU − PAXG，{len(diffs)} 分钟样本，中位数）")
    return basis


def _trigger_dir(kind: str, direction: int) -> int:
    """该价位允许的穿越方向：1=只允许上穿, -1=只允许下穿, 0=双向。

    做多的止盈只可能在「向上」被触及，做多的止损只可能在「向下」被触及。
    不做方向判断的话，价格从上方跌回止盈位也会误报「✅ 触及止盈」。
    """
    if kind == "入场":
        return 0                      # 入场位两侧都可能回到
    if kind == "止损":
        return -direction             # 做多=下穿止损, 做空=上穿止损
    return direction                  # 止盈：做多=上穿, 做空=下穿

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(BASE / "paxg_collector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("PAXG")

# ============================================================
# 数据库
# ============================================================
def init_db(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER,          -- 交易所成交时间(ms)
                recv_ms INTEGER,        -- 本地接收时间(ms)
                latency_ms INTEGER,     -- 本地接收 - 交易所时间
                price REAL, qty REAL,
                is_buyer_maker INTEGER  -- 1=主动卖(买方是挂单), 0=主动买
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_ms)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS klines_1m (
                open_ms INTEGER PRIMARY KEY,
                close_ms INTEGER,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, quote_volume REAL,
                trades INTEGER,
                closed INTEGER,         -- 1=该分钟已收盘
                updated_ms INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT, kind TEXT, detail TEXT
            )
        """)
        conn.commit()


def log_event(db_path: Path, kind: str, detail: str):
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO stream_events (ts, kind, detail) VALUES (?,?,?)",
                (datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"), kind, detail),
            )
            conn.commit()
    except Exception as e:
        log.error(f"[事件] 写入失败: {e}")


# ============================================================
# 做单数据推送：实时盯住 signals.db 里信号的可执行价位
# ============================================================
# ============================================================
# 数据库归档与清理（P2-10）
# ============================================================
# 为什么必须：trades 表约 0.5 条/秒 → 每天约 4 万条、一年 1500 万条。
# 不清理会让库无限膨胀、查询退化、备份变慢、磁盘吃紧。
#
# 策略：只归档「整月且已完全过期」的数据 —— 每个月的数据只会被写入一次文件，
# 重复执行不会重复归档（幂等）。归档为按月分片的 .csv.gz，日后可解压回灌。
# 最后尝试 VACUUM 回收空间（可能因采集器并发写而失败，容忍并记录）。
ARCHIVE_DIR = BASE / "archive"


def _next_month(y: int, m: int) -> datetime:
    """返回下一个月的 1 号（用于判断某月是否已完全过去）"""
    return datetime(y + (1 if m == 12 else 0), 1 if m == 12 else m + 1, 1)


def wal_truncate(db_path=None) -> bool:
    """把 WAL 文件截回 0（PASSIVE 之外的唯一办法）。

    注意：TRUNCATE 需要拿到写锁；若此刻主连接正在写入会返回 busy，
    此时**不能重试**（采集器每秒都在写），下一轮再来即可 —— 所以失败只记
    调试日志，绝不影响采集。返回 True 表示真正收缩了。
    """
    db = Path(db_path) if db_path else DEFAULT_DB
    try:
        with sqlite3.connect(str(db), timeout=5) as conn:
            busy, log_pages, ckpt = conn.execute(
                "PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if busy:
            return False
        return True
    except Exception as e:
        log.debug(f"[WAL] 收缩跳过: {e}")
        return False


def maintain_db(cfg: dict, db_path=None, dry_run: bool = False) -> dict:
    """归档 + 清理 paxg_stream.db，返回统计信息。"""
    db = Path(db_path or cfg.get("paxg_db_file", DEFAULT_DB))
    trades_days = float(cfg.get("paxg_trades_retention_days", 30))
    klines_days = float(cfg.get("paxg_klines_retention_days", 180))
    now = datetime.now()
    stat = {"archived": {}, "vacuum": "", "size_before": 0, "size_after": 0}
    if not db.exists():
        return stat
    stat["size_before"] = db.stat().st_size
    try:
        conn = sqlite3.connect(str(db), timeout=30)
        conn.execute("PRAGMA busy_timeout=30000")
    except Exception as e:
        log.error(f"[归档] 打开库失败: {e}")
        return stat
    try:
        for table, tscol, days in (("trades", "ts_ms", trades_days),
                                   ("klines_1m", "open_ms", klines_days)):
            cutoff = now - timedelta(days=days)
            yms = [r[0] for r in conn.execute(
                f"SELECT DISTINCT strftime('%Y-%m', {tscol}/1000, 'unixepoch') AS ym"
                f" FROM {table} ORDER BY ym") if r[0]]
            for ym in yms:
                y, m = int(ym[:4]), int(ym[5:7])
                if _next_month(y, m) > cutoff:
                    continue                      # 该月尚未完全过期 → 下次再归档
                rows = conn.execute(
                    f"SELECT * FROM {table}"
                    f" WHERE strftime('%Y-%m', {tscol}/1000, 'unixepoch') = ?"
                    f" ORDER BY {tscol}", (ym,)).fetchall()
                if not rows:
                    continue
                if dry_run:
                    log.info(f"[归档][试运行] {table} {ym}: {len(rows)} 行（未写入）")
                    continue
                cols = [d[0] for d in conn.execute(
                    f"SELECT * FROM {table} LIMIT 0").description]
                ARCHIVE_DIR.mkdir(exist_ok=True)
                fp = ARCHIVE_DIR / f"{table}_{ym}.csv.gz"
                is_new = not fp.exists()
                with gzip.open(fp, "at", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    if is_new:
                        w.writerow(cols)
                    w.writerows(rows)
                conn.execute(
                    f"DELETE FROM {table}"
                    f" WHERE strftime('%Y-%m', {tscol}/1000, 'unixepoch') = ?", (ym,))
                conn.commit()
                stat["archived"][f"{table}_{ym}"] = len(rows)
                log.info(f"[归档] {table} {ym}: {len(rows)} 行 → {fp.name}")
        if not dry_run:
            try:
                conn.execute("VACUUM")
                stat["vacuum"] = "ok"
            except Exception as e:
                stat["vacuum"] = f"跳过({type(e).__name__}: {e})"
    except Exception as e:
        log.error(f"[归档] 执行失败: {e}")
    finally:
        conn.close()
    try:
        stat["size_after"] = db.stat().st_size
    except Exception:
        pass
    if stat["archived"] or stat["vacuum"]:
        log.info(f"[归档] 完成: 归档 {sum(stat['archived'].values())} 行, "
                 f"库 {stat['size_before']/1e6:.1f}MB → {stat['size_after']/1e6:.1f}MB, "
                 f"vacuum={stat['vacuum']}")
    return stat


def _load_cfg() -> dict:
    try:
        with open(CONFIG_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def _webhooks(cfg: dict) -> list:
    whs = list(cfg.get("wecom_webhooks") or [])
    single = cfg.get("wecom_webhook")
    if single and single not in whs:
        whs.insert(0, single)
    return [w for w in whs if w and "YOUR" not in w]


def send_alert(title: str, content: str) -> bool:
    """推送到所有已配置的告警通道（复用监控的多通道冗余逻辑）"""
    whs = _webhooks(_load_cfg())
    if not whs:
        log.error("[做单] 未配置任何告警 webhook")
        return False
    ok = False
    for wh in whs:
        try:
            payload = {"msgtype": "markdown", "markdown": {"content": f"## {title}\n{content}"}}
            r = requests.post(wh, json=payload, timeout=10)
            r.raise_for_status()
            if r.json().get("errcode") == 0:
                ok = True
        except Exception as e:
            log.error(f"[做单] 推送异常: {e}")
    return ok


class LevelWatcher:
    """盘中实时监控「最近信号」的可执行价位（入场/止损/TP1/TP2）。

    与 4 分钟轮询互补：轮询负责「发信号」，本模块负责「信号发出后，
    价格盘中触及关键位时立刻提醒」，把 WS 的毫秒级精度变成做单价值。
    """

    def __init__(self, enabled: bool = True, max_age_h: float = DEFAULT_ALERT_MAX_AGE_H,
                 confirm_xau: bool = True, confirm_lookback_min: int = 3,
                 db_path=None):
        self.enabled = enabled
        self.db_path = Path(db_path) if db_path else DEFAULT_DB
        self.last_maintain = 0.0        # 0 → 启动后第一轮就做一次自维护，之后每日一次
        self.last_wal_ckpt = 0.0        # WAL 收缩计时
        self.max_age_h = max_age_h
        # 价差修正取不到时的告警只报一次，避免刷屏
        self._basis_warned = False
        # 触发前是否用真实 XAU/USD 修正 PAXG 价差（false = 直接用 PAXG，会有 $2-3 系统性偏差）
        self.confirm_xau = confirm_xau
        self.confirm_lookback_min = confirm_lookback_min
        self.levels = []
        self.fired = self._load_fired()
        self.prev = None
        self.last_reload = 0.0

    # ---------- 已触发记录持久化 ----------
    # 只放在内存里的话，每次重启服务都会把已提醒过的价位重新提醒一遍
    # （实测：重启后 TP1 又被推了一次）。
    def _load_fired(self) -> set:
        try:
            with open(FIRED_FILE, encoding="utf-8") as f:
                return set(json.load(f))
        except Exception:
            return set()

    def _save_fired(self):
        try:
            with open(FIRED_FILE, "w", encoding="utf-8") as f:
                json.dump(sorted(self.fired), f, ensure_ascii=False)
        except Exception as e:
            log.error(f"[做单] 已触发记录落盘失败: {e}")

    def _reload(self):
        if not SIGNALS_DB.exists():
            return
        # 回看窗口取各周期 TTL 的最大值（之后按周期逐个精筛）
        max_ttl_min = max(list(_LEVEL_TTL_MINUTES.values()) + [int(self.max_age_h * 60)])
        cutoff = (datetime.now() - timedelta(minutes=max_ttl_min)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with sqlite3.connect(SIGNALS_DB) as conn:
                rows = conn.execute(
                    "SELECT s.id, s.timeframe, s.direction, s.sig_type, s.grade, s.score,"
                    " s.entry, s.sl, s.tp1, s.tp2, s.pushed_at, o.outcome"
                    " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
                    " WHERE s.pushed_at >= ?"
                    "   AND (o.outcome IS NULL OR o.outcome = 'open')"   # 已了结的不再盯
                    " ORDER BY s.id DESC LIMIT ?",
                    (cutoff, LEVEL_MAX_SIGNALS),
                ).fetchall()
        except Exception as e:
            log.error(f"[做单价位] 读取 signals.db 失败: {e}")
            return

        now = datetime.now()
        lv, seen, dropped = [], set(), []
        for (sid, tf, d, styp, grade, score, entry, sl, tp1, tp2, pushed_at, outcome) in rows:
            # 各周期独立有效期：5分钟周期的信号 3 小时后提醒已毫无意义
            ttl_min = _LEVEL_TTL_MINUTES.get(tf, int(self.max_age_h * 60))
            try:
                age_min = (now - datetime.strptime(pushed_at, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
            except Exception:
                age_min = 0.0
            if age_min > ttl_min:
                dropped.append(f"#{sid}{tf}超期{age_min:.0f}分")
                continue
            if entry is None or sl is None:
                continue

            label = f"{tf} {'做多' if d > 0 else '做空'}"
            if styp:
                label += f" [{styp}]"
            if grade:
                label += f" [{grade}级{score}分]"
            for kind, price in (("入场", entry), ("止损", sl), ("TP1", tp1), ("TP2", tp2)):
                if price is None:
                    continue
                # 同一「类型+价位」只盯一次：哪怕信号表里因重复推送留下多行，
                # 也不会对同一个价位反复触发告警（防刷屏的第二道防线）
                key = (kind, round(float(price), 2))
                if key in seen:
                    continue
                seen.add(key)
                lv.append({
                    "id": f"{kind}:{float(price):.2f}", "kind": kind, "price": float(price),
                    "label": label, "entry": float(entry), "sl": float(sl),
                    "tp1": float(tp1) if tp1 is not None else None,
                    "tp2": float(tp2) if tp2 is not None else None,
                    "direction": int(d), "dir": _trigger_dir(kind, int(d)),
                    "pushed_at": pushed_at,
                })
        self.levels = lv
        # 已触发的记录只保留仍在监控中的价位，避免无限增长
        before = len(self.fired)
        self.fired &= {l["id"] for l in lv}
        if len(self.fired) != before:
            self._save_fired()
        msg = f"[做单价位] {len(rows)} 条未结案信号 → 监控 {len(lv)} 个价位"
        if dropped:
            msg += f" | 按周期过期剔除 {len(dropped)}: {', '.join(dropped[:4])}"
        log.info(msg)

    def check(self, price: float):
        if not self.enabled:
            return
        now = time.time()
        if now - self.last_reload >= LEVEL_RELOAD_SECS:
            self._reload()
            self.last_reload = now
        # 每日自维护：归档过期数据。放在采集器进程内执行 → 不与 WS 写入抢库锁，
        # 比外部 cron 更安全（VACUUM 需要独占，跨进程常因 busy 失败）。
        if now - self.last_maintain >= MAINTAIN_SECS:
            self.last_maintain = now
            try:
                maintain_db(_load_cfg(), self.db_path)
            except Exception as e:
                log.error(f"[归档] 自维护失败: {e}")
        # 每小时收缩一次 WAL，防止它涨到历史最高水位后不再回落
        if now - self.last_wal_ckpt >= WAL_TRUNCATE_SECS:
            self.last_wal_ckpt = now
            wal_truncate(self.db_path)
        if self.prev is None:
            self.prev = price
            return
        # 【关键】把 PAXG 价格换算成「XAU 等价价」后再与信号价位比较。
        # 不修正 → PAXG 因价差提前穿越，误报止损；
        # 改成「要 XAU 确认」的二元门 → PAXG 先穿越后永久静默，漏报止损。
        # 只有连续修正价差，两侧才是同一把尺子（详见 _paxg_basis 说明）。
        basis = 0.0
        if self.confirm_xau and self.levels:      # 没有价位要盯时不必消耗额度
            cfg_now = _load_cfg()
            b = _paxg_basis(cfg_now, int(cfg_now.get("paxg_confirm_lookback_min", 30)),
                               self.db_path)
            if b is None:
                b = float(cfg_now.get("paxg_basis_fallback", 2.0))
                if not self._basis_warned:
                    self._basis_warned = True
                    log.warning(f"[做单] 实时价差取不到，暂用兜底值 {b:+.2f} 美元"
                                f"（恢复正常前可能偏差）")
            basis = b
        p0, p1 = self.prev + basis, price + basis
        for l in self.levels:
            if l["id"] in self.fired:
                continue
            p = l["price"]
            if p0 < p <= p1:
                crossed = 1        # 上穿
            elif p0 > p >= p1:
                crossed = -1       # 下穿
            else:
                continue
            want = l.get("dir", 0)
            if want and crossed != want:
                continue           # 方向不符：做多的止盈不会在「下穿」时被触及
            self.fired.add(l["id"])
            self._save_fired()          # 落盘，重启后不会重发
            self._alert(l, price + basis)
        self.prev = price

    def _alert(self, l: dict, price: float):
        """推送盘中触发提醒 —— 带上完整做单计划（入场/止损/TP1/TP2 + R 倍数），
        否则用户只知道"碰到了某个价"，却不知道剩余目标在哪。"""
        emoji = {"入场": "🎯", "止损": "🛑", "TP1": "✅", "TP2": "🏆"}.get(l["kind"], "🔔")
        title = f"{emoji} 盘中触发 {l['kind']} | {l['label']}"

        e = float(l["entry"])
        sl = float(l["sl"])
        d = int(l.get("direction", 1)) or 1
        r = abs(e - sl) or 1.0     # 1R = 入场到止损的距离

        def rel(p):
            """价格 + 相对入场价的偏移 + R 倍数（正=有利，负=不利）"""
            if p is None:
                return "—"
            diff = (float(p) - e) * d
            return f"**{float(p):.2f}** ({diff:+.2f}, {diff / r:+.2f}R)"

        # 信号距今多久 —— 提醒里显示新鲜度，避免用户以为是刚出的信号
        try:
            age_min = (datetime.now() - datetime.strptime(l["pushed_at"], "%Y-%m-%d %H:%M:%S")).total_seconds() / 60
            age_txt = f"（{age_min:.0f} 分钟前）" if age_min < 120 else f"（{age_min / 60:.1f} 小时前）"
        except Exception:
            age_txt = ""

        lines = [
            f"> 触发价 **{l['price']:.2f}**  |  现价 **{price:.2f}**",
            f"> 🎯 入场 {e:.2f}",
            f"> 🛑 止损 {rel(sl)}",
            f"> ✅ TP1　{rel(l.get('tp1'))}",
            f"> 🏆 TP2　{rel(l.get('tp2'))}",
            f"> 信号时间 {l['pushed_at']}{age_txt}",
        ]
        log.info(f"[做单] 触发 {l['kind']} @ {l['price']:.2f} (现价 {price:.2f})")
        send_alert(title, "\n".join(lines) + "\n")


# ============================================================
# 采集
# ============================================================
class Collector:
    def __init__(self, db_path: Path, symbol: str, watcher: "LevelWatcher" = None):
        self.db = db_path
        self.symbol = symbol
        self.watcher = watcher or LevelWatcher(enabled=False)
        self.trade_buf = []
        self.last_flush = time.time()
        # 统计
        self.stat_ticks = 0
        self.stat_lat_sum = 0
        self.stat_lat_n = 0
        self.stat_klines = 0
        self.stat_start = time.time()
        self.last_price = None
        self.running = True

    # ---------- 落库 ----------
    def flush(self, force: bool = False):
        now = time.time()
        if not force and len(self.trade_buf) < TRADE_FLUSH_ROWS and (now - self.last_flush) < TRADE_FLUSH_SECS:
            return
        if not self.trade_buf:
            self.last_flush = now
            return
        rows, self.trade_buf = self.trade_buf, []
        try:
            with sqlite3.connect(self.db) as conn:
                conn.executemany(
                    "INSERT INTO trades (ts_ms, recv_ms, latency_ms, price, qty, is_buyer_maker)"
                    " VALUES (?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()
        except Exception as e:
            log.error(f"[落库] 成交写入失败({len(rows)}行): {e}")
        self.last_flush = now

    def save_kline(self, k: dict):
        try:
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "INSERT INTO klines_1m (open_ms, close_ms, open, high, low, close, volume,"
                    " quote_volume, trades, closed, updated_ms)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(open_ms) DO UPDATE SET"
                    " high=excluded.high, low=excluded.low, close=excluded.close,"
                    " volume=excluded.volume, quote_volume=excluded.quote_volume,"
                    " trades=excluded.trades, closed=excluded.closed, updated_ms=excluded.updated_ms",
                    (
                        k["t"], k["T"],
                        float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
                        float(k["v"]), float(k.get("q", 0) or 0),
                        int(k.get("n", 0)), 1 if k.get("x") else 0,
                        int(time.time() * 1000),
                    ),
                )
                conn.commit()
            self.stat_klines += 1
        except Exception as e:
            log.error(f"[落库] K线写入失败: {e}")

    # ---------- 消息处理 ----------
    def on_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        recv_ms = int(time.time() * 1000)

        if stream.endswith("@trade"):
            ts = int(data.get("T", 0))
            price = float(data.get("p", 0))
            self.trade_buf.append((
                ts, recv_ms, recv_ms - ts, price,
                float(data.get("q", 0)), 1 if data.get("m") else 0,
            ))
            self.stat_ticks += 1
            self.stat_lat_sum += recv_ms - ts
            self.stat_lat_n += 1
            self.last_price = price
            self.flush()
            # 做单数据：实时检查是否触及信号价位
            self.watcher.check(price)

        elif stream.endswith("@kline_1m"):
            k = data.get("k", {})
            if k:
                self.save_kline(k)
                self.last_price = float(k.get("c", 0)) or self.last_price

    # ---------- 统计日志 ----------
    def log_stats(self):
        elapsed = time.time() - self.stat_start
        tpm = self.stat_ticks * 60 / elapsed if elapsed else 0
        avg_lat = (self.stat_lat_sum / self.stat_lat_n) if self.stat_lat_n else 0
        log.info(
            f"[统计] 近{elapsed:.0f}s: 成交{self.stat_ticks}笔({tpm:.0f}/分) "
            f"K线更新{self.stat_klines}次 平均延迟{avg_lat:.0f}ms 最新价={self.last_price}"
        )
        self.stat_ticks = self.stat_lat_sum = self.stat_lat_n = self.stat_klines = 0
        self.stat_start = time.time()

    # ---------- 主循环 ----------
    async def run(self):
        streams = f"{self.symbol}@trade/{self.symbol}@kline_1m"
        url = WS_BASE + streams
        backoff = 1
        last_stat = time.time()
        log.info(f"[启动] 旁路采集器 | 交易对={self.symbol.upper()} | 库={self.db}")
        log.info(f"[启动] 订阅: {streams}")

        while self.running:
            try:
                async with websockets.connect(
                    url, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT, open_timeout=15
                ) as ws:
                    log.info("[连接] Binance WS 已连接")
                    log_event(self.db, "connect", url)
                    backoff = 1
                    async for raw in ws:
                        if not self.running:
                            break
                        self.on_message(raw)
                        now = time.time()
                        if now - last_stat >= STAT_INTERVAL:
                            self.flush(force=True)
                            self.log_stats()
                            last_stat = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"[连接] 异常: {type(e).__name__}: {e} → {backoff}s 后重连")
                log_event(self.db, "disconnect", f"{type(e).__name__}: {e}")
                self.flush(force=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        self.flush(force=True)
        log.info("[停止] 采集器已退出")

    def stop(self):
        self.running = False


def main():
    ap = argparse.ArgumentParser(description="Binance PAXG/USDT 旁路行情采集器")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Binance 交易对(小写), 默认 paxgusdt")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 库路径")
    ap.add_argument("--no-alert", action="store_true", help="关闭做单数据推送")
    ap.add_argument("--maintain", action="store_true",
                    help="只做数据库归档清理然后退出（供手工/cron 调用）")
    ap.add_argument("--maintain-dry-run", action="store_true",
                    help="配合 --maintain：只统计不写入")
    args = ap.parse_args()

    db_path = Path(args.db)
    init_db(db_path)

    cfg = _load_cfg()

    # 归档模式：不进 WS 采集循环，执行完就退出
    if args.maintain:
        log.info(f"[归档] 手工模式 | 库={db_path} | 试运行={args.maintain_dry_run}")
        st = maintain_db(cfg, db_path, dry_run=args.maintain_dry_run)
        log.info(f"[归档] 结果: {st}")
        return
    watcher = LevelWatcher(
        enabled=(not args.no_alert) and bool(cfg.get("paxg_alert_enabled", True)),
        max_age_h=float(cfg.get("paxg_alert_max_age_hours", DEFAULT_ALERT_MAX_AGE_H)),
        confirm_xau=bool(cfg.get("paxg_alert_confirm_xau", True)),
        confirm_lookback_min=int(cfg.get("paxg_alert_confirm_lookback_min", 3)),
        db_path=db_path,
    )
    log.info(f"[配置] 做单数据推送: {'开启' if watcher.enabled else '关闭'} | 价位有效期 {watcher.max_age_h}h")
    if watcher.enabled:
        log.info(f"[配置] PAXG 假穿越复核: {'开启' if watcher.confirm_xau else '关闭'}"
                 f"（用真实 XAU/USD 复核最近 {watcher.confirm_lookback_min} 分钟）")

    c = Collector(db_path, args.symbol, watcher=watcher)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, c.stop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(c.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
