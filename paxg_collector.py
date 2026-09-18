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
LEVEL_RELOAD_SECS = 60                       # 重新加载监控价位的间隔
LEVEL_MAX_SIGNALS = 20                       # 最多盯最近多少条信号
DEFAULT_ALERT_MAX_AGE_H = 24                 # 信号超过多少小时不再盯

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

    def __init__(self, enabled: bool = True, max_age_h: float = DEFAULT_ALERT_MAX_AGE_H):
        self.enabled = enabled
        self.max_age_h = max_age_h
        self.levels = []
        self.fired = set()
        self.prev = None
        self.last_reload = 0.0

    def _reload(self):
        if not SIGNALS_DB.exists():
            return
        cutoff = (datetime.now() - timedelta(hours=self.max_age_h)).strftime("%Y-%m-%d %H:%M:%S")
        try:
            with sqlite3.connect(SIGNALS_DB) as conn:
                rows = conn.execute(
                    "SELECT id, timeframe, direction, sig_type, grade, score, entry, sl, tp1, tp2, pushed_at"
                    " FROM signals WHERE pushed_at >= ? ORDER BY id DESC LIMIT ?",
                    (cutoff, LEVEL_MAX_SIGNALS),
                ).fetchall()
        except Exception as e:
            log.error(f"[做单价位] 读取 signals.db 失败: {e}")
            return

        lv, seen = [], set()
        for (sid, tf, d, styp, grade, score, entry, sl, tp1, tp2, pushed_at) in rows:
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
                    "pushed_at": pushed_at,
                })
        self.levels = lv
        log.info(f"[做单价位] {len(rows)} 条近期信号 → 监控 {len(lv)} 个价位")

    def check(self, price: float):
        if not self.enabled:
            return
        now = time.time()
        if now - self.last_reload >= LEVEL_RELOAD_SECS:
            self._reload()
            self.last_reload = now
        if self.prev is None:
            self.prev = price
            return
        p0, p1 = self.prev, price
        for l in self.levels:
            if l["id"] in self.fired:
                continue
            p = l["price"]
            if (p0 < p <= p1) or (p0 > p >= p1):   # 上穿或下穿
                self.fired.add(l["id"])
                self._alert(l, price)
        self.prev = price

    def _alert(self, l: dict, price: float):
        emoji = {"入场": "🎯", "止损": "🛑", "TP1": "✅", "TP2": "🏆"}.get(l["kind"], "🔔")
        title = f"{emoji} 盘中触发 {l['kind']} | {l['label']}"
        content = (
            f"> 触发价: **{l['price']:.2f}**\n"
            f"> 现价(PAXG): **{price:.2f}**\n"
            f"> 信号入场: {l['entry']:.2f} / 止损: {l['sl']:.2f}\n"
            f"> 信号时间: {l['pushed_at']}"
        )
        log.info(f"[做单] 触发 {l['kind']} @ {l['price']:.2f} (现价 {price:.2f})")
        send_alert(title, content)


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
    args = ap.parse_args()

    db_path = Path(args.db)
    init_db(db_path)

    cfg = _load_cfg()
    watcher = LevelWatcher(
        enabled=(not args.no_alert) and bool(cfg.get("paxg_alert_enabled", True)),
        max_age_h=float(cfg.get("paxg_alert_max_age_hours", DEFAULT_ALERT_MAX_AGE_H)),
    )
    log.info(f"[配置] 做单数据推送: {'开启' if watcher.enabled else '关闭'} | 价位有效期 {watcher.max_age_h}h")

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
