#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XAUMonitor Web 仪表盘（B9）

读取 signals.db + dpb_status.json，零 API 消耗展示：
  - 信号统计（总数 / 等级分布 / 周期分布 / 方向 / 今日）
  - 最近信号明细表
  - 各周期实时状态快照（趋势 / RSI / ATR / 信号）
  - 配置摘要
  - 【资金与盈亏】初始资金 → 当前权益，已实现 + 持仓浮动（实时价估算）
  - 【按日分区】信号按自然日归组，逐日胜负/当日R/当日金额/累计权益，可翻页

用法:
  uvicorn dashboard:app --host 0.0.0.0 --port 1689
"""
import json
import logging
import os
import secrets
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

_security = HTTPBasic(auto_error=False)

log = logging.getLogger("dashboard")
if not log.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

BASE = Path(__file__).parent


def _cfg() -> dict:
    """读取配置。

    这里**必须**报错而不是静默返回 {}：配置读不到时，account_equity 等
    会退回默认值，仪表盘照常渲染 —— 但显示的权益/盈亏全是错的，
    没有任何迹象提示你。静默失败在这里比崩溃更危险。
    """
    try:
        with open(BASE / "dpb_config.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log.error(f"读取 dpb_config.json 失败（仪表盘数值可能不准）: {e}")
        return {}


def _db_path() -> Path:
    p = Path(_cfg().get("db_file", "signals.db"))
    return p if p.is_absolute() else BASE / p


def _status() -> dict:
    p = Path(_cfg().get("status_file", "dpb_status.json"))
    p = p if p.is_absolute() else BASE / p
    if p.exists():
        try:
            with open(p, encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            log.warning(f"读取状态快照 {p.name} 失败（将显示为空快照）: {e}")
    else:
        log.warning(f"状态快照 {p.name} 不存在（监控进程可能没在跑）")
    return {"updated_at": None, "timeframes": {}, "ticker": None}


def _query(sql: str, args: tuple = ()) -> list:
    db = _db_path()
    if not db.exists():
        return []
    try:
        with sqlite3.connect(db) as conn:
            conn.row_factory = sqlite3.Row
            return [dict(r) for r in conn.execute(sql, args).fetchall()]
    except Exception as e:
        # 【不要再静默吞掉】曾因 s.resolved_at 写错表名而整表返回空，
        # 页面显示「0 笔」，排查绕了一大圈。出错必须看得见。
        print(f"[dashboard] SQL 失败: {type(e).__name__}: {e}")
        return []


# ============================================================
# 鉴权（HTTP Basic）
# ============================================================
# 为什么必须做：这个仪表盘经 Nginx 8088 暴露在**公网**，
# 上面有账户权益、信号明细、止损止盈价位。没有鉴权 = 任何人可看。
# 为什么没配置时自动生成而不是「放行」：默认敞开是更糟的失败方式。
# 密码写入 gitignored 的 dpb_config.json —— 不进仓库、不进日志。
_AUTH = {"user": None, "pw": None, "mtime": 0.0}


def _auth_creds() -> tuple:
    """返回 (用户名, 密码)；配置里没有则随机生成并写回配置（只做一次）。"""
    cfg_path = BASE / "dpb_config.json"
    try:
        mt = cfg_path.stat().st_mtime
    except OSError:
        mt = 0.0
    if _AUTH["user"] and _AUTH["mtime"] == mt:
        return _AUTH["user"], _AUTH["pw"]

    cfg = _cfg()
    auth = ((cfg.get("dashboard") or {}).get("auth") or {})
    user, pw = auth.get("user"), auth.get("password")
    if user and pw:
        _AUTH.update(user=user, pw=pw, mtime=mt)
        return user, pw

    user = user or "zeus"
    pw = secrets.token_urlsafe(12)
    cfg.setdefault("dashboard", {}).setdefault("auth", {})
    cfg["dashboard"]["auth"].update({"user": user, "password": pw})
    try:
        tmp = cfg_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
        os.replace(tmp, cfg_path)          # 原子替换，避免写坏生产配置
        log.warning("仪表盘启用了登录但未配置密码 → 已生成随机密码写入 "
                    "dpb_config.json 的 dashboard.auth（文件已 gitignore）。"
                    "本日志不打印明文。")
        mt = cfg_path.stat().st_mtime
    except Exception as e:
        log.error(f"写入仪表盘密码失败（本次仍启用该密码）: {e}")
    _AUTH.update(user=user, pw=pw, mtime=mt)
    return user, pw


def _auth_enabled() -> bool:
    """仪表盘是否要登录。**默认关闭**——这个面板本来就是开放出来看的。"""
    a = ((_cfg().get("dashboard") or {}).get("auth") or {})
    return bool(a.get("enabled", False))


def _require_auth(creds: HTTPBasicCredentials = Depends(_security)):
    """全站鉴权（可选）。

    默认**不启用**：这块面板的设计意图就是"开放出来看"。想在公网加锁时，
    在 dpb_config.json 里写：
        "dashboard": {"auth": {"enabled": true, "user": "...", "password": "..."}}
    只写 enabled=true 而不给密码，会自动生成一个随机密码并写回配置。
    """
    if not _auth_enabled():
        return True
    user, pw = _auth_creds()
    if creds and secrets.compare_digest(creds.username, user) \
            and secrets.compare_digest(creds.password, pw):
        return True
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="需要登录",
        headers={"WWW-Authenticate": 'Basic realm="XAUMonitor"'},
    )


app = FastAPI(title="XAUMonitor Dashboard", dependencies=[Depends(_require_auth)])


@app.get("/api/stats")
def api_stats():
    total = _query("SELECT COUNT(*) AS c FROM signals")
    total = total[0]["c"] if total else 0
    by_grade = _query("SELECT grade AS k, COUNT(*) AS c FROM signals GROUP BY grade")
    by_tf = _query("SELECT timeframe AS k, COUNT(*) AS c FROM signals GROUP BY timeframe")
    by_dir = _query("SELECT direction AS k, COUNT(*) AS c FROM signals GROUP BY direction")
    by_type = _query("SELECT sig_type AS k, COUNT(*) AS c FROM signals GROUP BY sig_type")
    today = _query("SELECT COUNT(*) AS c FROM signals WHERE pushed_at >= date('now','localtime')")
    today = today[0]["c"] if today else 0
    reso = _query("SELECT COUNT(*) AS c FROM signals WHERE resonance >= 2")
    reso = reso[0]["c"] if reso else 0
    return {
        "total": total,
        "today": today,
        "resonance": reso,
        "by_grade": {r["k"] or "?": r["c"] for r in by_grade},
        "by_timeframe": {r["k"] or "?": r["c"] for r in by_tf},
        "by_direction": {"多" if r["k"] == 1 else "空": r["c"] for r in by_dir},
        "by_type": {r["k"] or "?": r["c"] for r in by_type},
    }


@app.get("/api/signals")
def api_signals(limit: int = 60):
    rows = _query(
        "SELECT s.id, s.ts, s.pushed_at, s.timeframe, s.direction, s.sig_type, s.grade, s.score,"
        " s.band, s.entry, s.sl, s.tp1, s.tp2, s.rsi, s.atr, s.resonance,"
        " o.outcome, o.r_multiple, o.resolved_at"
        " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
        " ORDER BY s.id DESC LIMIT ?",
        (int(limit),),
    )
    return {"count": len(rows), "signals": rows}


@app.get("/api/outcomes")
def api_outcomes():
    """结果追踪统计：胜率 / 期望值 / 盈亏因子（做单策略的验证闭环）"""
    rows = _query(
        "SELECT o.outcome, o.r_multiple, o.bars, o.mfe, o.mae, o.resolved_at, o.cost_r,"
        " s.grade, s.sig_type, s.timeframe, s.direction, s.id"
        " FROM signal_outcomes o JOIN signals s ON s.id = o.signal_id"
        " ORDER BY s.id"
    )
    # 判据必须是 resolved_at（真正结案），不能只看 outcome：
    # TP1 已到但未到 TP2、窗口也没走满时，单子还在跑，仍属追踪中。
    closed = [r for r in rows if r["resolved_at"]]
    open_n = len(rows) - len(closed)
    if not closed:
        return {"closed": 0, "tracking": open_n, "win_rate": None, "expectancy_r": None,
                "profit_factor": None, "total_r": 0.0, "wins": 0, "losses": 0,
                "by_grade": {}, "by_type": {}, "by_timeframe": {}, "recent": []}

    rs = [float(r["r_multiple"] or 0) for r in closed]
    cost_total = sum(float(r["cost_r"] or 0) for r in closed)
    wins = [x for x in rs if x > 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(x for x in rs if x <= 0))

    def _agg(key):
        d = {}
        for r in closed:
            k = r[key] or "?"
            v = d.setdefault(str(k), {"n": 0, "wins": 0, "r": 0.0})
            v["n"] += 1
            v["r"] += float(r["r_multiple"] or 0)
            if float(r["r_multiple"] or 0) > 0:
                v["wins"] += 1
        return d

    recent = [dict(r) for r in closed][-20:][::-1]
    return {
        "closed": len(closed),
        "tracking": open_n,
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": len(wins) / len(closed) * 100,
        "expectancy_r": sum(rs) / len(closed),
        "total_r": sum(rs),
        "cost_total_r": cost_total,
        "gross_total_r": sum(rs) + cost_total,
        "avg_cost_r": cost_total / len(closed),
        "profit_factor": (gross_win / gross_loss) if gross_loss else None,
        "by_grade": _agg("grade"),
        "by_type": _agg("sig_type"),
        "by_timeframe": _agg("timeframe"),
        "recent": recent,
    }


@app.get("/api/status")
def api_status():
    cfg = _cfg()
    return {
        "snapshot": _status(),
        "config": {
            "ticker": cfg.get("ticker_td", cfg.get("ticker")),
            "trade_freq": cfg.get("trade_freq"),
            "trend_stability": cfg.get("trend_stability"),
            "signal_cooldown": cfg.get("signal_cooldown"),
            "min_signal_grade": cfg.get("min_signal_grade"),
            "resonance_min_count": cfg.get("resonance_min_count"),
            "breakout_sl_atr": cfg.get("breakout_sl_atr"),
            "use_momentum_filter": cfg.get("use_momentum_filter"),
            "momentum_body_ratio": cfg.get("momentum_body_ratio"),
            "check_interval_minutes": cfg.get("check_interval_minutes"),
        },
    }


# ============================================================
# 资金与盈亏（用户口径：固定 0.01 手 = 1 盎司 → 价格每波动 $1 = 盈亏 $1）
# ============================================================
def _num(cfg: dict, key: str, default: float) -> float:
    try:
        return float(cfg.get(key, default))
    except Exception:
        return float(default)


def _oz_per_trade(cfg: dict) -> float:
    """每单手数对应的盎司数。

    用户实盘口径：min_lot=0.01 手、contract_oz=100（1手=100盎司）
    → 每笔 0.01 × 100 = 1 盎司 → 价格每波动 1 美元，盈亏就是 1 美元。
    """
    return _num(cfg, "min_lot", 0.01) * _num(cfg, "contract_oz", 100.0)


def _pnl_usd(r: dict, oz: float):
    """单笔盈亏（美元）；数据不完整时返回 **None**（而不是 0）。

    net_r 是「以止损距离为 1R」的净倍数（已扣点差），所以
    价格波动 = net_r × 止损距离，盈亏 = 价格波动 × 盎司数。

    为什么返回 None 而不是 0.0：
      算美元必须同时拿到 r_multiple / entry / sl。旧版缺列时
      `float(x or 0.0)` 把「缺失」静默当成 0 —— 于是「算不出来」和
      「不赚不亏」长得一模一样。/api/charts 漏选 entry/sl 时，
      权益曲线就这样变成一条 650 的平线，而退化区间还让它看起来
      像一张正常图表。宁可返回 None，让调用方显式处理。
    """
    if r.get("r_multiple") is None:
        return None
    entry, sl = r.get("entry"), r.get("sl")
    if entry is None or sl is None:
        return None
    risk = abs(float(entry) - float(sl))
    if risk < 1e-9:
        return None
    return float(r["r_multiple"]) * risk * oz


def _sum_pnl(rs, oz, where="") -> float:
    """汇总一组信号的美元盈亏；跳过数据不全的，并把它**显式记进日志**。

    与 _pnl_usd 配套：把「算不出来」暴露出来，而不是静默当 0 累加
    （那正是权益曲线变成平线却没人发现的原因）。
    """
    tot, bad = 0.0, 0
    for r in rs:
        v = _pnl_usd(r, oz)
        if v is None:
            bad += 1
            v = 0.0          # 归一成 0，让调用方的 sum() 不会炸
        tot += v
        r["pnl_usd"] = v
    if bad:
        log.warning(f"{where} {bad} 笔信号缺 r_multiple/entry/sl，"
                    f"美元换算按 0 计入（合计会偏小）")
    return tot


def _is_closed(r: dict) -> bool:
    """是否真正结案 —— 判据是 resolved_at，不能只看 outcome。

    TP1 已到但还没到 TP2、追踪窗口也没走满时，单子仍在跑，属追踪中。
    """
    return bool(r.get("resolved_at"))


def _live_xau(snapshot: dict, cfg: dict):
    """尽量实时的 XAU 价格估计。

    两层：
      1) 权威层：监控端快照里各周期的收盘价（XAU/USD 真实行情，但最多 4 分钟旧）
      2) 实时层：Binance PAXG 的最新成交价 + 与 XAU 的价差修正
    PAXG 是稀薄市场的代理品、会与 XAU 有 $2-6 的价差，所以必须先修正再当价格用。
    返回 (price, source_zh)。
    """
    tfs = (snapshot or {}).get("timeframes") or {}
    xau = None
    for tf in ("5m", "15m", "1h", "4h", "1d"):
        d = tfs.get(tf) or {}
        if d.get("close"):
            xau = float(d["close"])
            break
    if xau is None:
        return None, "无快照"
    # 实时层：PAXG 最新价 + 价差
    paxg_db = BASE / "paxg_stream.db"
    if paxg_db.exists():
        try:
            with sqlite3.connect(paxg_db) as conn:
                row = conn.execute(
                    "SELECT price, ts_ms FROM trades ORDER BY ts_ms DESC LIMIT 1").fetchone()
                # 用最近 30 分钟逐分钟中位数算价差（抗单点插针）
                bars = conn.execute(
                    "SELECT open_ms, close FROM klines_1m WHERE closed=1"
                    " ORDER BY open_ms DESC LIMIT 35").fetchall()
            if row:
                # 价差锚点：快照那个周期K线的收盘分钟
                d = tfs.get("5m") or tfs.get("15m") or {}
                anchor = str(d.get("bar_time") or "")[:16]
                diff = None
                if anchor:
                    for ms, c in bars:
                        if c and datetime.fromtimestamp(ms / 1000).strftime("%Y-%m-%d %H:%M") == anchor:
                            diff = xau - float(c)
                            break
                if diff is None and bars:
                    # 锚点对不上时用最近一根，误差可接受（仅用于展示）
                    diff = xau - float(bars[0][1])
                if diff is not None and abs(diff) < 50:
                    age = (datetime.now().timestamp() * 1000 - float(row[1])) / 1000
                    return float(row[0]) + diff, f"PAXG实时+价差{abs(age) < 90 and '（新鲜）' or ''}"
        except Exception as e:
            # 实时价只是「更好看的估计」，失败就退回快照价 —— 属预期内的降级。
            # 但仍记 debug 日志：若它长期失败，说明 PAXG 库/库结构出了问题，
            # 那时仪表盘的「实时」标签就是假的，必须有迹可循。
            log.debug(f"实时价估算失败，退回快照价: {e}")
    return xau, "XAU/USD 快照"


@app.get("/api/equity")
def api_equity():
    """初始金额 → 现在的盈亏：已实现 + 浮动（含持仓中逐笔）"""
    cfg = _cfg()
    oz = _oz_per_trade(cfg)
    init = _num(cfg, "account_equity", 650.0)
    rows = _query(
        "SELECT s.id, s.timeframe, s.direction, s.sig_type, s.grade, s.score, s.entry, s.sl,"
        " s.tp1, s.tp2, s.pushed_at, o.resolved_at,"
        " o.outcome, o.r_multiple, o.cost_r, o.bars"
        " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
        " ORDER BY s.id"
    )
    snap = _status()
    live, src = _live_xau(snap, cfg)

    closed, openpos = [], []
    for r in rows:
        pnl = _pnl_usd(r, oz)
        risk = abs(float(r["entry"] or 0) - float(r["sl"] or 0))
        r["risk_usd"] = risk * oz
        r["pnl_usd"] = pnl
        if _is_closed(r):
            closed.append(r)
        elif (r.get("outcome") or "open") != "SL":
            # 追踪中：用实时价估浮动盈亏
            if live and r["direction"]:
                move = (live - float(r["entry"])) * int(r["direction"])
                r["float_pnl_usd"] = move * oz
                r["float_r"] = move / risk if risk else 0.0
                r["live_price"] = live
            else:
                r["float_pnl_usd"] = 0.0
                r["float_r"] = 0.0
            openpos.append(r)

    realized = _sum_pnl(closed, oz, "[equity]")
    unrealized = sum(x["float_pnl_usd"] or 0.0 for x in openpos)
    realized_r = sum(float(x["r_multiple"] or 0) for x in closed)
    wins = [x for x in closed if (x["r_multiple"] or 0) > 0]
    gross_win = sum(x["pnl_usd"] for x in wins)
    gross_loss = abs(sum(x["pnl_usd"] for x in closed if (x["r_multiple"] or 0) <= 0))

    return {
        "initial_equity": init,
        "oz_per_trade": oz,
        "equity_realized": init + realized,
        "equity_total": init + realized + unrealized,
        "realized_usd": realized,
        "unrealized_usd": unrealized,
        "realized_r": realized_r,
        "return_pct": (realized + unrealized) / init * 100 if init else 0.0,
        "closed": len(closed),
        "tracking": len(openpos),
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": (len(wins) / len(closed) * 100) if closed else None,
        "avg_win_usd": (gross_win / len(wins)) if wins else 0.0,
        "avg_loss_usd": (-gross_loss / (len(closed) - len(wins))) if closed and len(closed) > len(wins) else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss else None,
        "live_price": live,
        "live_source": src,
        "open_positions": openpos,
    }


@app.get("/api/daily")
def api_daily(page: int = 1, per_page: int = 5):
    """按自然日分区 + 分页，逐日给出当日与累计盈亏。"""
    cfg = _cfg()
    oz = _oz_per_trade(cfg)
    init = _num(cfg, "account_equity", 650.0)
    rows = _query(
        "SELECT s.id, s.timeframe, s.direction, s.sig_type, s.grade, s.score, s.entry, s.sl,"
        " s.tp1, s.tp2, s.pushed_at, o.resolved_at,"
        " o.outcome, o.r_multiple, o.cost_r"
        " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
        " ORDER BY s.pushed_at, s.id"
    )

    # 按自然日归组（用推送日期，保证「当日出了什么信号」一目了然）
    days_map = {}
    order = []
    for r in rows:
        d = str(r.get("pushed_at") or "")[:10] or "未知"
        if d not in days_map:
            days_map[d] = []
            order.append(d)
        r["closed"] = _is_closed(r)
        days_map[d].append(r)

    # 按时间顺序累计（累计口径必须时间正序，否则曲线会错）
    cum = init
    days = []
    for d in order:
        sigs = days_map[d]
        closed = [x for x in sigs if x["closed"]]
        day_real = _sum_pnl(closed, oz, f"[daily {d}]")
        cum += day_real
        wins = len([x for x in closed if (x["r_multiple"] or 0) > 0])
        days.append({
            "date": d,
            "n": len(sigs),
            "n_closed": len(closed),
            "n_open": len(sigs) - len(closed),
            "wins": wins,
            "losses": len(closed) - wins,
            "day_r": sum(float(x["r_multiple"] or 0) for x in closed),
            "day_usd": day_real,
            "equity_end": cum,
            "signals": sigs,
        })

    days.reverse()                       # 页面展示：最新的一天在最上面
    total = len(days)
    per_page = max(1, min(int(per_page), 31))
    pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(int(page), pages))
    start = (page - 1) * per_page

    return {
        "page": page,
        "per_page": per_page,
        "pages": pages,
        "total_days": total,
        "initial_equity": init,
        "oz_per_trade": oz,
        "days": days[start:start + per_page],
    }


# ============================================================
# E2 图表：服务端生成内联 SVG
# ============================================================
# 为什么自己画 SVG 而不引 Chart.js/ECharts：
#   ① 这页是公网访问的手机页面，多一个 CDN 就多一个加载失败点（国内尤甚）；
#   ② 图表要的东西很少（折线 + 柱状），几百行内联 SVG 足够，还省一次渲染抖动；
#   ③ viewBox + width:100% 天然自适应，桌面手机同一套代码。
def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _svg_line(pts, w=660, h=150, pad=26, base=None, fmt="{:.2f}"):
    """累积曲线。pts: [(label, value)]；base: 参考基准线（如初始资金）。"""
    if len(pts) < 2:
        return '<div class="muted ch-empty">数据不足，画不出曲线</div>'
    vals = [v for _, v in pts]
    lo, hi = min(vals), max(vals)
    if base is not None:
        lo, hi = min(lo, base), max(hi, base)
    if hi - lo < 1e-9:
        hi, lo = hi + 1, lo - 1
    span = hi - lo
    lo, hi = lo - span * 0.08, hi + span * 0.08   # 上下留白
    span = hi - lo
    iw, ih = w - pad * 2, h - pad * 2

    def X(i):
        return pad + iw * i / (len(pts) - 1)

    def Y(v):
        return pad + ih * (1 - (v - lo) / span)

    line = " ".join(f"{X(i):.1f},{Y(v):.1f}" for i, (_, v) in enumerate(pts))
    area = f"{pad},{pad+ih} " + line + f" {pad+iw},{pad+ih}"
    out = [f'<svg class="chart" viewBox="0 0 {w} {h}" preserveAspectRatio="none">',
           '<defs><linearGradient id="eqg" x1="0" y1="0" x2="0" y2="1">'
           '<stop offset="0%" stop-color="#3fb950" stop-opacity="0.34"/>'
           '<stop offset="100%" stop-color="#3fb950" stop-opacity="0"/></linearGradient></defs>']
    if base is not None and lo <= base <= hi:
        by = Y(base)
        out.append(f'<line x1="{pad}" y1="{by:.1f}" x2="{pad+iw}" y2="{by:.1f}"'
                   ' stroke="#8b949e" stroke-width="1" stroke-dasharray="5,4"/>')
        out.append(f'<text x="{pad+3}" y="{by-4:.1f}" fill="#8b949e" font-size="12">'
                   f'{_esc(fmt.format(base))}</text>')
    out.append(f'<polygon points="{area}" fill="url(#eqg)"/>')
    out.append(f'<polyline points="{line}" fill="none" stroke="#3fb950"'
               ' stroke-width="2" stroke-linejoin="round"/>')
    lx, ly = X(len(pts) - 1), Y(pts[-1][1])
    out.append(f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="3.5" fill="#3fb950"/>')
    out.append(f'<text x="{pad}" y="12" fill="#8b949e" font-size="12">'
               f'{_esc(fmt.format(hi))}</text>')
    out.append(f'<text x="{pad}" y="{h-6}" fill="#8b949e" font-size="12">'
               f'{_esc(fmt.format(lo))}</text>')
    out.append(f'<text x="{w-pad}" y="{h-6}" fill="#8b949e" font-size="12"'
               ' text-anchor="end">' + _esc(pts[-1][0]) + '</text>')
    out.append("</svg>")
    return "".join(out)


def _svg_bars(pts, w=660, h=140, pad=26, fmt="{:.2f}", unit=""):
    """柱状图（正绿负红）。pts: [(label, value)]"""
    if not pts:
        return '<div class="muted ch-empty">暂无数据</div>'
    vals = [v for _, v in pts]
    mx = max(abs(v) for v in vals) or 1.0
    iw, ih = w - pad * 2, h - pad * 2
    bw = max(2.0, iw / len(pts) * 0.68)
    zero = pad + ih / 2
    sc = (ih / 2) / (mx * 1.12)
    out = [f'<svg class="chart" viewBox="0 0 {w} {h}" preserveAspectRatio="none">',
           f'<line x1="{pad}" y1="{zero:.1f}" x2="{pad+iw}" y2="{zero:.1f}"'
           ' stroke="#30363d" stroke-width="1"/>']
    for i, (_, v) in enumerate(pts):
        cx = pad + iw * (i + 0.5) / len(pts)
        bh = abs(v) * sc
        y = zero - bh if v >= 0 else zero
        col = "#3fb950" if v >= 0 else "#f85149"
        out.append(f'<rect x="{cx-bw/2:.1f}" y="{y:.1f}" width="{bw:.1f}"'
                   f' height="{max(bh,0.6):.1f}" fill="{col}" rx="1.5"/>')
    out.append(f'<text x="{pad}" y="12" fill="#8b949e" font-size="12">'
               f'±{_esc(fmt.format(mx))}{_esc(unit)}</text>')
    out.append(f'<text x="{pad}" y="{h-6}" fill="#8b949e" font-size="12">'
               f'{_esc(pts[0][0])}</text>')
    out.append(f'<text x="{w-pad}" y="{h-6}" fill="#8b949e" font-size="12"'
               f' text-anchor="end">{_esc(pts[-1][0])}</text>')
    out.append("</svg>")
    return "".join(out)


@app.get("/api/charts")
def api_charts():
    """图表数据：累积权益曲线 / 逐日盈亏 / 按小时期望 / 信号分布。

    全部由 signals.db + signal_outcomes 现算，零 API 消耗。
    """
    cfg = _cfg()
    init = _num(cfg, "account_equity", 650.0)
    rows = _query(
        "SELECT s.id, s.timeframe, s.direction, s.sig_type, s.grade, s.pushed_at,"
        " s.entry, s.sl,"
        " o.outcome, o.r_multiple, o.resolved_at, o.cost_r"
        " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
        " ORDER BY s.id")
    oz = _oz_per_trade(cfg)
    days = {}
    for r in rows:
        d = (r.get("pushed_at") or "")[:10]
        if not d:
            continue
        days.setdefault(d, []).append(r)

    # ---- 累积权益曲线 + 逐日金额 ----
    # 不用手写循环累加，统一走 _sum_pnl（单一来源，避免再出现「某个面板
    # 漏了 entry/sl 就静默算成 0」这类问题）
    cum, eq_pts, day_pts = init, [], []
    for d in sorted(days):
        usd = _sum_pnl([r for r in days[d] if r.get("resolved_at")], oz, f"[charts {d}]")
        cum += usd
        eq_pts.append((d[5:], cum))
        day_pts.append((d[5:], usd))

    # ---- 按小时：期望 R（供「时段过滤」决策用，数据说话）----
    hours = {}
    for r in rows:
        if not r.get("resolved_at") or r.get("r_multiple") is None:
            continue
        hh = (r.get("pushed_at") or "")[11:13]
        if not hh.isdigit():
            continue
        hours.setdefault(int(hh), []).append(float(r["r_multiple"]))
    hour_pts = [(f"{h}点", sum(v) / len(v)) for h, v in sorted(hours.items()) if len(v) >= 2]

    # ---- 分布 ----
    def _count(fn):
        c = {}
        for r in rows:
            k = fn(r)
            if k:
                c[k] = c.get(k, 0) + 1
        return c

    dist = {
        "grade": _count(lambda r: r.get("grade")),
        "type": _count(lambda r: r.get("sig_type")),
        "tf": _count(lambda r: r.get("timeframe")),
        "outcome": _count(lambda r: r.get("outcome") if r.get("resolved_at") else "open"),
        "hour_n": {f"{h}点": len(v) for h, v in sorted(hours.items())},
    }
    return {
        "equity_svg": _svg_line(eq_pts, base=init),
        "daily_svg": _svg_bars(day_pts, fmt="{:+.2f}", unit=" USD"),
        "hour_svg": _svg_bars(hour_pts, fmt="{:+.2f}", unit=" R/笔"),
        "samples": {"days": len(days), "resolved": sum(
            1 for r in rows if r.get("resolved_at")),
            "hour_points": len(hour_pts)},
        "dist": dist,
    }


@app.get("/", response_class=HTMLResponse)
def index():
    return HTML_PAGE


HTML_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>XAUMonitor · DPB 黄金信号监控</title>
<style>
  /* ══════════════════════════════════════════════════════════════
     设计令牌（Design Tokens）
     优化前：字号 11/12/13/14/15/16/17/18/19/21/23 共 11 种散装值，
             间距 8~14px 随手写，圆角 8/10/11/12 混用，颜色只有 7 个变量。
             结果：卡片之间"看着差不多但都不一样"，属于视觉噪音。
     优化后：字号 6 级、间距 4px 基准 6 级、圆角 4 级、表面色 4 层。
             所有组件只引用令牌，不再出现魔法数字。
     ══════════════════════════════════════════════════════════════ */
  :root{
    /* ── 表层（暗→亮 4 层，建立纵深） ── */
    --bg:#0a0e13;      /* 页面底 */
    --s1:#131920;      /* 一级卡面 */
    --s2:#1a222c;      /* 内嵌 / 次级面 */
    --s3:#222c38;      /* 日头 / 悬浮面 */
    /* ── 描边（2 级：定界 vs 分隔） ── */
    --line:#28323e;
    --line2:#1c242e;
    /* ── 文字（3 级层次，替代原来只有 fg/mut 两级） ── */
    --fg:#e9eff7;      /* 主：数字、标题 */
    --fg2:#aab7c6;     /* 次：标签、说明 */
    --fg3:#6f7d8e;     /* 弱：脚注、单位 */
    /* ── 语义色 ── */
    --up:#3fb950; --dn:#f85149; --warn:#e3b341; --info:#58a6ff;
    --up-bg:#122b18; --dn-bg:#2c1414; --warn-bg:#2b2008;
    /* ── 空间（4px 基准，6 级） ── */
    --sp1:4px; --sp2:8px; --sp3:12px; --sp4:16px; --sp5:22px; --sp6:30px;
    /* ── 圆角（4 级） ── */
    --r1:6px; --r2:10px; --r3:14px; --r4:18px;
    /* ── 字号（6 级） ── */
    --fs-xs:11.5px; --fs-sm:13px; --fs-md:14.5px; --fs-lg:16.5px;
    --fs-xl:21px; --fs-2xl:30px;
    /* ── 旧变量名保留，避免 JS 内联样式失效 ── */
    --green:var(--up); --red:var(--dn); --gold:var(--warn); --blue:var(--info);
    --card:var(--s1); --border:var(--line); --mut:var(--fg2);
  }

  *{ box-sizing:border-box; -webkit-text-size-adjust:100%; }

  body{
    margin:0; background:var(--bg); color:var(--fg);
    font-size:var(--fs-md); line-height:1.5;
    font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,
                "PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
    /* 关键：等宽数字。仪表盘里所有数字必须逐位对齐，
       否则上下行的小数点会错位，扫读时极易看错一位。 */
    font-variant-numeric:tabular-nums;
    font-feature-settings:"tnum" 1,"lnum" 1;
    -webkit-font-smoothing:antialiased;
    -moz-osx-font-smoothing:grayscale;
    text-rendering:optimizeLegibility;
  }

  .wrap{ max-width:1240px; margin:0 auto; padding:var(--sp3) var(--sp3) var(--sp6); }

  /* ───── 页头 ───── */
  header{
    display:flex; align-items:baseline; gap:var(--sp3); flex-wrap:wrap;
    margin-bottom:var(--sp4); padding-bottom:var(--sp3);
    border-bottom:1px solid var(--line2);
  }
  h1{
    font-size:var(--fs-xl); margin:0; font-weight:800; letter-spacing:-.2px;
    white-space:nowrap; line-height:1.2;
  }

  /* ───── 区块标题：统一"左侧色条 + 标题 + 右侧说明"三层结构 ───── */
  section{ margin-bottom:var(--sp5); }
  h2{
    display:flex; align-items:baseline; gap:var(--sp2);
    font-size:var(--fs-lg); font-weight:750; letter-spacing:-.1px;
    margin:0 0 var(--sp3); padding-left:10px;
    border-left:3px solid var(--info); line-height:1.35;
  }
  h2 .meta{
    margin-left:auto; font-size:var(--fs-xs); font-weight:500;
    color:var(--fg3); letter-spacing:.2px; white-space:nowrap;
  }
  h3{ font-size:var(--fs-sm); font-weight:700; margin:0 0 var(--sp1); letter-spacing:.1px; }
  .sub{
    color:var(--fg3); font-size:var(--fs-xs); line-height:1.6;
    margin:0 0 var(--sp2);
  }
  .muted{ color:var(--fg2); }
  .big{ font-size:var(--fs-lg); font-weight:800; }

  /* ───── 指标卡：全局统一唯一实现 ─────
     优化前 .card / .tf / .sig / .pos / .ch-wrap 各自写一遍
     background+border+radius+padding，值还不一样 → 视觉噪音。
     现在所有面统一走 .card，变体只做最小覆盖。 */
  .cards{
    display:grid; grid-template-columns:repeat(auto-fit,minmax(108px,1fr));
    gap:var(--sp2);
  }
  .card{
    background:var(--s1); border:1px solid var(--line); border-radius:var(--r2);
    padding:var(--sp3); min-width:0;
  }
  .card .v{
    font-size:var(--fs-xl); font-weight:800; line-height:1.14;
    letter-spacing:-.4px; white-space:nowrap;
    /* 刻意不加 overflow:hidden / text-overflow:ellipsis ——
       被截断的数字会读成"另一个数字"（$1,748 看成 $1,7），
       在资金看板上这是数据错误而不是样式问题。
       宁可让它换行露出来，也不能悄悄切掉。 */
    overflow-wrap:anywhere;
  }
  .card .l{
    color:var(--fg3); font-size:var(--fs-xs); margin-top:3px;
    letter-spacing:.3px; line-height:1.35;
  }
  .card.good .v{ color:var(--up); }
  .card.bad  .v{ color:var(--dn); }
  .card.warn{ background:var(--dn-bg); border-color:var(--dn); }
  .card.warn .v{ color:#ff7b72; font-size:var(--fs-lg); }
  .card.warn .l{ color:#ff9a92; }
  /* 分组卡：把一堆同权重卡片变成"有结构的三簇" */
  .cgroup{ margin-bottom:var(--sp3); }
  .cgroup:last-child{ margin-bottom:0; }
  .cgroup > .cglabel{
    font-size:var(--fs-xs); color:var(--fg3); font-weight:600;
    letter-spacing:.6px; margin-bottom:var(--sp1); text-transform:uppercase;
  }

  /* ───── 主区：桌面左右双列 ───── */
  .main{ display:grid; gap:var(--sp5); align-items:start; }
  .col-b > section:last-child{ margin-bottom:0; }
  @media (min-width:960px){
    .main{ grid-template-columns:minmax(0,1.35fr) minmax(0,1fr); }
  }

  /* ───── 周期状态卡 ───── */
  .tf-grid{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:var(--sp2); }
  .tf{ background:var(--s1); border:1px solid var(--line); border-radius:var(--r2); padding:var(--sp3); min-width:0; }
  .tf .name{
    display:flex; justify-content:space-between; align-items:baseline;
    font-size:var(--fs-sm); font-weight:800; margin-bottom:var(--sp1);
  }
  .tf .name span{ font-size:var(--fs-xs); font-weight:600; color:var(--fg3); }
  .tf .row{
    display:flex; justify-content:space-between; gap:var(--sp2); min-width:0;
    font-size:var(--fs-xs); color:var(--fg3); padding:2px 0; line-height:1.5;
  }
  .tf .row b{ color:var(--fg); font-weight:700; }

  /* ───── 信号卡 ───── */
  .sig{
    background:var(--s1); border:1px solid var(--line); border-radius:var(--r3);
    padding:var(--sp3) var(--sp4); margin-bottom:var(--sp2);
  }
  .sig .top{ display:flex; align-items:center; gap:var(--sp2); flex-wrap:wrap; margin-bottom:var(--sp2); }
  .sig .tfname{ font-size:var(--fs-lg); font-weight:800; letter-spacing:-.2px; }
  .sig .time{ color:var(--fg3); font-size:var(--fs-xs); margin-left:auto; }
  .sig .grid{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:var(--sp1) var(--sp4); }
  .sig .kv{
    display:flex; justify-content:space-between; gap:var(--sp2); min-width:0;
    font-size:var(--fs-sm); color:var(--fg2);
  }
  .sig .kv b{ color:var(--fg); font-weight:800; }

  .dir-long{ color:var(--up); font-weight:800; }
  .dir-short{ color:var(--dn); font-weight:800; }
  .pill{ display:inline-block; padding:2px 9px; border-radius:99px; font-size:var(--fs-xs); font-weight:800; }
  .S{ background:var(--warn); color:#000; } .A{ background:var(--up); color:#000; }
  .B{ background:var(--info); color:#000; } .C{ background:#8a94a6; color:#000; }
  .fire{ color:var(--warn); font-weight:800; }
  .foot{
    text-align:center; padding:var(--sp3) 0 var(--sp5);
    font-size:var(--fs-xs); color:var(--fg3); line-height:1.7;
  }

  /* ───── 按日分区 ───── */
  .day{
    background:var(--s1); border:1px solid var(--line); border-radius:var(--r3);
    margin-bottom:var(--sp3); overflow:hidden;
  }
  .dayhead{
    display:flex; align-items:center; gap:var(--sp3); flex-wrap:wrap;
    padding:var(--sp3) var(--sp4); background:var(--s3);
    border-bottom:1px solid var(--line); cursor:pointer; user-select:none;
    transition:background .15s;
  }
  .dayhead:hover{ background:#28323f; }
  .dayhead .dt{ font-size:var(--fs-md); font-weight:800; letter-spacing:-.1px; }
  .dayhead .caret{ color:var(--fg3); font-size:var(--fs-xs); }
  .dayhead .sp{
    margin-left:auto; display:flex; gap:var(--sp3); flex-wrap:wrap;
    font-size:var(--fs-xs); color:var(--fg3);
  }
  .dayhead .sp b{ font-weight:800; color:var(--fg); }
  .up{ color:var(--up); } .dn{ color:var(--dn); }
  .daybody{ padding:var(--sp2) var(--sp4) var(--sp3); }
  .day.collapsed .daybody{ display:none; }

  .row-sig{
    display:grid; grid-template-columns:40px 72px minmax(0,1fr) auto auto;
    gap:var(--sp2); align-items:center;
    font-size:var(--fs-sm); padding:7px 0; border-bottom:1px solid var(--line2);
  }
  .row-sig:last-child{ border-bottom:0; }
  .row-sig .id{ color:var(--fg3); font-size:var(--fs-xs); }
  .row-sig .mid{ display:flex; gap:var(--sp1); align-items:center; flex-wrap:wrap; min-width:0; }
  .row-sig .res{ font-size:var(--fs-xs); font-weight:700; }
  .row-sig .pnl{ text-align:right; font-weight:800; min-width:80px; white-space:nowrap; }
  .row-sig .tm{ color:var(--fg3); font-size:var(--fs-xs); white-space:nowrap; }

  .live{ animation:pulse 1.6s ease-in-out infinite; }
  @keyframes pulse{ 0%,100%{opacity:1} 50%{opacity:.45} }

  /* ───── 持仓中 ───── */
  .pos{
    background:var(--s2); border:1px solid var(--line); border-radius:var(--r2);
    padding:var(--sp3); margin-bottom:var(--sp2);
  }
  .pos .t{ display:flex; gap:var(--sp2); align-items:center; flex-wrap:wrap; font-size:var(--fs-sm); }
  .pos .t .pnl{ margin-left:auto; font-weight:800; font-size:var(--fs-md); white-space:nowrap; }
  .pos .bar{ height:5px; border-radius:3px; background:var(--line); margin-top:var(--sp2); position:relative; }
  .pos .bar i{ position:absolute; top:-3px; width:3px; height:11px; background:var(--warn); border-radius:2px; }

  /* ───── 分页 ───── */
  .pager{ display:flex; align-items:center; justify-content:center; gap:var(--sp3); margin:var(--sp4) 0 var(--sp1); }
  .pager button{
    background:var(--s1); color:var(--fg); border:1px solid var(--line);
    border-radius:var(--r1); padding:9px 18px; font-size:var(--fs-sm); font-weight:700;
    cursor:pointer; transition:background .15s,border-color .15s;
    font-variant-numeric:tabular-nums;
  }
  .pager button:hover:not(:disabled){ background:var(--s3); border-color:#3a4653; }
  .pager button:disabled{ opacity:.32; cursor:default; }
  .pager button:focus-visible{ outline:2px solid var(--info); outline-offset:2px; }
  .pager .pg{ color:var(--fg2); font-size:var(--fs-xs); min-width:96px; text-align:center; }

  /* ───── 图表（服务端 SVG，零外部依赖） ─────
     max-width 上限：viewBox 是 660 宽，桌面容器 1200+ 时若不设上限，
     SVG 会被拉伸到 1.8 倍，图内文字变形、比例失衡。 */
  .chart{ display:block; width:100%; max-width:720px; height:auto; margin:0 auto; }
  .ch-empty{ padding:var(--sp4) 0; text-align:center; font-size:var(--fs-sm); }
  .ch-wrap{
    background:var(--s1); border:1px solid var(--line); border-radius:var(--r3);
    padding:var(--sp3) var(--sp4) var(--sp2); margin-bottom:var(--sp3);
  }
  .ch-wrap:last-child{ margin-bottom:0; }
  .ch-wrap .sub{ margin:0 0 var(--sp2); }

  /* ───── 响应式 ───── */
  @media (max-width:600px){
    :root{ --sp5:18px; --sp6:24px; --fs-xl:19px; --fs-2xl:26px; }
    .wrap{ padding:var(--sp3) 10px var(--sp5); }
    h1{ font-size:18px; }
    h2{ font-size:var(--fs-md); }
    h2 .meta{ font-size:var(--fs-xs); }
    /* 不再强制 3 列：auto-fit 会折叠空轨道，组内 3 张/4 张各自正好排成一行。
       强制 3 列时 7 张卡会排成 3+3+1，最后一张孤零零一行。 */
    .cards{ grid-template-columns:repeat(auto-fit,minmax(min(100%,104px),1fr)); gap:var(--sp2); }
    .card{ padding:var(--sp2) 10px; }
    .card .v{ font-size:var(--fs-lg); letter-spacing:-.2px; }
    .card .l{ font-size:var(--fs-xs); }
    /* 手机上 .tm(时间) 隐藏 → 剩 4 项。
       中间那格必须 minmax(0,1fr) 才能收缩，否则中文内容放不下会堆成两行。 */
    .row-sig{ grid-template-columns:34px minmax(0,1fr) auto auto; column-gap:7px; }
    .row-sig .pnl{ min-width:58px; font-size:var(--fs-xs); }
    .row-sig .tm{ display:none; }
    /* 日头：统计串独占一行并允许换行，否则最后一项会被挤出右边缘 */
    .dayhead{ row-gap:3px; padding:var(--sp2) var(--sp3); }
    .dayhead .sp{ flex:1 1 100%; margin-left:0; gap:9px; }
    .dayhead .dt{ font-size:var(--fs-md); }
    .daybody{ padding:var(--sp1) var(--sp3) var(--sp2); }
  }

  /* 尊重系统「减少动态效果」设置 */
  @media (prefers-reduced-motion:reduce){
    *{ animation-duration:.001ms !important; transition-duration:.001ms !important; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>⚡ XAUMonitor · DPB 黄金信号</h1>
    <div class="sub" id="sub" style="margin:0">加载中…</div>
  </header>

  <div id="cards"></div>
  <div id="stats" class="sub" style="margin:var(--sp3) 0 var(--sp5)"></div>

  <!-- 资金与盈亏（含持仓中实时浮动） -->
  <section>
    <h2>💰 资金与盈亏<span class="meta" id="eqSrc"></span></h2>
    <div id="eqCards"></div>
    <div id="posWrap" style="margin-top:var(--sp3)"></div>
  </section>

  <!-- 图表：权益曲线 / 逐日 / 按小时 -->
  <section>
    <h2>📈 走势<span class="meta">只统计已结案信号</span></h2>
    <div class="ch-wrap">
      <h3>累积权益曲线</h3>
      <div class="sub">初始资金 → 现在。按实测净 R 换算成美元累加，
        <b>虚线</b> = 初始资金基准。浮动盈亏不计入曲线（另在持仓区显示）。</div>
      <div id="eqChart"></div>
    </div>
    <div class="ch-wrap">
      <h3>逐日盈亏</h3>
      <div class="sub">每根柱子 = 该日已结案信号的美元合计。<span class="up">绿盈</span> / <span class="dn">红亏</span>。</div>
      <div id="dayChart"></div>
    </div>
    <div class="ch-wrap">
      <h3>按小时期望值</h3>
      <div class="sub">每根柱子 = 该小时全部已结案信号的<b>平均净 R</b>（样本 &lt;2 笔不画）。
        用于判断要不要开时段过滤 <code>block_hours</code> —— 长期为负才值得开，现在默认关。</div>
      <div id="hourChart"></div>
    </div>
    <div class="ch-wrap">
      <h3>信号分布</h3>
      <div id="distBox" class="sub" style="margin-bottom:0"></div>
    </div>
  </section>

  <div class="main">
    <!-- 左（视觉第一优先）：按日分区 + 分页 -->
    <section>
      <h2>📅 按日分区 · 信号与盈亏<span class="meta" id="dailyMeta"></span></h2>
      <div id="dailyWrap" class="sub">加载中…</div>
      <div class="pager" id="pager"></div>
    </section>

    <!-- 右：胜率 + 实时状态 -->
    <div class="col-b">
      <section>
        <h2>📊 结果追踪 · 胜率</h2>
        <div id="outCards"></div>
        <div id="outDetail" class="sub" style="margin:var(--sp3) 0 0"></div>
      </section>

      <section>
        <h2>📈 各周期实时状态</h2>
        <div class="tf-grid" id="tfGrid"></div>
      </section>
    </div>
  </div>

  <div class="foot">
    数据源 signals.db + dpb_status.json + paxg_stream.db<br>
    每 15 秒自动刷新 · 零 API 消耗
  </div>
</div>

<script>
const E = (t,c,x)=>{const e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e;};
const fmtN = v => (v==null?'-':v);
// 每日分区：分页状态必须放在 load() 外面，否则每次自动刷新都会跳回第 1 页
let dailyPage = 1, dailyPages = 1;
const DAY_PER_PAGE = 5;
const usd = v => (Number(v||0)>=0?'+':'') + Number(v||0).toFixed(2);
const usdAbs = v => Number(v||0).toFixed(2);
const cls = v => (Number(v||0)>=0 ? 'up' : 'dn');

async function load(){
  try{
    const [st, sy, oc, eq, dl, ch] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/outcomes').then(r=>r.json()).catch(()=>({closed:0})),
      fetch('/api/equity').then(r=>r.json()).catch(()=>({})),
      fetch('/api/daily?page='+dailyPage+'&per_page='+DAY_PER_PAGE)
        .then(r=>r.json()).catch(()=>({days:[],pages:1})),
      fetch('/api/charts').then(r=>r.json()).catch(()=>({})),
    ]);

    // 顶栏
    const snap = sy.snapshot||{};
    document.getElementById('sub').textContent =
      `${fmtN(sy.config.ticker)} · ${snap.updated_at?('快照更新 '+snap.updated_at):'暂无快照'} · 间隔 ${sy.config.check_interval_minutes} 分钟`;

    // 卡片
    // 数据发布窗口：命中时置顶警示（这是防爆仓的硬拦截）
    const nw = snap.news || {};
    // 顶栏分组渲染。
    // 优化前：7 张卡平铺进 3 列网格 → 排成 3+3+1，最后一张孤零零一行，很刺眼。
    // 现在按语义分「运行 / 信号评级」两组，每组各自成行（auto-fit 会折叠空轨道，
    // 组内卡片数 ≤ 能容纳的列数时天然是一整行）。
    const cw = document.getElementById('cards'); cw.innerHTML='';
    const cg=(label, items)=>{
      if(!items.length) return;
      const g=E('div','cgroup');
      if(label) g.appendChild(E('div','cglabel',label));
      const grid=E('div','cards');
      items.forEach(([l,v,cl])=>{
        const c=E('div','card'+(cl?' '+cl:''));
        c.appendChild(E('div','v',v));
        c.appendChild(E('div','l',l));
        grid.appendChild(c);
      });
      g.appendChild(grid); cw.appendChild(g);
    };
    const run = [];
    if(nw.blocked){
      run.push(['⛔ 数据窗口', nw.event||'禁开仓', 'warn']);
    } else if(nw.enabled && nw.upcoming){
      run.push(['下次数据', nw.upcoming.split(' ').slice(-1)[0], '']);
    }
    run.push(['信号总数', st.total, ''], ['今日', st.today, '']);
    cg('运行', run);
    cg('信号评级', [
      ['🔥 共振', st.resonance, ''],
      ['S 级', st.by_grade.S||0, 'gold'],
      ['A 级', st.by_grade.A||0, ''],
      ['B 级', st.by_grade.B||0, ''],
    ]);

    // 各周期状态
    const tg = document.getElementById('tfGrid'); tg.innerHTML='';
    const tfs = snap.timeframes||{};
    const keys = Object.keys(tfs);
    if(!keys.length){ tg.appendChild(E('div','muted','暂无状态快照（监控端首次循环后会写入）')); }
    Object.entries(tfs).forEach(([tf,d])=>{
      const box=E('div','tf');
      const nm=E('div','name');
      nm.appendChild(E('span',null,tf));
      nm.appendChild(E('span',null,d.trend||''));
      box.appendChild(nm);
      const add=(k,v)=>{const r=E('div','row');r.appendChild(E('span',null,k));r.appendChild(E('b',null,v));box.appendChild(r);};
      add('收盘/RSI/ATR', `${d.close} / ${d.rsi} / ${d.atr}`);
      if(d.adx!=null) add('ADX', `${d.adx} ${d.adx>=25?'强':d.adx<20?'弱':'中'}`);
      let sigTxt = d.signal===1?'🟢做多':d.signal===-1?'🔴做空':'—';
      if(d.signal!==0 && d.type) sigTxt += ` [${d.type}]`;
      if(d.signal!==0 && d.grade) sigTxt += ` ${d.grade}${d.score}/10`;
      add('信号', sigTxt);
      add('K线', d.bar_time);
      tg.appendChild(box);
    });

    // 结果追踪（胜率 / 期望值）
    const ow = document.getElementById('outCards'); ow.innerHTML='';
    const od = document.getElementById('outDetail'); od.textContent='';
    // 与「资金与盈亏」同一套分组渲染：分成「命中率 / 累计 R / 进度」三簇
    const mkg=(label, items)=>{
      const g=E('div','cgroup');
      if(label) g.appendChild(E('div','cglabel',label));
      const grid=E('div','cards');
      items.forEach(([l,v])=>{
        const c=E('div','card');
        c.appendChild(E('div','v',v));
        c.appendChild(E('div','l',l));
        grid.appendChild(c);
      });
      g.appendChild(grid); ow.appendChild(g);
    };
    if(!oc.closed){
      mkg('状态', [['已结案', 0], ['追踪中', oc.tracking||0]]);
      od.textContent = `暂无已结案信号（追踪中 ${oc.tracking||0} 笔）— 需先触及 SL/TP，或追踪窗口走满才算结案`;
    } else {
      mkg('命中率', [
        ['胜率', oc.win_rate.toFixed(1)+'%'],
        ['期望值', (oc.expectancy_r>=0?'+':'')+oc.expectancy_r.toFixed(2)+'R'],
        ['盈亏因子', oc.profit_factor==null?'∞':oc.profit_factor.toFixed(2)],
      ]);
      mkg('累计 R', [
        ['净值', (oc.total_r>=0?'+':'')+oc.total_r.toFixed(2)+'R'],
        ['毛值', ((oc.gross_total_r||0)>=0?'+':'')+(oc.gross_total_r||0).toFixed(2)+'R'],
        ['点差成本', '-'+(oc.cost_total_r||0).toFixed(2)+'R'],
      ]);
      mkg('进度', [['已结案', oc.closed], ['追踪中', oc.tracking||0]]);
      const agg=(obj,label)=>{const ks=Object.keys(obj||{});if(!ks.length)return '';
        return ` · ${label}: `+ks.map(k=>`${k} ${obj[k].wins}/${obj[k].n}(${obj[k].r>=0?'+':''}${obj[k].r.toFixed(1)}R)`).join('  ');};
      const oc2 = oc.by_outcome ? ' · 结局: '+Object.entries(oc.by_outcome).map(([k,v])=>`${k} ${v}`).join('  ') : '';
      const costNote = oc.cost_total_r ? ` · 点差每笔均 ${(oc.avg_cost_r||0).toFixed(3)}R(已扣在净值里)` : '';
      od.textContent = `胜 ${oc.wins} / 负 ${oc.losses} · 追踪中 ${oc.tracking||0}` + oc2 + costNote
        + agg(oc.by_grade,'按等级') + agg(oc.by_type,'按类型') + agg(oc.by_timeframe,'按周期');
    }

    // 统计
    const parts=[];
    const dict = o => Object.entries(o).map(([k,v])=>`${k}:${v}`).join('  ');
    parts.push(`按周期 — ${dict(st.by_timeframe)}`);
    parts.push(`按方向 — ${dict(st.by_direction)}`);
    parts.push(`按类型 — ${dict(st.by_type)}`);
    const sd=document.getElementById('stats'); sd.innerHTML='';
    parts.forEach(p=>sd.appendChild(E('div',null,p)));
    sd.appendChild(E('div',null,'配置 — trade_freq='+sy.config.trade_freq+`  trend_stability=${sy.config.trend_stability}  min_grade=${sy.config.min_signal_grade}  共振阈值=${sy.config.resonance_min_count}  突破SL=${sy.config.breakout_sl_atr}×ATR`));

    // ================= 资金与盈亏 =================
    const ew = document.getElementById('eqCards'); ew.innerHTML='';
    document.getElementById('eqSrc').textContent = eq.live_price
      ? `现价 ${Number(eq.live_price).toFixed(2)}（${eq.live_source||''}）· 每笔 ${eq.oz_per_trade} 盎司=$${eq.oz_per_trade}/美元波动`
      : '';
    if(eq.initial_equity!=null){
      const tot=eq.equity_total, real=eq.realized_usd, un=eq.unrealized_usd;
      const net=real+un, base=eq.initial_equity;
      // 分组渲染：优化前 9 张同权重卡片平铺成一条"数字墙"，扫读时找不到重点。
      // 现在按语义分 3 簇 —— 先看「账户规模」，再看「盈亏构成」，最后看「表现」。
      const group=(label, items)=>{
        const g=E('div','cgroup');
        if(label) g.appendChild(E('div','cglabel',label));
        const grid=E('div','cards');
        items.forEach(([l,v,c])=>{
          const d=E('div','card'+(c?' '+c:''));
          d.appendChild(E('div','v',v));
          d.appendChild(E('div','l',l));
          grid.appendChild(d);
        });
        g.appendChild(grid); ew.appendChild(g);
      };
      group('账户', [
        ['初始资金', Number(base).toFixed(0), ''],
        ['当前权益', Number(tot).toFixed(2), tot>=base?'good':'bad'],
        ['累计盈亏', usd(net), net>=0?'good':'bad'],
      ]);
      group('盈亏构成', [
        ['已实现', usd(real), real>=0?'good':'bad'],
        ['持仓浮动', usd(un), un>=0?'good':'bad'],
      ]);
      group('表现', [
        ['收益率', (eq.return_pct>=0?'+':'')+Number(eq.return_pct).toFixed(2)+'%',
           eq.return_pct>=0?'good':'bad'],
        ['累计 R', (eq.realized_r>=0?'+':'')+Number(eq.realized_r).toFixed(2)+'R', ''],
        ['胜率', eq.win_rate==null?'-':Number(eq.win_rate).toFixed(1)+'%', ''],
        ['结案 / 追踪', eq.closed+' / '+eq.tracking, ''],
      ]);
    }

    // ================= 图表（服务端生成的 SVG，直接注入） =================
    const OUT_LBL={SL:'❌止损', TP1:'✅TP1', TP2:'🏆TP2', EXP:'⏱超时', open:'⏳持仓中'};
    const eqc=document.getElementById('eqChart');
    const dyc=document.getElementById('dayChart');
    const hrc=document.getElementById('hourChart');
    const dbox=document.getElementById('distBox');
    if(ch && ch.equity_svg){
      eqc.innerHTML=ch.equity_svg;
      dyc.innerHTML=ch.daily_svg;
      hrc.innerHTML=ch.hour_svg;
      const d=ch.dist||{};
      const kv=(o,lab)=>{const e=Object.entries(o||{});
        if(!e.length) return '—';
        return e.sort((a,b)=>b[1]-a[1])
                .map(([k,v])=>(lab?lab(k):k)+' '+v).join(' · ');};
      dbox.innerHTML =
        '<b>结局</b> '   + kv(d.outcome, k=>OUT_LBL[k]||k) + '<br>'
      + '<b>类型</b> '   + kv(d.type)  + '<br>'
      + '<b>等级</b> '   + kv(d.grade) + '<br>'
      + '<b>周期</b> '   + kv(d.tf);
    }else{
      eqc.innerHTML='<div class="muted ch-empty">图表数据加载失败（看服务日志）</div>';
      dyc.innerHTML=''; hrc.innerHTML=''; dbox.textContent='—';
    }

    // ================= 持仓中（实时浮动） =================
    const pw = document.getElementById('posWrap'); pw.innerHTML='';
    const pos = eq.open_positions||[];
    if(pos.length){
      const t=E('div','muted','⏳ 持仓中 '+pos.length+' 笔 · 按实时价估算（'+(eq.live_source||'')+'）');
      t.style.marginBottom='7px'; pw.appendChild(t);
      pos.forEach(p=>{
        const box=E('div','pos'), tp=E('div','t');
        if(p.grade) tp.appendChild(E('span','pill '+p.grade, p.grade));
        tp.appendChild(E('span',null,p.timeframe));
        tp.appendChild(E('span', p.direction>0?'dir-long':'dir-short',
                          p.direction>0?'🟢做多':'🔴做空'));
        if(p.sig_type) tp.appendChild(E('span','muted',p.sig_type));
        tp.appendChild(E('span','pnl '+cls(p.float_pnl_usd),
          usd(p.float_pnl_usd)+' U ('+usd(p.float_r)+'R)'));
        box.appendChild(tp);
        const g=E('div','muted');
        g.style.cssText='font-size:13px;margin-top:3px';
        g.appendChild(E('span',null,
          `入 ${Number(p.entry).toFixed(2)} · 损 ${Number(p.sl).toFixed(2)}`
          + ` · TP1 ${Number(p.tp1).toFixed(2)} · TP2 ${Number(p.tp2).toFixed(2)}`
          + ` · 风险 ${usdAbs(p.risk_usd)} U · 现价 ${p.live_price?Number(p.live_price).toFixed(2):'-'}`));
        box.appendChild(g);
        // 仓位进度条：入场→止损 之间，标出现价位置
        const lo=Math.min(Number(p.entry),Number(p.sl)), hi=Math.max(Number(p.entry),Number(p.sl));
        const span=(hi-lo)||1;
        const bar=E('div','bar');
        const mark=document.createElement('i');
        let frac=(Number(p.live_price||p.entry)-lo)/span;
        frac=Math.max(0,Math.min(1,frac));
        mark.style.left=(frac*100).toFixed(1)+'%';
        bar.appendChild(mark); box.appendChild(bar);
        pw.appendChild(box);
      });
    }

    // ================= 按日分区 + 分页 =================
    dailyPages = dl.pages||1; dailyPage = dl.page||1;
    const dw=document.getElementById('dailyWrap'); dw.innerHTML='';
    const dlist = dl.days||[];
    // 区块标题右侧的元信息：把"一共多少天 / 哪一段"放在标题行，
    // 不用滑到分页器才能知道自己在第几页
    const dm=document.getElementById('dailyMeta');
    if(dm) dm.textContent = dlist.length
      ? `${dl.total_days||dlist.length} 天 · 第 ${dailyPage}/${dl.pages||1} 页`
      : '';
    if(!dlist.length){
      dw.appendChild(E('div','sub','暂无信号记录'));
    }
    dlist.forEach((day, idx)=>{
      const box=E('div','day');
      if(idx>0) box.classList.add('collapsed');       // 默认只展开最新一天
      const h=E('div','dayhead');
      h.appendChild(E('span','caret', idx>0?'▶':'▼'));
      h.appendChild(E('span','dt', day.date));
      const sp=E('span','sp');
      const bcl=(v)=>'<b class="'+(v>=0?'up':'dn')+'">'+(v>=0?'+':'')+Number(v).toFixed(2)+'</b>';
      sp.innerHTML = '<span>信号 <b>'+day.n+'</b></span>'
        + '<span>胜<b class="up">'+day.wins+'</b>/负<b class="dn">'+day.losses+'</b></span>'
        + '<span>当日 '+bcl(day.day_r)+'R</span>'
        + '<span>金额 '+bcl(day.day_usd)+'U</span>'
        + '<span>权益 <b>'+Number(day.equity_end).toFixed(2)+'</b></span>';
      h.appendChild(sp);
      h.onclick=()=>{
        box.classList.toggle('collapsed');
        const c=h.querySelector('.caret');
        if(c) c.textContent = box.classList.contains('collapsed')?'▶':'▼';
      };
      box.appendChild(h);

      const b=E('div','daybody');
      // EXP = 超时结案：窗口走满、既没碰止损也没碰止盈的死单（按末根收盘价计R）
      const OUT2={SL:['❌止损','dn'], TP1:['✅TP1','up'], TP2:['🏆TP2','up'],
                  EXP:['⏱超时','mu'], open:['⏳持仓','']};
      (day.signals||[]).slice().reverse().forEach(s=>{
        const r=E('div','row-sig');
        r.appendChild(E('span','id','#'+s.id));
        r.appendChild(E('span','tm', (s.pushed_at||'').slice(11,16)+' '+s.timeframe));
        const mid=E('span','mid');
        mid.appendChild(E('span', s.direction>0?'dir-long':'dir-short', s.direction>0?'多':'空'));
        if(s.sig_type) mid.appendChild(E('span','muted',s.sig_type));
        if(s.grade) mid.appendChild(E('span','pill '+s.grade, s.grade));
        r.appendChild(mid);
        const o=s.outcome||'?';
        const ot=OUT2[o]||[o,''];
        let rt=ot[0];
        if(s.r_multiple!=null) rt+=' '+(s.r_multiple>=0?'+':'')+Number(s.r_multiple).toFixed(2)+'R';
        const res=E('span','res '+(s.closed?ot[1]:''), rt);
        if(!s.closed) res.classList.add('live');
        r.appendChild(res);
        r.appendChild(E('span','pnl '+cls(s.pnl_usd), usd(s.pnl_usd)));
        b.appendChild(r);
      });
      box.appendChild(b);
      dw.appendChild(box);
    });

    // 分页控件
    const pg=document.getElementById('pager'); pg.innerHTML='';
    const mkBtn=(txt,dis,fn)=>{
      const btn=document.createElement('button');
      btn.textContent=txt; btn.disabled=dis; btn.onclick=fn; pg.appendChild(btn);
    };
    mkBtn('‹ 上一页', dailyPage<=1, ()=>{ if(dailyPage>1){ dailyPage--; load(); } });
    pg.appendChild(E('span','pg', '第 '+dailyPage+' / '+dailyPages+' 页'));
    mkBtn('下一页 ›', dailyPage>=dailyPages, ()=>{ if(dailyPage<dailyPages){ dailyPage++; load(); } });
  }catch(e){
    document.getElementById('sub').textContent='加载失败: '+e;
  }
}
load(); setInterval(load, 15000);   // 15 秒刷新：持仓浮动跟着实时价走
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    cfg = _cfg()
    port = int(cfg.get("dashboard_port", 1689))
    uvicorn.run(app, host="0.0.0.0", port=port)
