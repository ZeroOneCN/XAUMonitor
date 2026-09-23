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
import sqlite3
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

BASE = Path(__file__).parent


def _cfg() -> dict:
    try:
        with open(BASE / "dpb_config.json", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
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
        except Exception:
            pass
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


app = FastAPI(title="XAUMonitor Dashboard")


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


def _pnl_usd(r: dict, oz: float) -> float:
    """单笔盈亏（美元）。

    net_r 是「以止损距离为 1R」的净倍数（已扣点差），
    所以 价格波动 = net_r × 止损距离，盈亏 = 价格波动 × 盎司数。
    """
    try:
        rr = float(r.get("r_multiple") or 0.0)
        entry = float(r.get("entry") or 0.0)
        sl = float(r.get("sl") or 0.0)
        risk = abs(entry - sl)
        return rr * risk * oz
    except Exception:
        return 0.0


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
        except Exception:
            pass
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

    realized = sum(x["pnl_usd"] for x in closed)
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
        r["pnl_usd"] = _pnl_usd(r, oz)
        r["closed"] = _is_closed(r)
        days_map[d].append(r)

    # 按时间顺序累计（累计口径必须时间正序，否则曲线会错）
    cum = init
    days = []
    for d in order:
        sigs = days_map[d]
        closed = [x for x in sigs if x["closed"]]
        day_real = sum(x["pnl_usd"] for x in closed)
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
  :root { --bg:#0d1117; --card:#161b22; --border:#30363d; --fg:#e9eef5; --mut:#9aa7b6;
          --green:#3fb950; --red:#f85149; --gold:#e3b341; --blue:#58a6ff; --fs:17px; }
  * { box-sizing:border-box; -webkit-text-size-adjust:100%; }
  body { margin:0; background:var(--bg); color:var(--fg); font-size:var(--fs); line-height:1.5;
         font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif; }
  .wrap { max-width:1240px; margin:0 auto; padding:12px 12px 26px; }
  header { display:flex; align-items:baseline; gap:12px; flex-wrap:wrap; margin-bottom:10px; }
  h1 { font-size:21px; margin:0; letter-spacing:.3px; white-space:nowrap; }
  h2 { font-size:16px; margin:0 0 9px; padding-left:8px; border-left:4px solid var(--blue); }
  .sub { color:var(--mut); font-size:13px; }
  section { margin-bottom:14px; }
  /* 顶部指标条：紧凑，一屏放得下 */
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(104px,1fr)); gap:8px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:10px;
          padding:9px 12px; min-width:0; }
  .card .v { font-size:23px; font-weight:800; line-height:1.15;
              white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
  .card .l { color:var(--mut); font-size:12px; margin-top:1px; }
  /* 数据发布窗口告警卡：必须一眼可见 */
  .card.warn { background:#3d1d1d; border-color:#f85149; }
  .card.warn .v { color:#ff7b72; font-size:17px; }
  .card.warn .l { color:#ff9a92; }
  /* 主区：桌面左右双列，信号在左（视觉第一优先） */
  .main { display:grid; gap:14px; align-items:start; }
  .col-b > section:last-child { margin-bottom:0; }
  @media (min-width:960px){
    .main { grid-template-columns:minmax(0,1.3fr) minmax(0,1fr); }
  }
  /* 各周期状态：紧凑卡片网格 */
  .tf-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(158px,1fr)); gap:8px; }
  .tf { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:9px 11px; }
  .tf .name { font-size:16px; font-weight:800; margin-bottom:5px; display:flex; justify-content:space-between; align-items:baseline; }
  .tf .name span { font-size:12px; font-weight:600; color:var(--mut); }
  .tf .row { display:flex; justify-content:space-between; gap:8px; font-size:13px; color:var(--mut); padding:1px 0; }
  .tf .row b { color:var(--fg); font-weight:700; }
  .sig { background:var(--card); border:1px solid var(--border); border-radius:12px;
         padding:11px 13px; margin-bottom:9px; }
  .sig .top { display:flex; align-items:center; gap:9px; flex-wrap:wrap; margin-bottom:8px; }
  .sig .tfname { font-size:18px; font-weight:800; }
  .sig .time { color:var(--mut); font-size:13px; margin-left:auto; }
  .sig .grid { display:grid; grid-template-columns:repeat(2,1fr); gap:6px 14px; }
  .sig .kv { font-size:15px; color:var(--mut); display:flex; justify-content:space-between; gap:8px; }
  .sig .kv b { color:var(--fg); font-weight:800; }
  .dir-long { color:var(--green); font-weight:800; }
  .dir-short { color:var(--red); font-weight:800; }
  .pill { display:inline-block; padding:2px 9px; border-radius:11px; font-size:13px; font-weight:800; }
  .S{background:#e3b341;color:#000} .A{background:#3fb950;color:#000}
  .B{background:#58a6ff;color:#000} .C{background:#8a94a6;color:#000}
  .fire{color:var(--gold);font-weight:800}
  .muted{color:var(--mut);font-size:14px}
  .big{font-size:19px;font-weight:800}
  .foot{text-align:center;padding:8px 0 18px;font-size:12px;color:var(--mut)}
  /* 资金与盈亏 */
  .card.good .v{ color:var(--green); } .card.bad .v{ color:var(--red); }
  /* 按日分区 */
  .day { background:var(--card); border:1px solid var(--border); border-radius:12px;
         margin-bottom:10px; overflow:hidden; }
  .dayhead { display:flex; align-items:center; gap:10px; flex-wrap:wrap;
             padding:10px 13px; background:#1b2230; border-bottom:1px solid var(--border);
             cursor:pointer; user-select:none; }
  .dayhead .dt { font-size:17px; font-weight:800; }
  .dayhead .caret { color:var(--mut); font-size:12px; }
  .dayhead .sp { margin-left:auto; display:flex; gap:11px; flex-wrap:wrap;
                 font-size:13px; color:var(--mut); }
  .dayhead .sp b { font-weight:800; }
  .up{ color:var(--green); } .dn{ color:var(--red); }
  .daybody { padding:6px 13px 10px; }
  .day.collapsed .daybody { display:none; }
  .row-sig { display:grid; grid-template-columns:38px 74px 1fr auto auto;
             gap:8px; align-items:center; font-size:14px; padding:5px 0;
             border-bottom:1px dashed #232a35; }
  .row-sig:last-child { border-bottom:0; }
  .row-sig .id { color:var(--mut); font-size:12px; }
  .row-sig .mid { display:flex; gap:7px; align-items:center; flex-wrap:wrap; }
  .row-sig .res { font-size:13px; font-weight:700; }
  .row-sig .pnl { text-align:right; font-weight:800; min-width:78px; }
  .row-sig .tm { color:var(--mut); font-size:12px; }
  .live { animation:pulse 1.6s ease-in-out infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.45} }
  /* 持仓中 */
  .pos { background:#1b2230; border:1px solid var(--border); border-radius:10px;
         padding:9px 12px; margin-bottom:7px; }
  .pos .t { display:flex; gap:9px; align-items:center; flex-wrap:wrap; font-size:14px; }
  .pos .t .pnl { margin-left:auto; font-weight:800; font-size:16px; }
  .pos .bar { height:5px; border-radius:3px; background:#30363d; margin-top:8px; position:relative; }
  .pos .bar i { position:absolute; top:-3px; width:3px; height:11px; background:var(--gold);
                border-radius:2px; }
  /* 分页 */
  .pager { display:flex; align-items:center; justify-content:center; gap:12px; margin:12px 0 2px; }
  .pager button { background:var(--card); color:var(--fg); border:1px solid var(--border);
                  border-radius:8px; padding:8px 16px; font-size:15px; font-weight:700; cursor:pointer; }
  .pager button:disabled { opacity:.3; cursor:default; }
  .pager .pg { color:var(--mut); font-size:14px; min-width:96px; text-align:center; }
  @media (max-width:600px){
    h1{ font-size:19px; }
    .wrap{ padding:10px 9px 22px; }
    .cards{ grid-template-columns:repeat(3,minmax(0,1fr)); gap:6px; }
    .card{ padding:8px 9px; }
    .card .v{ font-size:17px; }
    .card .l{ font-size:11px; }
    /* 手机上 .tm(时间)被隐藏 → 剩 4 项。原来给"方向/类型/等级"那格只留 62px，
       内容放不下就换行堆叠成两行。改成让中间那格吃掉剩余空间，其余按内容自适应。 */
    .row-sig{ grid-template-columns:34px minmax(0,1fr) auto auto; column-gap:7px; row-gap:0; }
    .row-sig .pnl{ min-width:58px; font-size:13px; }
    .row-sig .tm{ display:none; }
    /* 日头：让统计串独占一行并换行，否则最后一项会被挤出右边缘 */
    .dayhead{ row-gap:3px; }
    .dayhead .sp{ flex:1 1 100%; margin-left:0; gap:9px; font-size:12px; }
    .dayhead .dt{ font-size:16px; }
  }
</style>
</head>
<body>
<div class="wrap">
  <header>
    <h1>⚡ XAUMonitor · DPB 黄金信号</h1>
    <div class="sub" id="sub">加载中…</div>
  </header>

  <div class="cards" id="cards"></div>
  <div id="stats" class="muted" style="margin:9px 0 13px"></div>

  <!-- 资金与盈亏（含持仓中实时浮动） -->
  <section>
    <h2>💰 资金与盈亏（美元） <span class="muted" id="eqSrc"
        style="font-weight:400;font-size:13px"></span></h2>
    <div class="cards" id="eqCards"></div>
    <div id="posWrap" style="margin-top:10px"></div>
  </section>

  <div class="main">
    <!-- 左（第一优先）：按日分区 + 分页 -->
    <section>
      <h2>📅 按日分区 · 信号与盈亏</h2>
      <div id="dailyWrap" class="muted">加载中…</div>
      <div class="pager" id="pager"></div>
    </section>

    <!-- 右：胜率 + 实时状态 -->
    <div class="col-b">
      <section>
        <h2>📊 结果追踪 · 胜率</h2>
        <div class="cards" id="outCards"></div>
        <div id="outDetail" class="muted" style="margin-top:10px"></div>
      </section>

      <section>
        <h2>📈 各周期实时状态</h2>
        <div class="tf-grid" id="tfGrid"></div>
      </section>
    </div>
  </div>

  <div class="foot">
    数据源 signals.db + dpb_status.json + paxg_stream.db · 每 15 秒自动刷新 · 零 API 消耗
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
    const [st, sy, oc, eq, dl] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/outcomes').then(r=>r.json()).catch(()=>({closed:0})),
      fetch('/api/equity').then(r=>r.json()).catch(()=>({})),
      fetch('/api/daily?page='+dailyPage+'&per_page='+DAY_PER_PAGE)
        .then(r=>r.json()).catch(()=>({days:[],pages:1})),
    ]);

    // 顶栏
    const snap = sy.snapshot||{};
    document.getElementById('sub').textContent =
      `${fmtN(sy.config.ticker)} · ${snap.updated_at?('快照更新 '+snap.updated_at):'暂无快照'} · 间隔 ${sy.config.check_interval_minutes} 分钟`;

    // 卡片
    // 数据发布窗口：命中时置顶警示（这是防爆仓的硬拦截）
    const nw = snap.news || {};
    const cards = [];
    if(nw.blocked){
      cards.push(['⛔ 数据窗口', nw.event||'禁开仓', 'warn']);
    } else if(nw.enabled && nw.upcoming){
      cards.push(['下次数据', nw.upcoming.split(' ').slice(-1)[0], '']);
    }
    cards.push(
      ['信号总数', st.total, ''], ['今日', st.today, ''],
      ['🔥共振', st.resonance, ''], ['S级', st.by_grade.S||0, 'gold'],
      ['A级', st.by_grade.A||0, ''], ['B级', st.by_grade.B||0, ''],
    );
    const cw = document.getElementById('cards'); cw.innerHTML='';
    cards.forEach(([l,v,cls])=>{
      const c=E('div','card'+(cls?' '+cls:''));
      c.appendChild(E('div','v',v)); c.appendChild(E('div','l',l)); cw.appendChild(c);
    });

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
    if(!oc.closed){
      const mk=(l,v)=>{const c=E('div','card');c.appendChild(E('div','v',v));c.appendChild(E('div','l',l));ow.appendChild(c);};
      mk('已结案', 0); mk('追踪中', oc.tracking||0);
      od.textContent = `暂无已结案信号（追踪中 ${oc.tracking||0} 笔）— 需先触及 SL/TP，或追踪窗口走满才算结案`;
    } else {
      const cards2 = [
        ['胜率', oc.win_rate.toFixed(1)+'%', ''],
        ['期望值', (oc.expectancy_r>=0?'+':'')+oc.expectancy_r.toFixed(2)+'R', ''],
        ['净值R', (oc.total_r>=0?'+':'')+oc.total_r.toFixed(2)+'R', ''],
        ['毛值R', ((oc.gross_total_r||0)>=0?'+':'')+(oc.gross_total_r||0).toFixed(2)+'R', ''],
        ['点差成本', '-'+(oc.cost_total_r||0).toFixed(2)+'R', ''],
        ['盈亏因子', oc.profit_factor==null?'∞':oc.profit_factor.toFixed(2), ''],
        ['已结案', oc.closed, ''],
        ['追踪中', oc.tracking||0, ''],
      ];
      cards2.forEach(([l,v])=>{const c=E('div','card');c.appendChild(E('div','v',v));c.appendChild(E('div','l',l));ow.appendChild(c);});
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
      const mk=(l,v,c)=>{const d=E('div','card'+(c?' '+c:''));
        d.appendChild(E('div','v',v)); d.appendChild(E('div','l',l)); ew.appendChild(d);};
      mk('初始资金', Number(base).toFixed(0), '');
      mk('当前权益', Number(tot).toFixed(2), tot>=base?'good':'bad');
      mk('累计盈亏', usd(net), net>=0?'good':'bad');
      mk('已实现', usd(real), real>=0?'good':'bad');
      mk('持仓浮动', usd(un), un>=0?'good':'bad');
      mk('收益率', (eq.return_pct>=0?'+':'')+Number(eq.return_pct).toFixed(2)+'%',
         eq.return_pct>=0?'good':'bad');
      mk('累计R', (eq.realized_r>=0?'+':'')+Number(eq.realized_r).toFixed(2)+'R', '');
      mk('胜率', eq.win_rate==null?'-':Number(eq.win_rate).toFixed(1)+'%', '');
      mk('结案/追踪', eq.closed+' / '+eq.tracking, '');
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
    if(!dlist.length){
      dw.appendChild(E('div','muted','暂无信号记录'));
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
      const OUT2={SL:['❌止损','dn'], TP1:['✅TP1','up'], TP2:['🏆TP2','up'], open:['⏳持仓','']};
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
