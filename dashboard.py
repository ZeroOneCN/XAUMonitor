#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XAUMonitor Web 仪表盘（B9）

读取 signals.db + dpb_status.json，零 API 消耗展示：
  - 信号统计（总数 / 等级分布 / 周期分布 / 方向 / 今日）
  - 最近信号明细表
  - 各周期实时状态快照（趋势 / RSI / ATR / 信号）
  - 配置摘要

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
    except Exception:
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
        "SELECT id, ts, pushed_at, timeframe, direction, sig_type, grade, score,"
        " band, entry, sl, tp1, tp2, rsi, atr, resonance"
        " FROM signals ORDER BY id DESC LIMIT ?",
        (int(limit),),
    )
    return {"count": len(rows), "signals": rows}


@app.get("/api/outcomes")
def api_outcomes():
    """结果追踪统计：胜率 / 期望值 / 盈亏因子（做单策略的验证闭环）"""
    rows = _query(
        "SELECT o.outcome, o.r_multiple, o.bars, o.mfe, o.mae,"
        " s.grade, s.sig_type, s.timeframe, s.direction, s.id"
        " FROM signal_outcomes o JOIN signals s ON s.id = o.signal_id"
        " ORDER BY s.id"
    )
    closed = [r for r in rows if r["outcome"] in ("SL", "TP1", "TP2")]
    open_n = len(rows) - len(closed)
    if not closed:
        return {"closed": 0, "open": open_n, "win_rate": None, "expectancy_r": None,
                "profit_factor": None, "total_r": 0.0, "wins": 0, "losses": 0,
                "by_grade": {}, "by_type": {}, "by_timeframe": {}, "recent": []}

    rs = [float(r["r_multiple"] or 0) for r in closed]
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
        "open": open_n,
        "wins": len(wins),
        "losses": len(closed) - len(wins),
        "win_rate": len(wins) / len(closed) * 100,
        "expectancy_r": sum(rs) / len(closed),
        "total_r": sum(rs),
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
  .wrap { max-width:840px; margin:0 auto; padding:14px 12px 30px; }
  h1 { font-size:22px; margin:0 0 2px; letter-spacing:.5px; }
  h2 { font-size:18px; margin:0 0 10px; padding-left:9px; border-left:4px solid var(--blue); }
  .sub { color:var(--mut); font-size:14px; margin-bottom:16px; }
  section { margin-bottom:20px; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(148px,1fr)); gap:10px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px 16px; }
  .card .v { font-size:30px; font-weight:800; line-height:1.15; }
  .card .l { color:var(--mut); font-size:14px; margin-top:2px; }
  .tf-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(250px,1fr)); gap:10px; }
  .tf { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px 16px; }
  .tf .name { font-size:19px; font-weight:800; margin-bottom:8px; }
  .tf .row { display:flex; justify-content:space-between; gap:10px; font-size:16px; color:var(--mut); padding:2px 0; }
  .tf .row b { color:var(--fg); font-weight:700; }
  .sig { background:var(--card); border:1px solid var(--border); border-radius:12px;
         padding:13px 15px; margin-bottom:10px; }
  .sig .top { display:flex; align-items:center; gap:9px; flex-wrap:wrap; margin-bottom:10px; }
  .sig .tfname { font-size:19px; font-weight:800; }
  .sig .time { color:var(--mut); font-size:14px; margin-left:auto; }
  .sig .grid { display:grid; grid-template-columns:repeat(2,1fr); gap:8px 14px; }
  .sig .kv { font-size:16px; color:var(--mut); display:flex; justify-content:space-between; gap:8px; }
  .sig .kv b { color:var(--fg); font-weight:800; }
  .dir-long { color:var(--green); font-weight:800; }
  .dir-short { color:var(--red); font-weight:800; }
  .pill { display:inline-block; padding:2px 9px; border-radius:11px; font-size:14px; font-weight:800; }
  .S{background:#e3b341;color:#000} .A{background:#3fb950;color:#000}
  .B{background:#58a6ff;color:#000} .C{background:#8a94a6;color:#000}
  .fire{color:var(--gold);font-weight:800}
  .muted{color:var(--mut);font-size:15px}
  .big{font-size:20px;font-weight:800}
  @media (max-width:430px){
    :root{ --fs:16px; }
    .wrap{ padding:12px 10px 26px; }
    .cards{ grid-template-columns:repeat(2,1fr); }
    .card .v{ font-size:26px; }
    .sig .grid{ grid-template-columns:1fr; }
    h1{ font-size:20px; }
  }
</style>
</head>
<body>
<div class="wrap">
  <h1>⚡ XAUMonitor · DPB 黄金信号监控</h1>
  <div class="sub" id="sub">加载中…</div>

  <div class="cards" id="cards"></div>

  <section>
    <h2>各周期实时状态</h2>
    <div class="tf-grid" id="tfGrid"></div>
  </section>

  <section>
    <h2>信号统计</h2>
    <div id="stats" class="muted"></div>
  </section>

  <section>
    <h2>结果追踪 · 胜率</h2>
    <div class="cards" id="outCards"></div>
    <div id="outDetail" class="muted" style="margin-top:12px"></div>
  </section>

  <section>
    <h2>最近信号</h2>
    <div id="sigWrap" class="muted">加载中…</div>
  </section>

  <div class="muted" style="text-align:center;padding:10px 0 30px;font-size:12px">
    数据源 signals.db + dpb_status.json · 每 30 秒自动刷新 · 零 API 消耗
  </div>
</div>

<script>
const E = (t,c,x)=>{const e=document.createElement(t);if(c)e.className=c;if(x!=null)e.textContent=x;return e;};
const fmtN = v => (v==null?'-':v);

async function load(){
  try{
    const [st, sg, sy, oc] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals?limit=60').then(r=>r.json()),
      fetch('/api/status').then(r=>r.json()),
      fetch('/api/outcomes').then(r=>r.json()).catch(()=>({closed:0})),
    ]);

    // 顶栏
    const snap = sy.snapshot||{};
    document.getElementById('sub').textContent =
      `${fmtN(sy.config.ticker)} · ${snap.updated_at?('快照更新 '+snap.updated_at):'暂无快照'} · 间隔 ${sy.config.check_interval_minutes} 分钟`;

    // 卡片
    const cards = [
      ['信号总数', st.total, ''], ['今日', st.today, ''],
      ['🔥共振', st.resonance, ''], ['S级', st.by_grade.S||0, 'gold'],
      ['A级', st.by_grade.A||0, ''], ['B级', st.by_grade.B||0, ''],
    ];
    const cw = document.getElementById('cards'); cw.innerHTML='';
    cards.forEach(([l,v])=>{const c=E('div','card');c.appendChild(E('div','v',v));c.appendChild(E('div','l',l));cw.appendChild(c);});

    // 各周期状态
    const tg = document.getElementById('tfGrid'); tg.innerHTML='';
    const tfs = snap.timeframes||{};
    const keys = Object.keys(tfs);
    if(!keys.length){ tg.appendChild(E('div','muted','暂无状态快照（监控端首次循环后会写入）')); }
    Object.entries(tfs).forEach(([tf,d])=>{
      const box=E('div','tf');
      box.appendChild(E('div','name',tf));
      const add=(k,v)=>{const r=E('div','row');r.appendChild(E('span',null,k));r.appendChild(E('b',null,v));box.appendChild(r);};
      add('趋势', d.trend); add('收盘', d.close); add('RSI', d.rsi); add('ATR', d.atr);
      let sigTxt = d.signal===1?'🟢做多':d.signal===-1?'🔴做空':'—';
      if(d.signal!==0 && d.type) sigTxt += ` [${d.type}]`;
      if(d.signal!==0 && d.grade) sigTxt += ` ${d.grade}级${d.score}/8`;
      add('信号', sigTxt);
      add('动能', d.vol_ok?'✓':'✗');
      add('K线时间', d.bar_time);
      tg.appendChild(box);
    });

    // 结果追踪（胜率 / 期望值）
    const ow = document.getElementById('outCards'); ow.innerHTML='';
    const od = document.getElementById('outDetail'); od.textContent='';
    if(!oc.closed){
      od.textContent = `暂无已结案信号（追踪中 ${oc.open||0} 笔）— 需价格先触及 SL 或 TP 才会有结果`;
    } else {
      const cards2 = [
        ['胜率', oc.win_rate.toFixed(1)+'%', ''],
        ['期望值', (oc.expectancy_r>=0?'+':'')+oc.expectancy_r.toFixed(2)+'R', ''],
        ['累计R', (oc.total_r>=0?'+':'')+oc.total_r.toFixed(2)+'R', ''],
        ['盈亏因子', oc.profit_factor==null?'∞':oc.profit_factor.toFixed(2), ''],
        ['已结案', oc.closed, ''],
        ['追踪中', oc.open||0, ''],
      ];
      cards2.forEach(([l,v])=>{const c=E('div','card');c.appendChild(E('div','v',v));c.appendChild(E('div','l',l));ow.appendChild(c);});
      const agg=(obj,label)=>{const ks=Object.keys(obj||{});if(!ks.length)return '';
        return ` · ${label}: `+ks.map(k=>`${k} ${obj[k].wins}/${obj[k].n}(${obj[k].r>=0?'+':''}${obj[k].r.toFixed(1)}R)`).join('  ');};
      od.textContent = `胜 ${oc.wins} / 负 ${oc.losses}`
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

    // 信号卡片列表（移动端友好，不横向滚动）
    const sw=document.getElementById('sigWrap'); sw.innerHTML='';
    if(!sg.signals.length){ sw.appendChild(E('div','muted','暂无信号记录')); return; }
    const num = v => (v==null||v==='')?'-':(typeof v==='number'?v.toFixed(2):v);
    sg.signals.forEach(s=>{
      const box=E('div','sig');
      const top=E('div','top');
      top.appendChild(E('span','tfname', s.timeframe));
      top.appendChild(E('span', s.direction===1?'dir-long':'dir-short', s.direction===1?'🟢 做多':'🔴 做空'));
      if(s.sig_type) top.appendChild(E('span','muted', s.sig_type));
      if(s.grade) top.appendChild(E('span','pill '+s.grade, s.grade+'级 '+((s.score??'')+'/8')));
      if(s.resonance>=2) top.appendChild(E('span','fire','🔥共振'+s.resonance));
      top.appendChild(E('span','time', s.pushed_at||''));
      box.appendChild(top);
      const g=E('div','grid');
      const kv=(k,v,cls)=>{const d=E('div','kv');d.appendChild(E('span',null,k));d.appendChild(E('b',cls||null,v));g.appendChild(d);};
      kv('入场', num(s.entry), 'big');
      kv('止损', num(s.sl));
      kv('TP1', num(s.tp1));
      kv('TP2', num(s.tp2));
      kv('RSI', s.rsi==null?'-':s.rsi);
      kv('ATR', num(s.atr));
      box.appendChild(g);
      sw.appendChild(box);
    });
  }catch(e){
    document.getElementById('sub').textContent='加载失败: '+e;
  }
}
load(); setInterval(load, 30000);
</script>
</body>
</html>
"""


if __name__ == "__main__":
    import uvicorn
    cfg = _cfg()
    port = int(cfg.get("dashboard_port", 1689))
    uvicorn.run(app, host="0.0.0.0", port=port)
