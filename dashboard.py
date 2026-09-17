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
  :root { --bg:#0d1117; --card:#161b22; --border:#30363d; --fg:#e6edf3; --mut:#8b949e;
          --green:#3fb950; --red:#f85149; --gold:#d29922; --blue:#58a6ff; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--fg);
         font-family:-apple-system,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif; }
  .wrap { max-width:1100px; margin:0 auto; padding:20px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--mut); font-size:13px; margin-bottom:18px; }
  .cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr)); gap:10px; margin-bottom:18px; }
  .card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:14px; }
  .card .v { font-size:24px; font-weight:700; }
  .card .l { color:var(--mut); font-size:12px; margin-top:4px; }
  section { margin-bottom:22px; }
  h2 { font-size:15px; margin:0 0 10px; color:var(--fg); border-left:3px solid var(--blue); padding-left:8px; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { padding:7px 9px; text-align:left; border-bottom:1px solid var(--border); white-space:nowrap; }
  th { color:var(--mut); font-weight:600; font-size:12px; }
  tr:hover td { background:#1c2230; }
  .pill { display:inline-block; padding:1px 7px; border-radius:10px; font-size:11px; font-weight:600; }
  .S { background:#d29922; color:#000; } .A { background:#3fb950; color:#000; }
  .B { background:#58a6ff; color:#000; } .C { background:#8b949e; color:#000; }
  .long { color:var(--green); font-weight:700; } .short { color:var(--red); font-weight:700; }
  .tf-grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px; }
  .tf { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:12px; }
  .tf .name { font-weight:700; font-size:14px; margin-bottom:8px; }
  .tf .row { display:flex; justify-content:space-between; font-size:12px; color:var(--mut); margin:3px 0; }
  .tf .row b { color:var(--fg); }
  .fire { color:var(--gold); }
  .muted { color:var(--mut); font-size:13px; }
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
    const [st, sg, sy] = await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals?limit=60').then(r=>r.json()),
      fetch('/api/status').then(r=>r.json()),
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

    // 统计
    const parts=[];
    const dict = o => Object.entries(o).map(([k,v])=>`${k}:${v}`).join('  ');
    parts.push(`按周期 — ${dict(st.by_timeframe)}`);
    parts.push(`按方向 — ${dict(st.by_direction)}`);
    parts.push(`按类型 — ${dict(st.by_type)}`);
    const sd=document.getElementById('stats'); sd.innerHTML='';
    parts.forEach(p=>sd.appendChild(E('div',null,p)));
    sd.appendChild(E('div',null,'配置 — trade_freq='+sy.config.trade_freq+`  trend_stability=${sy.config.trend_stability}  min_grade=${sy.config.min_signal_grade}  共振阈值=${sy.config.resonance_min_count}  突破SL=${sy.config.breakout_sl_atr}×ATR`));

    // 信号表
    const sw=document.getElementById('sigWrap'); sw.innerHTML='';
    if(!sg.signals.length){ sw.appendChild(E('div','muted','暂无信号记录')); return; }
    const tb=E('table');
    const hr=E('tr'); ['时间','周期','方向','类型','等级','评分','入场','止损','TP1','TP2','RSI','共振']
      .forEach(h=>hr.appendChild(E('th',null,h))); tb.appendChild(hr);
    sg.signals.forEach(s=>{
      const tr=E('tr');
      const cells=[
        [s.pushed_at||'', ''], [s.timeframe,''],
        [s.direction===1?'多':'空', s.direction===1?'long':'short'],
        [s.sig_type||'', ''], [s.grade||'', ''], [(s.score??'')+'/8', ''],
        [s.entry?.toFixed?.(2)??s.entry,''], [s.sl?.toFixed?.(2)??s.sl,''],
        [s.tp1?.toFixed?.(2)??s.tp1,''], [s.tp2?.toFixed?.(2)??s.tp2,''],
        [s.rsi??'', ''], [s.resonance>=2?('🔥'+s.resonance):(s.resonance||''), s.resonance>=2?'fire':''],
      ];
      cells.forEach(([v,c])=>{
        const td=E('td',c);
        if(c==='' && /^[SABC]$/.test(v)){ const p=E('span','pill '+v,v); td.appendChild(p); }
        else td.textContent=v;
        tr.appendChild(td);
      });
      tb.appendChild(tr);
    });
    sw.appendChild(tb);
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
