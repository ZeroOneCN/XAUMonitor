#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""B1 验证：硬性风控闸门三道门是否真能拦住。

用临时库构造场景，不碰生产 signals.db。
"""
import os
import sqlite3
import tempfile
from datetime import datetime, timedelta

import dpb_monitor as m

TMP = tempfile.mktemp(suffix=".db")
SCHEMA = """
CREATE TABLE signals (id INTEGER PRIMARY KEY, ts TEXT, pushed_at TEXT, timeframe TEXT,
  direction INTEGER, sig_type TEXT, grade TEXT, score INTEGER, band TEXT,
  entry REAL, sl REAL, tp1 REAL, tp2 REAL, r_size REAL, rsi REAL, atr REAL,
  resonance INTEGER, close REAL);
CREATE TABLE signal_outcomes (signal_id INTEGER PRIMARY KEY, timeframe TEXT,
  direction INTEGER, outcome TEXT, r_multiple REAL, bars INTEGER, mfe REAL,
  mae REAL, resolved_at TEXT, updated_at TEXT, cost_r REAL);
"""


def build(rows, ts=None):
    """rows: (signal_id, r_multiple, resolved_at|None)"""
    ts = ts or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if os.path.exists(TMP):
        os.remove(TMP)
    c = sqlite3.connect(TMP)
    c.executescript(SCHEMA)
    for sid, r, res in rows:
        c.execute("INSERT INTO signals (id,ts,pushed_at,timeframe,direction) VALUES (?,?,?,?,?)",
                  (sid, ts, ts, "5m", 1))
        c.execute("INSERT INTO signal_outcomes (signal_id,timeframe,direction,outcome,"
                  "r_multiple,bars,mfe,mae,resolved_at,updated_at,cost_r)"
                  " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                  (sid, "5m", 1, "SL", r, 3, 0.1, 1.0, res, res, 0.02))
    c.commit()
    c.close()


CFG = {"db_file": TMP, "account_equity": 650, "risk_per_trade_pct": 1.0}

print("=" * 76)
print("① 无记录 → 应放行")
print("=" * 76)
build([])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  reasons={r['reasons']}")
ok1 = r["allowed"] is True
print(f"  {'✅ 通过' if ok1 else '❌ 失败'}")

print()
print("=" * 76)
print("② 单日亏损熔断：账户 650、风险 1%/笔 = $6.50。今日 3 笔各 -1R = -$19.50")
print("   限额 3% = $19.50 → 恰好触及，应拦")
print("=" * 76)
today = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
build([(1, -1.0, today), (2, -1.0, today), (3, -1.0, today)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}")
for x in r["reasons"]:
    print(f"    ⛔ {x}")
print(f"  今日 {r['detail']['today_usd']:+.2f} 美元  限额 -{r['detail']['daily_limit_usd']:.2f}")
ok2 = r["allowed"] is False and any("熔断" in x for x in r["reasons"])
print(f"  {'✅ 通过' if ok2 else '❌ 失败'}")

print()
print("=" * 76)
print("③ 未达限额（-1R×2 = -$13 < $19.50）→ 不该因熔断拦")
print("=" * 76)
build([(1, -1.0, today), (2, -1.0, today)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  reasons={r['reasons']}")
print(f"  今日 {r['detail']['today_usd']:+.2f} 美元")
ok3 = r["allowed"] is True
print(f"  {'✅ 通过' if ok3 else '❌ 失败'}")

print()
print("=" * 76)
print("④ 最大同时持仓：3 笔未结案 → 应拦")
print("=" * 76)
build([(1, None, None), (2, None, None), (3, None, None)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  持仓={r['detail']['open_positions']}")
for x in r["reasons"]:
    print(f"    ⛔ {x}")
ok4 = r["allowed"] is False and any("持仓已满" in x for x in r["reasons"])
print(f"  {'✅ 通过' if ok4 else '❌ 失败'}")

print()
print("=" * 76)
print("⑤ 连亏冷静期：最近 3 笔全亏，最后一笔 1 小时前（需满 4 小时）→ 应拦")
print("=" * 76)
t1 = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
build([(1, -1.0, t1), (2, -1.0, t1), (3, -1.0, t1)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  距最后一笔 {r['detail'].get('cooldown_hours_since', 0):.1f} 小时")
for x in r["reasons"]:
    print(f"    ⛔ {x}")
ok5 = r["allowed"] is False and any("冷静期" in x for x in r["reasons"])
print(f"  {'✅ 通过' if ok5 else '❌ 失败'}")

print()
print("=" * 76)
print("⑥ 冷静期满（最后一笔 6 小时前）→ 应放行")
print("=" * 76)
t2 = (datetime.now() - timedelta(hours=6)).strftime("%Y-%m-%d %H:%M:%S")
build([(1, -1.0, t2), (2, -1.0, t2), (3, -1.0, t2)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  reasons={r['reasons']}")
print(f"  距最后一笔 {r['detail'].get('cooldown_hours_since', 0):.1f} 小时")
ok6 = r["allowed"] is True
print(f"  {'✅ 通过' if ok6 else '❌ 失败'}")

print()
print("=" * 76)
print("⑦ 连亏但中间夹一个盈利单 → 不算连亏，应放行")
print("=" * 76)
build([(1, -1.0, t2), (2, 0.5, t2), (3, -1.0, t2)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  reasons={r['reasons']}")
ok7 = r["allowed"] is True
print(f"  {'✅ 通过' if ok7 else '❌ 失败（连亏判定写错了）'}")

print()
print("=" * 76)
print("⑧ 关掉闸门（enabled=False）→ 任何情况都放行")
print("=" * 76)
build([(1, -5.0, today), (2, -5.0, today), (3, -5.0, today),
       (4, None, None), (5, None, None), (6, None, None)])
cfg_off = dict(CFG, risk_gate={"enabled": False})
r = m.risk_gate(cfg_off)
print(f"  allowed={r['allowed']}  disabled={r.get('disabled')}")
ok8 = r["allowed"] is True and r.get("disabled") is True
print(f"  {'✅ 通过' if ok8 else '❌ 失败'}")

print()
print("=" * 76)
print("⑨ 库读不到时不能把信号全堵死 → 读不到应放行并报错")
print("=" * 76)
cfg_bad = dict(CFG, db_file="/nonexistent/dir/nope.db")
r = m.risk_gate(cfg_bad)
print(f"  allowed={r['allowed']}  error={str(r.get('error'))[:50]}")
ok9 = r["allowed"] is True and r.get("error")
print(f"  {'✅ 通过（宁可漏拦，不要因统计故障堵死信号）' if ok9 else '❌ 失败'}")

print()
print("=" * 76)
print("⑩ 老信号不算活持仓：3 笔未结案但都是 10 小时前的 5m 单（可执行窗口 2 小时）")
print("   → 不该因「持仓已满」拦住")
print("=" * 76)
old_ts = (datetime.now() - timedelta(hours=10)).strftime("%Y-%m-%d %H:%M:%S")
build([(1, None, None), (2, None, None), (3, None, None)], ts=old_ts)
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  活持仓={r['detail']['open_positions']} （应为 0）")
print(f"  reasons={r['reasons']}")
ok10 = r["allowed"] is True and r["detail"]["open_positions"] == 0
print(f"  {'✅ 通过' if ok10 else '❌ 失败'}")

print()
print("=" * 76)
print("⑪ 新信号算活持仓：3 笔未结案且是刚发出的 5m 单 → 应拦住")
print("=" * 76)
build([(1, None, None), (2, None, None), (3, None, None)])
r = m.risk_gate(CFG)
print(f"  allowed={r['allowed']}  活持仓={r['detail']['open_positions']} （应为 3）")
for x in r["reasons"]:
    print(f"    ⛔ {x}")
ok11 = r["allowed"] is False and r["detail"]["open_positions"] == 3
print(f"  {'✅ 通过' if ok11 else '❌ 失败'}")

if os.path.exists(TMP):
    os.remove(TMP)
res = [ok1, ok2, ok3, ok4, ok5, ok6, ok7, ok8, ok9, ok10, ok11]
print()
print("=" * 76)
print(f"总判定: {'✅ 全部 ' + str(len(res)) + ' 项通过' if all(res) else '❌ 失败项: ' + str([i+1 for i,v in enumerate(res) if not v])}")
print("=" * 76)
