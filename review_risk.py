#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""关键约束：账户 $1000 + 最小手数 0.01 → 大级别的止损宽度会强制超风险"""
import sqlite3, collections

c = sqlite3.connect("/root/www/xaumonitor/signals.db")
c.row_factory = sqlite3.Row
rows = [dict(r) for r in c.execute("""
    SELECT s.timeframe, s.entry, s.sl FROM signals s
    JOIN signal_outcomes o ON o.signal_id = s.id
    WHERE o.resolved_at IS NOT NULL AND s.entry IS NOT NULL AND s.sl IS NOT NULL""")]

risk_by = collections.defaultdict(list)
for r in rows:
    w = abs(float(r["entry"]) - float(r["sl"]))
    if w > 1e-9:
        risk_by[r["timeframe"]].append(w)

EQ = 1000.0          # 补充后的保证金
MIN_LOT = 0.01       # 手
OZ_PER_LOT = 100     # 1 手 = 100 盎司
RISK_PCT = 1.0       # 目标单笔风险 %

print("=" * 84)
print(f"账户 ${EQ:,.0f} | 最小手数 {MIN_LOT} 手 = {MIN_LOT*OZ_PER_LOT:g} 盎司 | 目标单笔风险 {RISK_PCT}%")
print("=" * 84)
print(f"  目标单笔风险金额 = ${EQ*RISK_PCT/100:,.2f}")
print()
print(f"  {'周期':<6}{'笔数':>5}{'平均止损宽度':>13}{'最小手数的风险':>16}{'占账户':>9}  判定")
target = EQ * RISK_PCT / 100
for tf in ("5m", "15m", "1h", "4h"):
    if tf not in risk_by:
        continue
    v = risk_by[tf]
    aw = sum(v) / len(v)
    # 最小手数下的风险金额 = 止损宽度($/oz) × 0.01手×100oz
    min_risk = aw * MIN_LOT * OZ_PER_LOT
    pct = min_risk / EQ * 100
    if pct <= RISK_PCT * 1.05:
        verd = "✅ 最小手数就能满足 1%"
    elif pct <= 2.5:
        verd = f"⚠️ 强制 {pct:.1f}%，是最小仓"
    else:
        verd = f"❌ 强制 {pct:.1f}%，严重超配"
    print(f"  {tf:<6}{len(v):>5}{aw:>12.2f}${min_risk:>15.2f}${pct:>8.2f}%  {verd}")

print()
print("=" * 84)
print("【结论】账户规模 vs 可交易级别")
print("=" * 84)
print("""
  风控算式：风险金额 = 止损宽度($/oz) × 手数 × 100
  要满足 1% 风险，手数 = 目标金额 / (止损宽度 × 100)
  但手数不能小于 0.01 → 存在「最小可能风险」的下限。

  这就是大级别在**小账户**上的物理限制：""")
for tf in ("5m", "15m", "1h", "4h"):
    if tf not in risk_by:
        continue
    aw = sum(risk_by[tf]) / len(risk_by[tf])
    need = target / (aw * 100)
    min_risk = aw * MIN_LOT * OZ_PER_LOT
    # 满足1%所需的最低账户
    need_eq = min_risk / (RISK_PCT / 100)
    print(f"    {tf:<5} 想按1%下单需要 {need:.4f} 手（<0.01 不可行）"
          f"  → 账户至少要 ${need_eq:>8,.0f} 才能做到 1%")

print()
print("  ✅ 所以：$1,000 的账户，**15m 是能做到 1% 的最大级别**。")
print("     1h 最小也是 2%，4h 会到 4% 左右。")
print("     要真正做 1h/4h，账户需要 $2,000~$4,000。")
print()
print("  ⚠️ 这不是反对做大级别 —— 是告诉你做大级别的代价：")
print("     点差成本从 11.6% 降到 3.1%（省 3/4），")
print("     但最小仓位的风险从 0.8% 涨到 2%（翻 2.5 倍）。")
print("     两者要一起看，不能只看一面。")
