#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""「做大级别」到底有没有数学依据？—— 点差成本 / 止损宽度 按周期拆分"""
import sqlite3, math, collections

DB = "/root/www/xaumonitor/signals.db"
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row
rows = [dict(r) for r in c.execute("""
    SELECT s.id, s.timeframe, s.direction, s.entry, s.sl, s.tp1, s.tp2, s.pushed_at,
           o.outcome, o.r_multiple, o.cost_r, o.bars
    FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id
    WHERE o.resolved_at IS NOT NULL ORDER BY s.id""")]

SPREAD = 0.2      # 配置口径
SPREAD_REAL = 0.62  # 实测中位

print("=" * 78)
print("【核心问题】「做大级别」有没有依据？")
print("=" * 78)

print("""
  做大级别通常有两个理由，必须分开验证：
    (a) 胜率更高 / 期望更好  →  这是「统计推断」，小样本不能下结论
    (b) 点差成本占比更低      →  这是「结构性确定」，可以直接算
  下面分别看。""")

# ---------- (b) 点差成本：结构性确定 ----------
print()
print("=" * 78)
print("(b) 点差成本占 R 的比例 —— 结构性确定，可以直接算")
print("=" * 78)
by = collections.defaultdict(lambda: {"n": 0, "cost": [], "risk": [], "r": []})
for r in rows:
    e, s_ = r["entry"], r["sl"]
    if e is None or s_ is None:
        continue
    risk = abs(e - s_)
    if risk < 1e-9:
        continue
    k = r["timeframe"]
    by[k]["n"] += 1
    by[k]["risk"].append(risk)
    if r["cost_r"] is not None:
        by[k]["cost"].append(r["cost_r"])
    by[k]["r"].append(r["r_multiple"])

print(f"  {'周期':<5}{'笔数':>5}{'平均止损宽度':>13}{'点差成本(0.2)':>15}{'点差成本(实测0.62)':>19}{'期望':>9}")
for tf in ("5m", "15m", "1h", "4h"):
    if tf not in by:
        continue
    d = by[tf]
    ar = sum(d["risk"]) / len(d["risk"])
    # cost_r = 点差 / 止损宽度（R 以止损宽度为单位）
    c02 = SPREAD / ar
    c62 = SPREAD_REAL / ar
    m = sum(d["r"]) / len(d["r"])
    print(f"  {tf:<5}{d['n']:>5}{ar:>13.2f}${c02:>14.3f}R{c62:>18.3f}R{m:>+9.3f}")

print()
print("  🔍 读法：止损越宽，同样 0.62 美元的点差占 R 的比例越小。")
print("     5m 的止损只有 3~5 美元 → 点差吃掉 12~20% 的 R；")
print("     1h 的止损 10~20 美元   → 点差只吃 3~6% 的 R。")
print("     ✅ 这是「做大级别」最硬的理由 —— 不依赖任何胜率假设。")

# ---------- (a) 胜率：统计推断 ----------
print()
print("=" * 78)
print("(a) 胜率/期望按周期 —— 这是统计推断，小样本不能下结论")
print("=" * 78)


def ci95(xs):
    n = len(xs)
    if n < 2:
        return (float("nan"), float("nan"))
    m = sum(xs) / n
    v = sum((x - m) ** 2 for x in xs) / (n - 1)
    se = math.sqrt(v / n)
    return (m - 1.96 * se, m + 1.96 * se)


print(f"  {'周期':<5}{'笔数':>5}{'期望':>9}{'95%CI':>22}{'胜率':>8}  能否下结论")
for tf, cnt in sorted(((k, v["n"]) for k, v in by.items()), key=lambda kv: -kv[1]):
    v = by[tf]["r"]
    if not v:
        continue
    m = sum(v) / len(v)
    l, h = ci95(v)
    w = len([x for x in v if x > 0]) / len(v)
    concl = "样本太少，不能下结论" if len(v) < 30 else (
        "显著为负" if h < 0 else ("显著为正" if l > 0 else "不显著"))
    print(f"  {tf:<5}{len(v):>5}{m:>+9.3f}    [{l:+.3f}, {h:+.3f}]   {w*100:>5.1f}%  {concl}")

print()
print("  ⚠️ 关键：1h 只有 8 笔。用 8 笔去判断「1h 更好」= 挑子组，")
print("     几乎必然挑到噪音。要判定 1h 是否更好，至少需要 30~50 笔。")

# ---------- 修点差后的真实期望 ----------
print()
print("=" * 78)
print("【最要紧的一张表】用「实测点差 0.62」重算期望")
print("=" * 78)
print(f"  {'周期':<5}{'记账期望(0.2点差)':>20}{'真实期望(0.62点差)':>22}{'差额':>10}")
for tf in ("5m", "15m", "1h"):
    if tf not in by or not by[tf]["r"]:
        continue
    d = by[tf]
    m = sum(d["r"]) / len(d["r"])
    ar = sum(d["risk"]) / len(d["risk"])
    # 记账口径已含 0.2 点差；换成 0.62 需再扣 (0.62-0.2)/ar
    delta = (SPREAD_REAL - SPREAD) / ar
    print(f"  {tf:<5}{m:>+19.3f}R{m - delta:>+21.3f}R{-delta:>+9.3f}R")
print()
print("  → 记账口径本身就低估了成本。5m 的真实期望比账面还要低约 0.1R/笔。")
