#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交叉验证：等级A的负期望，是真效应还是被周期/类型混杂出来的？"""
import sqlite3, collections, math

c = sqlite3.connect("/root/www/xaumonitor/signals.db")
c.row_factory = sqlite3.Row
rows = [dict(r) for r in c.execute("""
    SELECT s.grade, s.sig_type, s.timeframe, s.direction, s.score, o.r_multiple
    FROM signals s JOIN signal_outcomes o ON o.signal_id = s.id
    WHERE o.resolved_at IS NOT NULL""")]


def avg(xs):
    return sum(xs) / len(xs) if xs else float("nan")


print("=" * 78)
print("【1】等级 × 类型（看 A 的负期望是不是因为 A 全是「突破」）")
print("=" * 78)
tab = collections.defaultdict(list)
for r in rows:
    tab[(str(r["grade"]), str(r["sig_type"]))].append(r["r_multiple"])
print(f"  {'等级':<5}{'类型':<6}{'笔数':>5}{'期望':>10}{'胜率':>8}")
for (g, t), v in sorted(tab.items()):
    w = len([x for x in v if x > 0]) / len(v)
    print(f"  {g:<5}{t:<6}{len(v):>5}{avg(v):>+10.3f}{w*100:>7.1f}%")

print("\n  边际（只看等级，跨类型汇总）:")
for g in ("A", "B", "S"):
    v = [r["r_multiple"] for r in rows if str(r["grade"]) == g]
    if v:
        print(f"    {g}: {len(v):>3} 笔  {avg(v):>+.3f}R")

print("\n  边际（只看类型，跨等级汇总）:")
for t in set(str(r["sig_type"]) for r in rows):
    v = [r["r_multiple"] for r in rows if str(r["sig_type"]) == t]
    print(f"    {t}: {len(v):>3} 笔  {avg(v):>+.3f}R")

print()
print("=" * 78)
print("【2】等级 × 周期（A 是不是集中在某个周期）")
print("=" * 78)
tab2 = collections.defaultdict(list)
for r in rows:
    tab2[(str(r["grade"]), str(r["timeframe"]))].append(r["r_multiple"])
print(f"  {'等级':<5}{'周期':<6}{'笔数':>5}{'期望':>10}")
for (g, tf), v in sorted(tab2.items()):
    print(f"  {g:<5}{tf:<6}{len(v):>5}{avg(v):>+10.3f}")

print()
print("=" * 78)
print("【3】等级 × 分数段（A 的 score 是不是反而更低？）")
print("=" * 78)
sc = collections.defaultdict(list)
for r in rows:
    s = r["score"]
    if s is None:
        continue
    sc[str(r["grade"])].append(float(s))
for g, v in sorted(sc.items()):
    v.sort()
    print(f"  {g}: n={len(v):>3}  score 中位 {v[len(v)//2]:.1f}  范围 [{v[0]:.1f}, {v[-1]:.1f}]")

print()
print("=" * 78)
print("【4】若只看「回踩」+「B」这个子集（去掉两个显著为负的维度）")
print("=" * 78)
sub = [r["r_multiple"] for r in rows
       if str(r["sig_type"]) == "回踩" and str(r["grade"]) == "B"]
if sub:
    m = avg(sub)
    n = len(sub)
    var = sum((x - m) ** 2 for x in sub) / (n - 1)
    se = math.sqrt(var / n)
    l, h = m - 1.96 * se, m + 1.96 * se
    w = len([x for x in sub if x > 0]) / n
    print(f"  回踩+B: {n} 笔  期望 {m:+.3f}R  CI[{l:+.3f}, {h:+.3f}]  胜率 {w*100:.1f}%")
    print(f"  ⚠️ 这是「挑子组」的结果，不是结论 —— 只是用来看方向。")
print()
print("  提示：任何子组的最终判定都需要 30+ 笔独立样本，")
print("        且要按「测试次数」做 Bonferroni 校正。")
