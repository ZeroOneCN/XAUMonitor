#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""DPB 信号复盘：期望值 / R:R 结构 / 周期差异（带置信区间）"""
import sqlite3, math, collections

DB = "/root/www/xaumonitor/signals.db"
c = sqlite3.connect(DB)
c.row_factory = sqlite3.Row

rows = [dict(r) for r in c.execute("""
    SELECT s.id, s.timeframe, s.direction, s.sig_type, s.grade, s.score,
           s.entry, s.sl, s.tp1, s.tp2, s.pushed_at,
           o.outcome, o.r_multiple, o.resolved_at, o.cost_r
    FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id
    WHERE o.resolved_at IS NOT NULL
    ORDER BY s.id""")]

def ci95(xs):
    """均值的 95% 置信区间（正态近似）"""
    n = len(xs)
    if n < 2:
        return (float('nan'), float('nan'))
    m = sum(xs) / n
    var = sum((x - m) ** 2 for x in xs) / (n - 1)
    se = math.sqrt(var / n)
    return (m - 1.96 * se, m + 1.96 * se)

def blk(title):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)

# ---------- 总账 ----------
blk("【1】总账")
rs = [r["r_multiple"] for r in rows]
n = len(rs)
m = sum(rs) / n
lo, hi = ci95(rs)
print(f"  已结案 {n} 笔   期望 {m:+.3f}R   95%CI [{lo:+.3f}, {hi:+.3f}]")
print(f"  累计 {sum(rs):+.1f}R")
print(f"  判定: {'❌ 期望显著为负（CI 上界 < 0）' if hi < 0 else ('✅ 期望显著为正' if lo > 0 else '⚠️ CI 跨 0，尚不能判定（样本不足）')}")
wins = [x for x in rs if x > 0]
loss = [x for x in rs if x <= 0]
print(f"  胜 {len(wins)} 笔 均 +{sum(wins)/len(wins):.3f}R   负 {len(loss)} 笔 均 {sum(loss)/len(loss):.3f}R")
print(f"  胜率 {len(wins)/n*100:.1f}%")

# 盈亏平衡所需胜率
aw = sum(wins) / len(wins)
al = abs(sum(loss) / len(loss))
be = al / (aw + al)
print(f"\n  ⚠️ 按当前「均盈 {aw:.3f}R / 均亏 {al:.3f}R」，盈亏平衡需要胜率 {be*100:.1f}%")
print(f"     实际胜率 {len(wins)/n*100:.1f}%  →  缺口 {(be - len(wins)/n)*100:+.1f} 个百分点")

# ---------- R:R 结构 ----------
blk("【2】R:R 结构 —— 这是亏损的真正来源")
rr = []
for r in rows:
    e, s_ = r["entry"], r["sl"]
    if e is None or s_ is None:
        continue
    risk = abs(e - s_)
    if risk < 1e-9:
        continue
    d = {"tp1": None, "tp2": None}
    for k in ("tp1", "tp2"):
        if r[k] is not None:
            d[k] = abs(r[k] - e) / risk
    rr.append((r["timeframe"], r["outcome"], d["tp1"], d["tp2"]))

for k, idx in (("tp1", 2), ("tp2", 3)):
    vals = [x[idx] for x in rr if x[idx] is not None]
    if vals:
        srt = sorted(vals)
        print(f"  {k.upper()} 距离 = {sum(vals)/len(vals):.2f}R  (中位 {srt[len(srt)//2]:.2f}R, n={len(vals)})")

print("\n  按结局看实际拿到的 R：")
for oc in ("TP1", "TP2", "SL"):
    v = [r["r_multiple"] for r in rows if r["outcome"] == oc]
    if v:
        print(f"    {oc:<4} {len(v):>3} 笔  平均 {sum(v)/len(v):+.3f}R")

print("\n  🔍 结构诊断：")
tp1v = [x[2] for x in rr if x[2] is not None]
if tp1v:
    avg_tp1 = sum(tp1v) / len(tp1v)
    print(f"    TP1 只有 {avg_tp1:.2f}R —— 一半仓位在 0.5R 附近就跑了")
    print(f"    结果：赢了赚 {aw:.3f}R，输了亏 {al:.3f}R —— 赔率 < 1")
    print(f"    要在这赔率下活下来，胜率必须 > {be*100:.0f}%（实际 {len(wins)/n*100:.0f}%）")

# ---------- 按周期 ----------
blk("【3】按周期（决定「要不要做大级别」）")
by = collections.defaultdict(list)
for r in rows:
    by[r["timeframe"]].append(r["r_multiple"])
print(f"  {'周期':<6}{'笔数':>5}{'期望':>9}{'95%CI':>22}{'胜率':>8}{'累计R':>9}  判定")
for tf, v in sorted(by.items(), key=lambda kv: -len(kv[1])):
    mm = sum(v) / len(v)
    l, h = ci95(v)
    w = len([x for x in v if x > 0]) / len(v)
    if len(v) < 10:
        verd = "样本不足"
    elif h < 0:
        verd = "显著为负"
    elif l > 0:
        verd = "显著为正"
    else:
        verd = "不显著"
    print(f"  {tf:<6}{len(v):>5}{mm:>+9.3f}    [{l:+.3f}, {h:+.3f}]   {w*100:>6.1f}%{sum(v):>+9.1f}  {verd}")

# ---------- 方向 / 类型 / 等级 ----------
blk("【4】方向 / 类型 / 等级")
for key, lbl in (("direction", "方向"), ("sig_type", "类型"), ("grade", "等级")):
    print(f"\n  [{lbl}]")
    g = collections.defaultdict(list)
    for r in rows:
        g[str(r[key])].append(r["r_multiple"])
    for k, v in sorted(g.items(), key=lambda kv: -len(kv[1])):
        mm = sum(v) / len(v)
        l, h = ci95(v)
        w = len([x for x in v if x > 0]) / len(v)
        sig = "✓显著" if (len(v) >= 10 and (l > 0 or h < 0)) else "  "
        print(f"    {k:<6} {len(v):>3} 笔  {mm:>+7.3f}R  CI[{l:+.2f},{h:+.2f}]  胜率 {w*100:>5.1f}%  {sig}")

# ---------- 成本 ----------
blk("【5】点差成本（吃掉了多少）")
cr = [r["cost_r"] for r in rows if r["cost_r"] is not None]
if cr:
    print(f"  平均点差成本 {sum(cr)/len(cr):.3f}R / 笔")
    print(f"  累计 {sum(cr):.1f}R")
    print(f"  若无点差，期望会变成 {m + sum(cr)/len(cr):+.3f}R")
print("\n  注: cost_r 是「点差 0.2 美元」的口径；")
print("      你经纪商实测点差中位 0.62 美元 → 实际成本约 3 倍")
print(f"      → 真实期望约 {m - 2*sum(cr)/len(cr):+.3f}R（点差按 3 倍估）")
