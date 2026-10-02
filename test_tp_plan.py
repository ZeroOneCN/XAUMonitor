#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""验证「1:2」真的落成了 1:2 —— 不是 1:4，也不是 0.84:1。

不测就发上线是很危险的：tp1_exit_pct=0 时仓位会一路持到 TP2，
所以实际赔率由 r2 决定。如果只改 r1 而忘了 r2，配出来的「1:2」
会静默变成 1:4 —— 表面上一切正常，统计出来的数字却是错的。
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import numpy as np
import pandas as pd
import dpb_monitor as m

CFG = {
    "account_equity": 2000, "risk_per_trade_pct": 1.0, "contract_oz": 100,
    "min_lot": 0.01, "max_lot": 10.0, "leverage": 1000,
    "sl_cushion": 0.3, "min_sl_atr": 0.3, "max_sl_atr": 4.0,
    "fallback_sl_atr": 1.5, "breakout_sl_atr": 1.5,
    "tp1_exit_pct": 0.0, "be_after_tp1": False,
    "tp_plan": {"15m": {"r1": 2.0, "r2": 2.0, "partial": 0.0},
                "1h":  {"r1": 2.0, "r2": 2.0, "partial": 0.0},
                "4h":  {"r1": 2.0, "r2": 2.0, "partial": 0.0}},
}

ok = []

def show(title, passed, detail=""):
    ok.append(passed)
    print(f"  {'✅' if passed else '❌'} {title}" + (f"   {detail}" if detail else ""))


def mkrow(close=4300.0, atr=5.0, ema70=None):
    """构造一行行情，用于 calc_sl_tp"""
    r = pd.Series({
        "Close": close, "atr": atr, "ema70": ema70 if ema70 is not None else close - atr * 2,
        "rsi": 50.0, "signal_type": "回踩",
    })
    return r


print("=" * 76)
print("① calc_sl_tp：tp_plan 是否被读取，tp1/tp2 是否都在 2R")
print("=" * 76)
for tf in ("15m", "1h", "4h"):
    row = mkrow()
    entry, sl, r_size, r1, r2, tp1, tp2 = m.calc_sl_tp(1, row, CFG, tf)
    exp_tp = entry + r_size * 2.0
    print(f"  {tf:<4} entry={entry:.2f} sl={sl:.2f} r_size={r_size:.2f}  "
          f"r1={r1} r2={r2}  tp1={tp1:.2f} tp2={tp2:.2f}")
    show(f"{tf}: r1=r2=2.0", abs(r1 - 2.0) < 1e-9 and abs(r2 - 2.0) < 1e-9)
    show(f"{tf}: tp1 == tp2 == entry+2R", abs(tp1 - exp_tp) < 1e-9 and abs(tp2 - exp_tp) < 1e-9)

print()
print("=" * 76)
print("② _evaluate_one：实际结算出来的 R 是不是 1:2")
print("=" * 76)
ENTRY, SL = 4300.0, 4290.0
RSZ = abs(ENTRY - SL)          # = 10.0 → 1R = $10
TP1 = ENTRY + RSZ * 2          # 4320
TP2 = ENTRY + RSZ * 2          # 4320（r1=r2）

def bars(seq_hl):
    """seq_hl: [(high, low), ...] 造 K 线。

    注意：第 0 根是「信号K线本身」——_evaluate_one 从 idx+1 开始扫描，
    所以场景 K 线必须排在第 0 根之后。少了这根占位，切片为空，
    会返回 open/r=0，看起来像"策略没判"，其实是测试自己造错了数据。
    """
    seq = [(4300.0, 4300.0)] + list(seq_hl)
    idx = pd.date_range("2026-10-02", periods=len(seq), freq="15min")
    return pd.DataFrame({"High": [h for h, _ in seq],
                         "Low":  [l for _, l in seq],
                         "Close": [h for h, _ in seq]}, index=idx)


# 场景 A：直接打到 2R 目标
df = bars([(4305, 4298), (4325, 4305)])
r = m._evaluate_one(df, 1, ENTRY, SL, TP1, TP2, RSZ, df.index[0], 100, CFG)
print(f"  A 一路涨到 2R 目标     → outcome={r['outcome']:<4} R={r['r']:+.3f}")
show("A: 命中目标得 +2.000R", abs(r["r"] - 2.0) < 1e-6, f"实际 {r['r']:+.3f}")

# 场景 B：直接打止损
df = bars([(4302, 4285)])
r = m._evaluate_one(df, 1, ENTRY, SL, TP1, TP2, RSZ, df.index[0], 100, CFG)
print(f"  B 直接打到止损        → outcome={r['outcome']:<4} R={r['r']:+.3f}")
show("B: 止损得 -1.000R", abs(r["r"] + 1.0) < 1e-6, f"实际 {r['r']:+.3f}")

# 场景 C：先到 1R（未到 2R）然后回落打止损 —— 关键场景
#   旧结构（50%在1R了结+移保本）这里会得到 +0.5R；
#   新结构（全仓持到2R、不移保本）应该得到 -1.0R，即「冲高失败就是亏1R」。
df = bars([(4312, 4300), (4300, 4285)])
r = m._evaluate_one(df, 1, ENTRY, SL, TP1, TP2, RSZ, df.index[0], 100, CFG)
print(f"  C 冲到1R后回落止损     → outcome={r['outcome']:<4} R={r['r']:+.3f}")
show("C: 冲高失败 = -1.000R（全仓承担）", abs(r["r"] + 1.0) < 1e-6, f"实际 {r['r']:+.3f}")

# 场景 D：做空对称性
df = bars([(4295, 4280)])
r = m._evaluate_one(df, -1, ENTRY, 4310.0, ENTRY - 20, ENTRY - 20, 10.0, df.index[0], 100, CFG)
print(f"  D 做空命中 2R 目标     → outcome={r['outcome']:<4} R={r['r']:+.3f}")
show("D: 做空对称得 +2.000R", abs(r["r"] - 2.0) < 1e-6, f"实际 {r['r']:+.3f}")

print()
print("=" * 76)
print("③ 赔率与盈亏平衡胜率")
print("=" * 76)
win, loss = 2.0, 1.0
be = loss / (win + loss)
print(f"  赢 +{win:g}R / 亏 -{loss:g}R  →  盈亏平衡胜率 = {be*100:.1f}%")
print(f"  旧结构：均盈 0.839R / 均亏 1.041R → 需要 55.4% 胜率（实际 46.4%，缺口 9 点）")
print(f"  新结构：需要 {be*100:.1f}% 胜率")
print()
print(f"  ⚠️ 但要注意：目标从 1R 挪到 2R 后，「达标率」也会下降。")
print(f"     实测样本里到过 1R 的占 46.4%，到过 2R 的只占 18.2%。")
print(f"     18.2% < 33.3% → 仅按旧样本外推，新结构期望 = "
      f"{0.182*2 - 0.818*1:+.3f}R（旧结构 {0.169*-1:+.3f}R）")
print(f"     结论：这是**待验证的假设**，不是已验证的改进。必须跑够样本再判。")

print()
print("=" * 76)
print(f"总判定: {'✅ 全部 ' + str(len(ok)) + ' 项通过' if all(ok) else '❌ 失败项: ' + str([i+1 for i,v in enumerate(ok) if not v])}")
print("=" * 76)
