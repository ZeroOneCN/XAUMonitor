#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""A1 验证：窗口走满但未触及 SL/TP1 的信号，是否正确结案为 EXP。

对照点：旧逻辑返回 done=False（永久追踪 + 统计偏差），新逻辑返回
done=True 且 outcome=EXP，R 按窗口末根收盘价计。
"""
import pandas as pd
import numpy as np

import dpb_monitor as m

TS = pd.Timestamp("2026-09-01 00:00:00")
N = 320
idx = pd.date_range(TS, periods=N, freq="5min")


def mkdf(highs, lows, closes, n=None):
    k = n if n is not None else len(highs)
    return pd.DataFrame({"High": list(highs)[:k], "Low": list(lows)[:k],
                         "Close": list(closes)[:k]}, index=idx[:k])


print("=" * 74)
print("用例1：横盘死单 —— 价格始终在 entry±1 内，SL 在 -5、TP1 在 +5")
print("=" * 74)
h = np.full(N, 4301.0)
l = np.full(N, 4299.0)
c = np.full(N, 4300.2)
df = mkdf(h, l, c)
res = m._evaluate_one(df, 1, 4300.0, 4295.0, 4305.0, 4310.0, 5.0,
                      TS.strftime("%Y-%m-%d %H:%M:%S"), 288, {})
print(f"  结果: {res}")
ok1 = res and res["outcome"] == "EXP" and res["done"] is True
exp_r = (4300.2 - 4300.0) / 5.0
print(f"  期望: outcome=EXP done=True r={exp_r:+.4f}")
print(f"  {'✅ 通过' if ok1 and abs(res['r'] - exp_r) < 1e-9 else '❌ 失败'}")
print(f"  （旧逻辑这里会给 outcome=open done=False → 该信号永久卡在追踪中）")

print()
print("=" * 74)
print("用例2：真止损 —— 第 3 根跌破 SL，应判 SL 且 done=True")
print("=" * 74)
h2, l2, c2 = h.copy(), l.copy(), c.copy()
l2[3] = 4294.0
c2[3] = 4294.5
res2 = m._evaluate_one(mkdf(h2, l2, c2), 1, 4300.0, 4295.0, 4305.0, 4310.0, 5.0,
                       TS.strftime("%Y-%m-%d %H:%M:%S"), 288, {})
print(f"  结果: {res2}")
ok2 = res2 and res2["outcome"] == "SL" and res2["done"] and abs(res2["r"] + 1.0) < 1e-9
print(f"  {'✅ 通过' if ok2 else '❌ 失败'}")

print()
print("=" * 74)
print("用例3：窗口没走满（只给 50 根）→ 必须仍是 done=False（继续追踪）")
print("=" * 74)
df3 = mkdf(h, l, c, n=50)
res3 = m._evaluate_one(df3, 1, 4300.0, 4295.0, 4305.0, 4310.0, 5.0,
                       TS.strftime("%Y-%m-%d %H:%M:%S"), 288, {})
print(f"  结果: {res3}")
ok3 = res3 and res3["outcome"] == "open" and res3["done"] is False
print(f"  {'✅ 通过（未走满不结案，保留升级到 TP2 的机会）' if ok3 else '❌ 失败'}")

print()
print("=" * 74)
print("用例4：做空方向的超时单")
print("=" * 74)
res4 = m._evaluate_one(df, -1, 4300.0, 4305.0, 4295.0, 4290.0, 5.0,
                       TS.strftime("%Y-%m-%d %H:%M:%S"), 288, {})
exp4 = (4300.0 - 4300.2) / 5.0
print(f"  结果: {res4}")
ok4 = res4 and res4["outcome"] == "EXP" and abs(res4["r"] - exp4) < 1e-9
print(f"  期望 r={exp4:+.4f}  {'✅ 通过' if ok4 else '❌ 失败'}")

print()
print("=" * 74)
print("用例5：窗口走满 + 曾触及 TP1（中途）→ 应保持 TP1 且 done=True")
print("=" * 74)
h5, l5, c5 = h.copy(), l.copy(), c.copy()
h5[10] = 4306.0          # 第10根碰到 TP1
c5[10] = 4305.5
res5 = m._evaluate_one(mkdf(h5, l5, c5), 1, 4300.0, 4295.0, 4305.0, 4310.0, 5.0,
                       TS.strftime("%Y-%m-%d %H:%M:%S"), 288, {})
print(f"  结果: {res5}")
ok5 = res5 and res5["outcome"] == "TP1" and res5["done"] is True
print(f"  {'✅ 通过' if ok5 else '❌ 失败'}")

print()
allok = all([ok1, ok2, ok3, ok4, ok5])
print("=" * 74)
print(f"总判定: {'✅ 全部通过' if allok else '❌ 有失败项'}")
print("=" * 74)
