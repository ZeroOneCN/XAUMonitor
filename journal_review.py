#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""交易流水复盘工具 —— 把 lifeos2 导出的 investment_forex JSON 做成一份诊断报告。

用法:
    ./venv/bin/python journal_review.py <backup.json> [--market]

    --market   额外拉真实 XAU 行情，测算「盈利单的最大浮亏(MAE)」并模拟硬止损效果。
               （会消耗 Twelve Data 额度，约 4~5 次请求）

诊断维度（按重要性排序）：
  1 总账            净盈亏 / 胜率 / 均盈均亏 / 盈亏因子
  2 亏损集中度      最惨的 N% 亏损单吃掉了多少 → 判断是「判断力」还是「风控」问题
  3 止损模拟        每笔亏损硬性截断在 K 美元后的结果
  4 硬止损误杀率    用真实行情算盈利单走出利润前先浮亏多少（--market）
  5 持仓时长        盈利单 vs 亏损单的持仓时间 → 是否「截断盈利、放任亏损」
  6 加仓行为        亏损之后是否加大手数（马丁/报复性交易）
  7 手数分档        每个手数档的净盈亏 → 大亏是否只来自大仓
  8 时段 / 逐日权益 / 最大回撤
"""
import json
import statistics as st
import sys
from collections import defaultdict

# 各品种每手盎司数（用于校验记录里的盈亏是否说得通）
CONTRACT_OZ = {"XAUUSD": 100.0, "XAGUSD": 5000.0}
DEFAULT_OZ = 100.0


def load(path):
    D = json.load(open(path, encoding="utf-8"))
    rows = D["data"]["investment_forex"]
    for r in rows:
        for k in ("pnl", "commission", "overnight_fee", "lot_size", "holding",
                  "open_price", "close_price"):
            try:
                r[k] = float(r[k] or 0)
            except Exception:
                r[k] = 0.0
        r["net"] = r["pnl"] + r["commission"] + r["overnight_fee"]
        r["win"] = r["net"] > 0
        r["oz"] = CONTRACT_OZ.get(r["symbol"].split(".")[0].rstrip("+ms"),
                                  DEFAULT_OZ) * r["lot_size"]
    rows.sort(key=lambda r: (r["open_time"] or ""))
    return rows


def hr(t):
    print()
    print("=" * 74)
    print(t)
    print("=" * 74)


def report(rows, with_market=False):
    N = len(rows)
    net = sum(r["net"] for r in rows)
    wins = [r for r in rows if r["win"]]
    loss = [r for r in rows if not r["win"]]
    aw = st.mean([r["net"] for r in wins]) if wins else 0
    al = abs(st.mean([r["net"] for r in loss])) if loss else 0

    hr("① 总账")
    print(f"  笔数 {N}   净盈亏 {net:+.2f}   胜率 {len(wins)/N*100:.1f}%")
    print(f"  均盈 {aw:+.2f}   均亏 {-al:+.2f}   盈亏比 {aw/al if al else 0:.3f}"
          f"   盈亏因子 {sum(r['net'] for r in wins)/abs(sum(r['net'] for r in loss)) if loss else 0:.3f}")
    # 打平条件: p×均盈 = (1-p)×均亏  →  均亏 = p/(1-p) × 均盈
    # （第一版把分子分母写反，算出 $0.86 这种荒谬值，已修正）
    wr = len(wins) / N
    be = wr / (1 - wr) * aw
    print(f"  → 以当前胜率 {wr*100:.1f}%，均亏压到 ${be:.2f} 即打平"
          f"（现在 ${al:.2f}，只需改善 {(al-be)/al*100:.0f}%）")

    hr("② 亏损集中度")
    ls = sorted(r["net"] for r in loss)
    for pct in (1, 2, 5, 10):
        k = max(1, int(len(ls) * pct / 100))
        print(f"  最惨 {pct:>2d}%（{k:>3d}笔）合计 {sum(ls[:k]):>+10.2f}"
              f"   = 总亏损的 {sum(ls[:k])/abs(net)*100:>5.1f}%")
    print(f"  ⇒ {'⚠️ 少数几笔主导——是风控问题，不是判断力问题' if abs(sum(ls[:max(1,int(len(ls)*0.02))]))/abs(net)>0.5 else '分布较均匀'}")

    hr("③ 止损模拟（亏损单按 K 美元截断，盈利单不变）")
    print("  原理：亏了 $200 的单子必然途经 -$K，故截断在物理上可达（近似）")
    print(f"  {'硬止损':<9}{'净盈亏':>12}{'相对现在':>12}")
    for K in (3, 5, 8, 10, 15, 20, 30, 50):
        tot = sum(r["net"] for r in wins) + sum(-min(abs(r["net"]), K) for r in loss)
        print(f"  ${K:<8}{tot:>+12.2f}{tot-net:>+12.2f}")

    hr("⑤ 持仓时长")
    print(f"  {'区间':<12}{'笔数':>7}{'净盈亏':>12}{'胜率':>8}{'均每笔':>10}")
    for a, b, nm in [(0, 5, '<5分钟'), (5, 15, '5-15分钟'), (15, 60, '15-60分钟'),
                     (60, 240, '1-4小时'), (240, 1440, '4-24小时'),
                     (1440, 10**9, '>1天')]:
        g = [r for r in rows if a <= r["holding"] < b]
        if not g:
            continue
        nn = sum(x["net"] for x in g)
        w = len([x for x in g if x["win"]])
        print(f"  {nm:<12}{len(g):>7}{nn:>+12.2f}{w/len(g)*100:>7.1f}%{nn/len(g):>+10.3f}")
    hw = st.mean([r["holding"] for r in wins]) if wins else 0
    hl = st.mean([r["holding"] for r in loss]) if loss else 0
    print(f"\n  盈利单均值 {hw:.0f} 分  亏损单均值 {hl:.0f} 分  → 亏损单久 {hl/hw:.2f}x" if hw else "")

    hr("⑥ 加仓行为（亏损之后是否加大手数）")
    for label, flag in (("盈利单之后", True), ("亏损单之后", False)):
        g = [rows[i] for i in range(1, N) if rows[i-1]["win"] is flag]
        big = [x for x in g if x["lot_size"] > 0.011]
        if not g:
            continue
        print(f"  {label:<10}{len(g):>6}笔  其中加仓 {len(big):>4}笔({len(big)/len(g)*100:>4.1f}%)"
              f"  那批净 {sum(x['net'] for x in big):>+9.2f}  平均"
              f"{st.mean([x['net'] for x in big]) if big else 0:>+7.2f}")

    hr("⑦ 按手数")
    by = defaultdict(list)
    for r in rows:
        by[r["lot_size"]].append(r)
    for s in sorted(by):
        g = by[s]
        print(f"  {s:>5.2f}手 {len(g):>5}笔  净{sum(x['net'] for x in g):>+10.2f}"
              f"  平均{sum(x['net'] for x in g)/len(g):>+8.3f}")

    hr("⑧ 逐日权益与最大回撤")
    byday = defaultdict(float)
    for r in rows:
        byday[r["trade_date"]] += r["net"]
    cum = peak = mdd = 0.0
    worst = None
    for d in sorted(byday):
        cum += byday[d]
        peak = max(peak, cum)
        mdd = max(mdd, peak - cum)
        if worst is None or byday[d] < worst[1]:
            worst = (d, byday[d])
    ds = sorted(byday)
    print(f"  交易天数 {len(ds)}（{ds[0]} ~ {ds[-1]}）  日均 {net/len(ds):+.2f}")
    print(f"  最差单日 {worst[0]} {worst[1]:+.2f}   最大回撤 {mdd:.2f}")
    print(f"  亏损天数 {sum(1 for d in ds if byday[d] < 0)}/{len(ds)}")

    hr("⑨ 过路费核算（点差 / 佣金）")
    cost_analysis(rows)

    if with_market:
        try:
            market_mae(rows)
        except Exception as e:
            print(f"\n  （行情分析失败: {e}）")


def cost_analysis(rows):
    """过路费核算 —— 实测经纪商点差（同一分钟内一多一空的开仓价差），
    据此估算总成本。这一步常常是「为什么胜率高却亏钱」的真答案。"""
    import datetime as dt
    from collections import defaultdict
    for r in rows:
        try:
            r["_t"] = dt.datetime.fromisoformat(r["open_time"])
        except Exception:
            r["_t"] = None
    bysym = defaultdict(list)
    for r in rows:
        if r["_t"] and r.get("symbol", "").startswith("XAUUSD"):
            bysym[r["symbol"]].append(r)
    spreads = []
    for sym, g in bysym.items():
        g.sort(key=lambda x: x["_t"])
        sp = []
        for i in range(len(g) - 1):
            a, b = g[i], g[i + 1]
            if ((b["_t"] - a["_t"]).total_seconds() <= 60
                    and a["order_type"] != b["order_type"]
                    and a["lot_size"] == b["lot_size"]):
                d = b["open_price"] - a["open_price"]
                if a["order_type"] == "sell":
                    d = -d          # 卖价 − 买价（负值），取绝对值即点差
                sp.append(abs(d))
        if len(sp) >= 15:
            print(f"    {sym:<12}配对 {len(sp):>4} 组   点差中位 {st.median(sp):.2f}")
        spreads += sp
    if not spreads:
        print("    （配对样本不足，无法实测点差）")
        return
    sp = st.median(spreads)
    n = len(rows)
    comm = sum(r["commission"] for r in rows)
    tot_sp = sum(r["lot_size"] * 100 * sp for r in rows)
    net = sum(r["net"] for r in rows)
    print(f"\n    实测点差中位 {sp:.2f} 美元/盎司（{len(spreads)} 组配对）")
    print(f"    点差成本 ≈ -{tot_sp:.2f}   佣金 {comm:+.2f}   合计过路费 "
          f"≈ -{tot_sp + abs(comm):.2f}")
    print(f"    平均每笔 {(tot_sp + abs(comm)) / n:.3f}   （净盈亏 {net:+.2f}）")
    print(f"    ⇒ 过路费占净亏损 {abs(tot_sp + abs(comm)) / abs(net) * 100:.0f}%"
          if net else "")
    print("    注意：点差已包含在你的开/平仓价里（开仓取买价、平仓取卖价），"
          "所以\n    毛盈亏本身已扣过一次点差；此处是把它单独拎出来看清成本量级。")


def market_mae(rows):
    """用真实行情测算盈利单的浮亏，判断硬止损会不会误杀盈利单。"""
    import pandas as pd
    import backtest as bt
    import dpb_monitor as m
    cfg = m.load_config()
    hr("④ 硬止损误杀率（真实行情实测）")
    df = bt.fetch_history("5m", cfg)
    print(f"  行情 {len(df)} 根 {df.index[0]} ~ {df.index[-1]}")
    idx = df.index
    res = []
    for r in rows:
        if not r["symbol"].startswith("XAUUSD"):
            continue
        try:
            t0, t1 = pd.Timestamp(r["open_time"]), pd.Timestamp(r["close_time"])
        except Exception:
            continue
        seg = df[(idx >= t0) & (idx <= t1)]
        if seg.empty:
            continue
        if r["order_type"] == "buy":
            r["mae_usd"] = max(0.0, (r["open_price"] - float(seg["Low"].min())) * r["oz"])
        else:
            r["mae_usd"] = max(0.0, (float(seg["High"].max()) - r["open_price"]) * r["oz"])
        res.append(r)
    wins = [r for r in res if r["win"]]
    loss = [r for r in res if not r["win"]]
    maes = sorted(r["mae_usd"] for r in wins)
    n = len(maes)
    print(f"  匹配到行情 {len(res)} 笔（黄金），其中盈利单 {n} 笔")
    print("  盈利单在走出利润前的最大浮亏：")
    for p in (50, 75, 90, 95):
        print(f"    {p:>2d} 分位 ${maes[int(n*p/100)]:.2f}")
    base = sum(r["net"] for r in res)
    print(f"\n  综合模拟（亏损截断 + 剔除被误杀的盈利单），现状 {base:+.2f}：")
    print(f"  {'硬止损':<9}{'净盈亏':>12}{'相对现在':>12}{'误杀盈利单':>11}")
    for K in (3, 5, 8, 10, 15, 20, 30, 50):
        kept = [r for r in wins if r["mae_usd"] <= K]
        killed = [r for r in wins if r["mae_usd"] > K]
        tot = sum(r["net"] for r in kept) + sum(-min(abs(r["net"]), K) for r in loss)
        print(f"  ${K:<8}{tot:>+12.2f}{tot-base:>+12.2f}{len(killed):>11}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    report(load(sys.argv[1]), "--market" in sys.argv)
