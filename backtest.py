#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XAUMonitor 回测引擎 —— 用历史数据验证信号质量，不必再等前向结果。

为什么需要
----------
没有回测时，验证「A级是不是真的比B级强」要靠每天几笔的前向结果，
要等一两周才有统计意义，参数调整只能拍脑袋。有了它，2 年历史几十秒跑完。

与实盘同源（这是可信度的关键）
------------------------------
不重写任何策略逻辑，全部复用实盘函数：
    calc_signals()     —— 信号与评分（已验证是因果的，可一次性算完逐根读取）
    calc_sl_tp()       —— 入场/止损/止盈
    _evaluate_one()    —— 结果判定（含分批止盈与保守的「同根先止损」原则）
    _GRADE_ORDER       —— 等级门槛
因此回测结论与实盘统计口径一致，不会「回测很美、实盘两回事」。

用法
----
    ./venv/bin/python backtest.py                    # 全部周期，默认历史深度
    ./venv/bin/python backtest.py --tf 5m 15m        # 只看指定周期
    ./venv/bin/python backtest.py --min-grade A      # 只统计 A 级以上
    ./venv/bin/python backtest.py --sweep-grade      # 扫描各等级门槛做对比
    ./venv/bin/python backtest.py --refresh          # 强制重拉历史
"""
import argparse
import json
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))
import dpb_monitor as m          # noqa: E402

BASE = Path(__file__).parent
CACHE_DIR = BASE / "backtest_cache"

# 各周期的默认历史深度与请求间隔。Twelve Data 单请求上限 5000 根，
# 用 end_date 向前翻页累积；越高周期单位请求覆盖的时间越长。
TF_SPEC = {
    "5m":  {"interval": "5min",  "days": 60},
    "15m": {"interval": "15min", "days": 180},
    "1h":  {"interval": "1h",    "days": 730},
    "4h":  {"interval": "4h",    "days": 1825},
    "1d":  {"interval": "1d",    "days": 3650},
}
MAX_PAGES = 12                    # 每个周期最多翻多少页，防跑飞
WARMUP_BARS = 220                 # 跳过前期指标未成形的K线（ema70 等）


# ============================================================
# 历史数据：翻页拉取 + 落盘缓存
# ============================================================
def _page(cfg: dict, interval: str, end: datetime) -> pd.DataFrame:
    """拉一页（5000 根）历史，返回升序 DataFrame"""
    keys = m._available_keys(cfg)
    if not keys:
        raise ValueError("没有可用的 Twelve Data key")
    for _ in range(len(keys) * 2):
        m._throttle()
        key = m._rotate_key(keys)
        try:
            r = m._session.get(
                "https://api.twelvedata.com/time_series",
                params={"symbol": cfg.get("ticker_td", "XAU/USD"), "interval": interval,
                        "outputsize": 5000, "apikey": key,
                        "timezone": cfg.get("timezone", m.TZ_NAME),
                        "end_date": end.strftime("%Y-%m-%d %H:%M:%S")},
                timeout=(m._CONNECT_TIMEOUT, m._READ_TIMEOUT))
            text = (r.text or "").strip()
            if not r.ok or not text:
                continue
            d = r.json()
        except Exception:
            continue
        if d.get("status") == "error" or "values" not in d:
            if m._is_daily_limit(str(d.get("message", ""))):
                continue
            return pd.DataFrame()
        df = pd.DataFrame(d["values"]).rename(columns={
            "datetime": "Date", "open": "Open", "high": "High",
            "low": "Low", "close": "Close", "volume": "Volume"})
        df["Date"] = pd.to_datetime(df["Date"])
        df = df.set_index("Date").sort_index()
        for c in ("Open", "High", "Low", "Close"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        return df[[c for c in ("Open", "High", "Low", "Close", "Volume") if c in df.columns]]
    return pd.DataFrame()


def fetch_history(tf: str, cfg: dict, refresh: bool = False) -> pd.DataFrame:
    """拉取某周期的长历史（带 6 小时磁盘缓存，重复跑回测不耗额度）"""
    spec = TF_SPEC[tf]
    CACHE_DIR.mkdir(exist_ok=True)
    cf = CACHE_DIR / f"{tf}.csv"
    if cf.exists() and not refresh and (time.time() - cf.stat().st_mtime) < 6 * 3600:
        df = pd.read_csv(cf, index_col=0, parse_dates=True)
        print(f"  [{tf}] 用缓存 {len(df)} 根（{df.index[0]:%Y-%m-%d} → {df.index[-1]:%Y-%m-%d}）")
        return df

    start = datetime.now() - timedelta(days=spec["days"])
    frames, end = [], datetime.now()
    for page in range(MAX_PAGES):
        d = _page(cfg, spec["interval"], end)
        if d.empty:
            break
        frames.append(d)
        oldest = d.index[0]
        print(f"  [{tf}] 第{page+1}页 {len(d)} 根 → 最早 {oldest:%Y-%m-%d %H:%M}")
        if oldest <= start:
            break
        end = oldest - timedelta(minutes=1)

    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames)
    df = df[~df.index.duplicated(keep="first")].sort_index()
    df = df[df.index >= start]
    df.to_csv(cf)
    print(f"  [{tf}] 合计 {len(df)} 根（{df.index[0]:%Y-%m-%d} → {df.index[-1]:%Y-%m-%d}）")
    return df


# ============================================================
# 回测核心
# ============================================================
def backtest_tf(df: pd.DataFrame, tf: str, cfg: dict,
                min_grade: str | None = None, adx_gate: float | None = None) -> dict:
    """逐根回放一个周期，返回信号列表与统计

    与实盘一致的过滤：等级门槛(min_signal_grade)、ADX 硬门槛(min_adx_filter)。
    实盘的「同周期同方向同一根K线只推一次」在逐根遍历里天然成立。
    """
    if df is None or len(df) < WARMUP_BARS + 10:
        return {"tf": tf, "signals": [], "skipped_open": 0}

    d = m.calc_signals(df.copy(), cfg)          # 已验证因果，可一次性算完
    min_grade = min_grade or cfg.get("min_signal_grade", "C")
    gate_min = m._GRADE_ORDER.get(min_grade, 0)
    if adx_gate is None:
        adx_gate = float(cfg.get("min_adx_filter", 0) or 0)
    spread = float(cfg.get("spread_usd", 0.2) or 0)
    max_bars = m._OUTCOME_MAX_BARS.get(tf, 300)

    sigs, skipped_open = [], 0
    for i in range(WARMUP_BARS, len(d)):
        row = d.iloc[i]
        sig = int(row.get("signal", 0) or 0)
        if sig == 0:
            continue
        grade = str(row.get("signal_grade", "") or "")
        if m._GRADE_ORDER.get(grade, 0) < gate_min:
            continue
        if adx_gate > 0 and float(row.get("adx", 0) or 0) < adx_gate:
            continue

        try:
            entry, sl, r_size, r1, r2, tp1, tp2 = m.calc_sl_tp(sig, row, cfg, tf)
            if not r_size or r_size <= 0:
                continue
            res = m._evaluate_one(d, sig, entry, sl, tp1, tp2, r_size,
                                  row.name, max_bars, cfg)
        except Exception:
            continue
        if not res:
            continue
        if not res.get("done"):        # 窗口未走满 → 尚未结案，不计入胜率
            skipped_open += 1
            continue

        cost_r = spread / r_size if r_size else 0.0
        sigs.append({
            "tf": tf, "bar": str(row.name), "direction": int(sig),
            "type": str(row.get("signal_type", "") or ""),
            "grade": grade, "score": int(row.get("signal_score", 0) or 0),
            "band": str(row.get("signal_band", "") or ""),
            "adx": round(float(row.get("adx", 0) or 0), 1),
            "entry": float(entry), "sl": float(sl), "tp1": float(tp1), "tp2": float(tp2),
            "r_size": float(r_size),
            "outcome": res["outcome"], "gross_r": float(res["r"]), "cost_r": cost_r,
            "net_r": float(res["r"]) - cost_r,
            "bars": int(res["bars"]), "mfe": float(res["mfe"]), "mae": float(res["mae"]),
        })
    return {"tf": tf, "signals": sigs, "skipped_open": skipped_open, "bars": len(d)}


def _agg(rows: list, key: str) -> dict:
    out = {}
    for r in rows:
        k = str(r.get(key) or "?")
        a = out.setdefault(k, {"n": 0, "wins": 0, "net_r": 0.0, "gross_r": 0.0, "cost_r": 0.0})
        a["n"] += 1
        a["wins"] += 1 if r["net_r"] > 0 else 0
        a["net_r"] += r["net_r"]
        a["gross_r"] += r["gross_r"]
        a["cost_r"] += r["cost_r"]
    for a in out.values():
        a["win_rate"] = a["wins"] / a["n"] * 100 if a["n"] else 0.0
        a["expectancy"] = a["net_r"] / a["n"] if a["n"] else 0.0
    return out


def summarize(rows: list) -> dict:
    if not rows:
        return {"n": 0}
    rs = [r["net_r"] for r in rows]
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x <= 0]
    gw, gl = sum(wins), abs(sum(losses))
    return {
        "n": len(rows), "wins": len(wins), "losses": len(losses),
        "win_rate": len(wins) / len(rows) * 100,
        "net_r": sum(rs), "gross_r": sum(r["gross_r"] for r in rows),
        "cost_r": sum(r["cost_r"] for r in rows),
        "expectancy": sum(rs) / len(rows),
        "avg_win": (gw / len(wins)) if wins else 0.0,
        "avg_loss": (-gl / len(losses)) if losses else 0.0,
        "profit_factor": (gw / gl) if gl else float("inf"),
        "by_grade": _agg(rows, "grade"), "by_type": _agg(rows, "type"),
        "by_tf": _agg(rows, "tf"),
        "by_dir": _agg(rows, "direction"),
        "by_outcome": _agg(rows, "outcome"),
    }


# ============================================================
# 报告
# ============================================================
def _line(label: str, a: dict) -> str:
    return (f"  {label:<12s} {a['n']:>5d}笔  胜率{a['win_rate']:>5.1f}%  "
            f"净{a['net_r']:>+8.2f}R  期望{a['expectancy']:>+6.3f}R")


def report(s: dict, tf_infos: dict, title: str = ""):
    print()
    print("=" * 66)
    print(f"回测结果 {title}")
    print("=" * 66)
    if not s.get("n"):
        print("  没有产生任何已结案信号。")
        return
    print(f"  样本: {s['n']} 笔（已结案）| 胜率 {s['win_rate']:.1f}% "
          f"({s['wins']}胜 {s['losses']}负)")
    print(f"  毛值 {s['gross_r']:+.2f}R  −  点差 {s['cost_r']:.2f}R  =  净值 {s['net_r']:+.2f}R")
    print(f"  期望值 {s['expectancy']:+.3f}R/笔 | 平均盈 {s['avg_win']:+.2f}R | "
          f"平均亏 {s['avg_loss']:+.2f}R | 盈亏因子 {s['profit_factor']:.2f}")
    print()
    print("  ── 按等级 ──")
    for k in sorted(s["by_grade"], reverse=True):
        print(_line(f"{k}级", s["by_grade"][k]))
    print("  ── 按类型 ──")
    for k in sorted(s["by_type"]):
        print(_line(k, s["by_type"][k]))
    print("  ── 按周期 ──")
    for k in s["by_tf"]:
        print(_line(k, s["by_tf"][k]))
    print("  ── 按方向 ──")
    for k in sorted(s["by_dir"], key=lambda x: str(x)):
        print(_line("做多" if str(k) == "1" else "做空", s["by_dir"][k]))
    print("  ── 按结局 ──")
    for k in sorted(s["by_outcome"]):
        print(_line(k, s["by_outcome"][k]))
    if tf_infos:
        print()
        print("  ── 数据范围 ──")
        for tf, info in tf_infos.items():
            print(f"  {tf:<5s} {info}")


def main():
    ap = argparse.ArgumentParser(description="XAUMonitor 策略回测")
    ap.add_argument("--tf", nargs="*", default=None, help="指定周期，默认全部")
    ap.add_argument("--min-grade", default=None, help="最低等级 C/B/A/S")
    ap.add_argument("--adx-gate", type=float, default=None, help="ADX 硬门槛")
    ap.add_argument("--refresh", action="store_true", help="强制重拉历史")
    ap.add_argument("--csv", default=None, help="把每笔信号导出到 CSV")
    ap.add_argument("--sweep-grade", action="store_true", help="对比各等级门槛")
    args = ap.parse_args()

    cfg = m.load_config()
    tfs = args.tf or list(TF_SPEC.keys())
    for tf in tfs:
        if tf not in TF_SPEC:
            print(f"未知周期 {tf}，可选: {list(TF_SPEC)}")
            return 1

    print(f"[回测] 周期={tfs} | 等级门槛={args.min_grade or cfg.get('min_signal_grade')} "
          f"| 点差=${cfg.get('spread_usd', 0.2)}")

    infos, all_rows, skipped = {}, [], 0
    for tf in tfs:
        df = fetch_history(tf, cfg, refresh=args.refresh)
        if df.empty:
            print(f"  [{tf}] 取数失败，跳过")
            continue
        infos[tf] = f"{len(df)} 根  {df.index[0]:%Y-%m-%d} → {df.index[-1]:%Y-%m-%d}"
        res = backtest_tf(df, tf, cfg, min_grade=args.min_grade, adx_gate=args.adx_gate)
        all_rows.extend(res["signals"])
        skipped += res["skipped_open"]
        print(f"  [{tf}] {res['bars']} 根K线 → {len(res['signals'])} 笔已结案信号"
              f"（另有 {res['skipped_open']} 笔窗口未走满未计入）")

    s = summarize(all_rows)
    report(s, infos,
           title=f"（门槛≥{args.min_grade or cfg.get('min_signal_grade','C')}级，"
                 f"未结案 {skipped} 笔已剔除）")

    if args.csv and all_rows:
        pd.DataFrame(all_rows).to_csv(args.csv, index=False)
        print(f"\n  明细已导出: {args.csv}")

    if args.sweep_grade:
        print()
        print("=" * 66)
        print("等级门槛对比（其余条件不变）")
        print("=" * 66)
        print(f"  {'门槛':<8s} {'样本':>6s} {'胜率':>7s} {'净值R':>9s} {'期望值':>8s} {'盈亏因子':>8s}")
        for g in ("C", "B", "A", "S"):
            rows = []
            for tf in tfs:
                df = fetch_history(tf, cfg)
                if df.empty:
                    continue
                rows.extend(backtest_tf(df, tf, cfg, min_grade=g)["signals"])
            st = summarize(rows)
            if not st.get("n"):
                print(f"  {g:<8s} {0:>6d} {'—':>7s} {'—':>9s} {'—':>8s} {'—':>8s}")
                continue
            print(f"  {g:<8s} {st['n']:>6d} {st['win_rate']:>6.1f}% {st['net_r']:>+9.2f} "
                  f"{st['expectancy']:>+8.3f} {st['profit_factor']:>8.2f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
