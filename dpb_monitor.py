#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DPB 二次回踩信号监控 - 黄金 XAUUSD
==================================
定时抓取数据 → 计算信号 → 推送到企业微信

用法:
  python dpb_monitor.py          # 单次检查
  python dpb_monitor.py --loop   # 循环监控（每5分钟一次）
"""

import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
import pandas as pd
import numpy as np

# ============================================================
# 配置
# ============================================================
CONFIG_FILE = Path(__file__).parent / "dpb_config.json"

# 目标时区：Twelve Data 默认返回 UTC+10，通过 API 的 timezone 参数显式指定为目标时区
TZ_NAME = "Asia/Shanghai"  # UTC+8 北京时间

# 配置缓存：避免每次请求/节流都重新读盘+解析 JSON（原来是每请求读一次）
_config_cache = {"mtime": 0.0, "data": None}

def load_config():
    """加载配置（带 mtime 缓存），不存在则创建默认"""
    if CONFIG_FILE.exists():
        mtime = CONFIG_FILE.stat().st_mtime
        if _config_cache["data"] is not None and _config_cache["mtime"] == mtime:
            return _config_cache["data"]
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        _config_cache["mtime"] = mtime
        _config_cache["data"] = cfg
        return cfg
    
    default = {
        "wecom_webhook": "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=YOUR_KEY_HERE",
        "twelve_data_key": "YOUR_API_KEY_HERE",
        "ticker": "GC=F",
        "ticker_td": "XAU/USD",
        "timeframes": {
            "5m":  {"interval": "5min"},
            "15m": {"interval": "15min"},
            "1h":  {"interval": "1h"},
            "4h":  {"interval": "4h"},
            "1d":  {"interval": "1day"},
        },
        "ema": {"short1": 15, "short2": 25, "mid1": 50, "mid2": 70},
        "trade_freq": "激进",
        "use_breakout": True,
        "breakout_stability": 8,
        "breakout_dist_atr": 2.0,
        "breakout_consec": 3,
        "breakout_vol_ratio": 1.0,
        "trend_stability": 20,
        "signal_cooldown": 10,
        "breakout_tolerance": 5,
        "min_signal_grade": "C",   # 最低推送等级(S/A/B/C)，治理信号过频
        "rsi_len": 14,
        "rsi_long_min": 40,
        "rsi_short_max": 60,
        "risk1": 1.5,
        "risk2": 3.0,
        "check_interval_minutes": 5,
        "state_file": str(Path(__file__).parent / "dpb_state.json"),
        "timezone": TZ_NAME,
        # 新增参数
        "use_volume_filter": True,
        "vol_ratio": 0.8,
        "use_momentum_filter": True,   # 无 Volume 数据时用「实体/ATR」动能代理
        "momentum_body_ratio": 0.6,    # 实体 ≥ 该倍数×ATR 视为有效动能
        "use_rsi_filter": True,
        "use_pullback_confirm": True,
        "pullback_confirm_bars": 3,
        "sl_cushion": 0.3,
        "breakout_sl_atr": 1.5,   # 突破模式固定 ATR 止损倍数
        "use_trend_cooldown": True,
        "trend_cooldown_bars": 5,
    }
    with open(CONFIG_FILE, "w", encoding="utf-8") as f:
        json.dump(default, f, indent=2, ensure_ascii=False)
    print(f"[配置] 已创建默认配置文件: {CONFIG_FILE}")
    print("[配置] 请编辑 dpb_config.json 填入企业微信 Webhook 地址")
    return default


# ============================================================
# 日志（文件+控制台）
# ============================================================
LOG_FILE = Path(__file__).parent / "dpb_signals.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),  # 写入文件（所有日志）
        logging.StreamHandler(),  # 控制台（只显示信号和错误）
    ],
)
log = logging.getLogger("DPB")

# 控制台只显示信号和错误（数据获取日志已删除，不会输出）

# 信号专用日志（只记录信号，方便复盘）
SIGNAL_LOG_FILE = Path(__file__).parent / "dpb_signals.txt"
signal_log = logging.getLogger("DPB_SIGNAL")
signal_log.setLevel(logging.INFO)
signal_handler = logging.FileHandler(SIGNAL_LOG_FILE, encoding="utf-8")
signal_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S"))
signal_log.addHandler(signal_handler)
signal_log.propagate = False  # 不传播到主logger


# ============================================================
# 数据获取（Twelve Data 多key轮询）
# ============================================================
# 全局请求节流：免费版限制 8 次/分钟/key。
# 多个 key 轮换时，每个 key 实际被请求频率 = gap × key数，
# 因此按 key 数放大间隔即可提速又不超限（保底1.3倍缓冲）。
_last_request_ts = 0.0
# 每个 key 每日 800 次额度；命中限额后切到下一个 key
_DAILY_LIMIT_KEYWORDS = ("out of api credits", "limit of 800", "800 credits")
_key_fail_ts = {}   # key -> 上次因每日限额失败的时间戳
_KEY_RETRY_AFTER = 6 * 3600  # 一个 key 命中限额后，6 小时内不再尝试

# ---------- 多key 真正均衡轮换 + 每日计数监控 ----------
_key_cursor = 0          # 全局游标：每次 fetch 从下一个 key 开始，避免永远打第一个 key
_daily_req = {}          # key -> 当天成功请求次数
_daily_req_day = ""      # 当天日期串（用于跨天重置）
_DAILY_LIMIT = 800       # 每个 key 每日额度
_REQUEST_WARN_RATIO = 0.8  # 使用量达 80% 时告警
# 额度告警阈值（占每日额度比例）——每 key 每阈值每天仅告警一次，避免刷屏
_WARN_THRESHOLDS = (_REQUEST_WARN_RATIO, 0.9, 0.95, 1.0)
_warned_thresholds = {}  # key -> set(今日已告警的阈值)

# 复用连接，减少海外握手开销、降低超时概率
_session = requests.Session()
_CONNECT_TIMEOUT = 15    # 连接超时（秒）
_READ_TIMEOUT = 20       # 读取超时（秒），网络不稳时快速失败重试而非干等30s

def _rotate_key(keys: list) -> str:
    """返回当前应使用的 key，并前移游标实现轮换"""
    global _key_cursor
    if not keys:
        raise ValueError("无可用key")
    key = keys[_key_cursor % len(keys)]
    _key_cursor += 1
    return key

def _reset_daily_if_needed():
    """跨天重置请求计数与告警状态"""
    global _daily_req_day, _daily_req, _warned_thresholds
    today = datetime.now().strftime("%Y-%m-%d")
    if _daily_req_day != today:
        _daily_req_day = today
        _daily_req = {}
        _warned_thresholds = {}

def _track_request(key: str):
    """记录一次成功请求；跨过额度阈值时告警（日志 + 企业微信，每阈值每天仅一次）

    修复前：`used >= 80%` 在 640 次之后恒为真 → 每次请求都告警（刷屏），
    且告警只写日志不推送，key 打满后监控静默停摆无人知晓。
    """
    _reset_daily_if_needed()
    _daily_req[key] = _daily_req.get(key, 0) + 1
    used = _daily_req[key]
    ratio = used / _DAILY_LIMIT
    warned = _warned_thresholds.setdefault(key, set())
    for th in _WARN_THRESHOLDS:
        if ratio >= th and th not in warned:
            warned.add(th)
            pct = int(th * 100)
            log.warning(f"[额度] key {key[:6]}... 今日已用 {used}/{_DAILY_LIMIT} ({pct}%+)")
            # 推送到企业微信：避免额度耗尽后监控静默停摆却无人知晓
            try:
                cfg = load_config()
                webhook = cfg.get("wecom_webhook", "")
                if webhook and "YOUR" not in webhook:
                    send_wecom(
                        webhook,
                        "⚠️ Twelve Data 额度告警",
                        f"> key `{key[:6]}...`\n"
                        f"> 今日已用 **{used}/{_DAILY_LIMIT}**（已达 {pct}%）\n"
                        f"> 额度用尽后该 key 将暂停 {_KEY_RETRY_AFTER // 3600} 小时",
                    )
            except Exception as e:
                log.error(f"[额度] 告警推送失败: {e}")
            break  # 每次请求最多触发一个阈值

def _throttle():
    """按 key 数自适应间隔：加大请求密度，同时每个 key 不超每分钟 8 次"""
    global _last_request_ts
    cfg = load_config()
    keys = cfg.get("twelve_data_keys") or ([cfg.get("twelve_data_key")] if cfg.get("twelve_data_key") else [])
    n = max(1, sum(1 for k in keys if k and k != "YOUR_API_KEY_HERE"))
    # 每 key 60 秒内最多 8 次 → 每次至少 7.5 秒；n 个 key 均匀分摊 → 全局间隔可缩到 7.5/n
    per_key_gap = 7.5
    gap_target = (per_key_gap / n) * 1.3  # 1.3 缓冲防止临街
    now = time.time()
    gap = gap_target - (now - _last_request_ts)
    if gap > 0:
        time.sleep(gap)
    _last_request_ts = time.time()


def _is_daily_limit(msg: str) -> bool:
    """判断是否命中每日额度限制"""
    m = msg.lower()
    return any(kw in m for kw in _DAILY_LIMIT_KEYWORDS)


def _available_keys(cfg: dict) -> list:
    """返回所有可用（未在冷却期）的 key"""
    keys = cfg.get("twelve_data_keys") or ([cfg["twelve_data_key"]] if cfg.get("twelve_data_key") else [])
    now = time.time()
    fresh = []
    for k in keys:
        if not k or k == "YOUR_API_KEY_HERE":
            continue
        # 跳过 6 小时冷却期内的 key
        if k in _key_fail_ts and (now - _key_fail_ts[k]) < _KEY_RETRY_AFTER:
            continue
        fresh.append(k)
    return fresh or [k for k in keys if k and k != "YOUR_API_KEY_HERE"]


def fetch_data(ticker: str, period: str, interval: str, retries: int = 3) -> pd.DataFrame:
    """用 Twelve Data 获取K线数据：多key轮询 + 节流 + 重试"""
    cfg = load_config()
    keys = _available_keys(cfg)
    if not keys:
        raise ValueError("请先配置 twelve_data_keys / twelve_data_key")
    
    last_error = None
    # 外层：轮询所有 key；内层：对每个 key 最多重试 retries 次
    for attempt in range(len(keys) * retries):
        _throttle()
        # 用全局游标轮换 key（真正均衡，而非永远打第一个 key）
        key = _rotate_key(keys)
        try:
            params = {
                "symbol": ticker,
                "interval": interval,
                "outputsize": 1000,
                "apikey": key,
                "timezone": cfg.get("timezone", TZ_NAME),  # 显式时区，避免默认 UTC+10
            }
            resp = _session.get(
                "https://api.twelvedata.com/time_series",
                params=params,
                timeout=(_CONNECT_TIMEOUT, _READ_TIMEOUT),
            )
            # 判空/非JSON防护：超时或海外断流时 body 常为空，
            # 直接 json() 会抛 "Expecting value"，白白再耗一次重试。
            text = resp.text.strip()
            if not resp.ok or not text:
                raise ValueError(f"HTTP {resp.status_code}, 响应为空")
            try:
                data = resp.json()
            except ValueError:
                raise ValueError(
                    f"响应非JSON (HTTP {resp.status_code}, 前80字符: {text[:80]})"
                )
            
            msg = str(data.get("message", ""))
            
            # 命中每日额度 → 标记此 key 冷却，切换下一个
            if _is_daily_limit(msg):
                _key_fail_ts[key] = time.time()
                last_error = f"key {key[:6]}... 每日额度用尽"
                log.warning(f"[额度用尽] key {key[:6]}... 额度用尽，切换其他key")
                continue
            
            # 每分钟限流 → 短等待后重试同一 key
            code = str(data.get("code", ""))
            if code == "429" or "rate" in msg.lower() or "limit" in msg.lower():
                last_error = f"限流: {msg}"
                wait = 20 * ((attempt // max(len(keys),1)) + 1)
                log.warning(f"[限流] {interval} 等待{wait}秒后重试")
                time.sleep(wait)
                continue
            
            # 其他错误
            if data.get("status") == "error":
                raise ValueError(f"API错误: {msg}")
            if "values" not in data:
                raise ValueError(f"未找到数据: {list(data.keys())} {msg}")
            
            # 解析数据
            values = data["values"]
            df = pd.DataFrame(values)
            df = df.rename(columns={
                "datetime": "Date",
                "open": "Open",
                "high": "High",
                "low": "Low",
                "close": "Close",
                "volume": "Volume"
            })
            df["Date"] = pd.to_datetime(df["Date"])  # 已是目标时区（API 显式指定 timezone）
            df = df.set_index("Date")
            df = df.sort_index()
            
            for col in ["Open", "High", "Low", "Close", "Volume"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")
            
            df = df.dropna(subset=["Close"])
            
            if df.empty:
                raise ValueError(f"未获取到数据: {ticker} {interval}")
            
            log.info(f"[数据] 获取 {len(df)} 根K线 (key {key[:6]}...), 范围 {df.index[0]} ~ {df.index[-1]}")
            _track_request(key)  # 计一次成功请求
            return df
            
        except ValueError as e:
            raise
        except Exception as e:
            last_error = str(e)
            log.warning(f"[错误] {interval} 获取失败: {e}，重试")
            # 指数退避：3s→6s→12s→... 网络不稳时避免连续狂打
            time.sleep(3 * (2 ** (attempt // max(len(keys), 1))))
    
    raise RuntimeError(f"所有key多次重试仍失败: {ticker} {interval} ({last_error})")


# ============================================================
# DPB 信号计算（与 Pine Script 逻辑一致）
# ============================================================
def calc_ema(series: pd.Series, length: int) -> pd.Series:
    return series.ewm(span=length, adjust=False).mean()


def calc_atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high, low, close = df["High"], df["Low"], df["Close"]
    prev_close = close.shift(1)
    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(span=length, adjust=False).mean()


def calc_rsi(series: pd.Series, length: int = 14) -> pd.Series:
    delta = series.diff()
    gain = delta.where(delta > 0, 0.0)
    loss = (-delta).where(delta < 0, 0.0)
    avg_gain = gain.ewm(span=length, adjust=False).mean()
    avg_loss = loss.ewm(span=length, adjust=False).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# 交易频率预设：仅当 config 未显式提供对应参数时兜底
_FREQ_PRESETS = {
    "保守": {"trend_stability": 20, "signal_cooldown": 10, "breakout_tolerance": 5},
    "标准": {"trend_stability": 15, "signal_cooldown": 5, "breakout_tolerance": 3},
    "激进": {"trend_stability": 10, "signal_cooldown": 3, "breakout_tolerance": 2},
}

# 信号等级序（用于 A5 最小等级门槛过滤）
_GRADE_ORDER = {"C": 0, "B": 1, "A": 2, "S": 3}


def _apply_freq_preset(cfg: dict) -> dict:
    """套用交易频率预设作为兜底，但 config 中显式提供的参数优先。

    修复前：无论 config 写什么，trend_stability / signal_cooldown /
    breakout_tolerance 都会被 trade_freq 强制覆盖 → 用户改 config 无效。
    """
    freq = cfg.get("trade_freq", "激进")
    preset = _FREQ_PRESETS.get(freq, _FREQ_PRESETS["激进"])
    merged = {k: v for k, v in preset.items() if k not in cfg}
    merged.update(cfg)
    return merged


def _score_signal(row, direction: int, mode: str, **extra) -> tuple:
    """统一信号评分（0-8 分），回踩与突破模式共用；direction: 1=多, -1=空。

    评分维度（每项 0/1）：
      1 趋势稳定  2 趋势强度(EMA分离)  3 RSI 合理区  4 动能确认
      5 模式项    6 形态项             7 带位/稳定性  8 结构确认
    等级：S>=7, A>=5, B>=3, C<3

    修复前：突破模式只算 3 项（上限 = B 级），回踩模式含一项"假设结构过滤通过"
    的假分 → 实测近千根 K 线 S 级恒为 0。此函数用真实指标替代，恢复等级区分度。
    """
    long = direction > 0
    atr = row["atr"]
    s = 0
    # 1 趋势稳定
    s += 1 if (row["stable_up"] if long else row["stable_down"]) else 0
    # 2 趋势强度：EMA15 与 EMA70 分离度
    if not np.isnan(atr) and abs(row["ema15"] - row["ema70"]) > atr * 0.5:
        s += 1
    # 3 RSI 合理区
    rsi = row["rsi"]
    s += 1 if ((50 < rsi < 70) if long else (30 < rsi < 50)) else 0
    # 4 动能确认（含 A1 的无量动能代理）
    s += 1 if row.get("vol_pass", True) else 0
    # 5-7 模式相关项
    if mode == "回踩":
        s += 1 if extra.get("pullback_depth") == 2 else 0
        shadow = row["lower_shadow"] if long else row["upper_shadow"]
        s += 1 if (not np.isnan(shadow) and shadow > 0.5) else 0
        s += 1 if extra.get("band_reason") == "短→中" else 0
    else:  # 突破
        dist = row["long_dist"] if long else row["short_dist"]
        s += 1 if (not np.isnan(atr) and dist > atr * 3) else 0
        consec = row["long_consec"] if long else row["short_consec"]
        s += 1 if consec >= 4 else 0
        s += 1 if (row["bo_stable_up"] if long else row["bo_stable_down"]) else 0
    # 8 结构确认：价格在慢线正确一侧
    s += 1 if ((row["Close"] > row["ema50"]) if long else (row["Close"] < row["ema50"])) else 0
    grade = "S" if s >= 7 else "A" if s >= 5 else "B" if s >= 3 else "C"
    return s, grade


def calc_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    计算 DPB 信号，返回带 signal 列的 DataFrame
    signal: 0=无, 1=买入, -1=卖出
    """
    # config 显式参数优先，trade_freq 仅在缺省时兜底
    c = _apply_freq_preset(cfg)

    df = df.copy()
    
    # EMA
    df["ema15"] = calc_ema(df["Close"], c["ema"]["short1"])
    df["ema25"] = calc_ema(df["Close"], c["ema"]["short2"])
    df["ema50"] = calc_ema(df["Close"], c["ema"]["mid1"])
    df["ema70"] = calc_ema(df["Close"], c["ema"]["mid2"])
    
    # ATR, RSI
    df["atr"] = calc_atr(df, 14)
    df["rsi"] = calc_rsi(df["Close"], c["rsi_len"])
    
    # K线形态
    df["range"] = df["High"] - df["Low"]
    df["lower_shadow"] = np.where(
        df["Close"] < df["Open"],
        (df["Close"] - df["Low"]) / df["range"].replace(0, np.nan),
        (df["Open"] - df["Low"]) / df["range"].replace(0, np.nan),
    )
    df["upper_shadow"] = np.where(
        df["Close"] < df["Open"],
        (df["High"] - df["Open"]) / df["range"].replace(0, np.nan),
        (df["High"] - df["Close"]) / df["range"].replace(0, np.nan),
    )
    df["is_green"] = df["Close"] > df["Open"]
    df["is_red"] = df["Close"] < df["Open"]
    
    # 趋势定义
    fast_top = df[["ema15", "ema25"]].max(axis=1)
    fast_bot = df[["ema15", "ema25"]].min(axis=1)
    slow_top = df[["ema50", "ema70"]].max(axis=1)
    slow_bot = df[["ema50", "ema70"]].min(axis=1)
    
    raw_up = fast_bot > slow_top
    raw_down = fast_top < slow_bot
    
    # 稳定性过滤
    stability = c["trend_stability"]
    df["stable_up"] = raw_up.rolling(stability).sum() == stability
    df["stable_down"] = raw_down.rolling(stability).sum() == stability
    
    # 破位检测
    break_long = (df["Close"] < df["ema70"]).astype(int)
    break_short = (df["Close"] > df["ema70"]).astype(int)
    
    # 连续破位计数
    break_long_count = []
    break_short_count = []
    cnt_l = 0
    cnt_s = 0
    for i in range(len(df)):
        cnt_l = cnt_l + 1 if break_long.iloc[i] else 0
        cnt_s = cnt_s + 1 if break_short.iloc[i] else 0
        break_long_count.append(cnt_l)
        break_short_count.append(cnt_s)
    
    df["break_long_count"] = break_long_count
    df["break_short_count"] = break_short_count
    
    tolerance = c["breakout_tolerance"]
    df["trend_broken_long"] = df["break_long_count"] > tolerance
    df["trend_broken_short"] = df["break_short_count"] > tolerance
    
    df["is_uptrend"] = df["stable_up"] & ~df["trend_broken_long"]
    df["is_downtrend"] = df["stable_down"] & ~df["trend_broken_short"]
    
    # 动能/成交量过滤
    # 优先用真实成交量；若数据源无 Volume（如 XAU/USD 现货金，实测所有周期均无），
    # 回退到用「K线实体 / ATR」作为放量动能代理，避免过滤静默失效。
    use_volume_filter = c.get("use_volume_filter", True)
    vol_ratio = c.get("vol_ratio", 0.8)
    use_momentum_filter = c.get("use_momentum_filter", True)
    momentum_body_ratio = c.get("momentum_body_ratio", 0.6)

    df["body_atr"] = (df["Close"] - df["Open"]).abs() / df["atr"].replace(0, np.nan)
    if "Volume" in df.columns and use_volume_filter:
        vol_sma = df["Volume"].rolling(20).mean()
        df["vol_pass"] = df["Volume"] > vol_sma * vol_ratio
        df["vol_source"] = "volume"
    elif use_momentum_filter:
        # 无成交量：用实体/ATR 衡量 K 线动能（大实体 = 强推进）
        df["vol_pass"] = df["body_atr"] > momentum_body_ratio
        df["vol_source"] = "momentum"
    else:
        df["vol_pass"] = True
        df["vol_source"] = "none"
    
    # 回踩触发
    use_rsi_filter = c.get("use_rsi_filter", True)
    support_short = (df["Low"] <= df["ema15"]) & (df["Close"] >= df["ema25"])
    support_mid = (df["Low"] <= df["ema50"]) & (df["Close"] >= df["ema70"])
    valid_long = df["is_green"] | (df["lower_shadow"] > 0.3)
    rsi_pass_long = df["rsi"] > c["rsi_long_min"] if use_rsi_filter else True
    
    resist_short = (df["High"] >= df["ema15"]) & (df["Close"] <= df["ema25"])
    resist_mid = (df["High"] >= df["ema50"]) & (df["Close"] <= df["ema70"])
    valid_short = df["is_red"] | (df["upper_shadow"] > 0.3)
    rsi_pass_short = df["rsi"] < c["rsi_short_max"] if use_rsi_filter else True
    
    long_trig_short = df["is_uptrend"] & support_short & valid_long & rsi_pass_long & df["vol_pass"]
    long_trig_mid = df["is_uptrend"] & support_mid & valid_long & rsi_pass_long & df["vol_pass"]
    
    short_trig_short = df["is_downtrend"] & resist_short & valid_short & rsi_pass_short & df["vol_pass"]
    short_trig_mid = df["is_downtrend"] & resist_mid & valid_short & rsi_pass_short & df["vol_pass"]
    
    # ==========================================
    # F. 突破模式检测（与TV同步）
    # ==========================================
    use_breakout = c.get("use_breakout", True)
    bo_stability = c.get("breakout_stability", 8)
    bo_dist_atr = c.get("breakout_dist_atr", 2.0)
    bo_consec = c.get("breakout_consec", 3)
    bo_vol_ratio = c.get("breakout_vol_ratio", 1.0)
    
    # 突破模式用更短的稳定性
    df["bo_stable_up"] = raw_up.rolling(bo_stability).sum() == bo_stability
    df["bo_stable_down"] = raw_down.rolling(bo_stability).sum() == bo_stability
    
    # 价格距离EMA70超过N倍ATR
    df["long_dist"] = df["Close"] - df["ema70"]
    df["short_dist"] = df["ema70"] - df["Close"]
    df["long_bo_dist"] = df["long_dist"] > df["atr"] * bo_dist_atr
    df["short_bo_dist"] = df["short_dist"] > df["atr"] * bo_dist_atr
    
    # 连续同向K线计数
    long_consec = []
    short_consec = []
    lc = 0
    sc = 0
    for i in range(len(df)):
        if df["is_green"].iloc[i]:
            lc += 1
        else:
            lc = 0
        if df["is_red"].iloc[i]:
            sc += 1
        else:
            sc = 0
        long_consec.append(lc)
        short_consec.append(sc)
    
    df["long_consec"] = long_consec
    df["short_consec"] = short_consec
    df["long_consec_ok"] = df["long_consec"] >= bo_consec
    df["short_consec_ok"] = df["short_consec"] >= bo_consec
    
    # 突破动能过滤（与 vol_pass 同源：有量用成交量，无量用实体/ATR）
    if "Volume" in df.columns and use_volume_filter:
        df["bo_vol_pass"] = df["Volume"] > df["Volume"].rolling(20).mean() * bo_vol_ratio
    elif use_momentum_filter:
        df["bo_vol_pass"] = df["body_atr"] > (momentum_body_ratio * bo_vol_ratio)
    else:
        df["bo_vol_pass"] = True
    
    # 突破信号条件
    df["long_bo_signal"] = use_breakout & df["bo_stable_up"] & df["long_bo_dist"] & df["long_consec_ok"] & df["bo_vol_pass"] & ~df["trend_broken_long"]
    df["short_bo_signal"] = use_breakout & df["bo_stable_down"] & df["short_bo_dist"] & df["short_consec_ok"] & df["bo_vol_pass"] & ~df["trend_broken_short"]
    
    # ==========================================
    # 趋势反转冷却机制（与TV同步）
    # ==========================================
    use_trend_cooldown = c.get("use_trend_cooldown", True)
    trend_cooldown_bars = c.get("trend_cooldown_bars", 5)
    
    long_trend_cooldown = [False] * len(df)
    short_trend_cooldown = [False] * len(df)
    long_cooldown_cnt = 0
    short_cooldown_cnt = 0
    
    for i in range(len(df)):
        # 检测趋势反转
        if i > 0:
            if df["is_uptrend"].iloc[i] and not df["is_uptrend"].iloc[i-1]:
                long_trend_cooldown[i] = True
                long_cooldown_cnt = 0
            if df["is_downtrend"].iloc[i] and not df["is_downtrend"].iloc[i-1]:
                short_trend_cooldown[i] = True
                short_cooldown_cnt = 0
        
        # 冷却计数
        if long_trend_cooldown[i]:
            long_cooldown_cnt += 1
            if long_cooldown_cnt >= trend_cooldown_bars:
                long_trend_cooldown[i] = False
                long_cooldown_cnt = 0
        
        if short_trend_cooldown[i]:
            short_cooldown_cnt += 1
            if short_cooldown_cnt >= trend_cooldown_bars:
                short_trend_cooldown[i] = False
                short_cooldown_cnt = 0
    
    df["long_trend_cooldown"] = long_trend_cooldown
    df["short_trend_cooldown"] = short_trend_cooldown
    df["long_trend_ok"] = ~df["long_trend_cooldown"] if use_trend_cooldown else True
    df["short_trend_ok"] = ~df["short_trend_cooldown"] if use_trend_cooldown else True
    
    # 状态机（完整遍历所有bar，与TV一致）
    # TV时序：
    #   bar N:   状态机检测到二次回踩 → state 1→2
    #   bar N+1: 检测到 state_prev==2 → 标记信号
    #   bar N+2: 状态机检测到 state==2 → 重置为 0
    signals = [0] * len(df)  # 0=无, 1=买入, -1=卖出
    signal_types = [""] * len(df)  # "回踩" or "突破"
    signal_grades = [""] * len(df)  # 信号等级 S/A/B/C
    signal_scores = [0] * len(df)  # 信号评分 0-8
    signal_bands = [""] * len(df)  # 回踩带 短带/中带/短→中
    signal_pullback_extremes = [np.nan] * len(df)  # 回踩极值（做多记录低点，做空记录高点）
    
    state_long = 0
    band_long = ""
    state_short = 0
    band_short = ""
    last_long_bar = -999999
    last_short_bar = -999999
    cooldown = c["signal_cooldown"]
    
    # 确认窗口
    use_pullback_confirm = c.get("use_pullback_confirm", True)
    pullback_confirm_bars = c.get("pullback_confirm_bars", 3)
    long_confirm_window = 0
    short_confirm_window = 0
    long_pullback_high = np.nan
    short_pullback_low = np.nan
    long_pullback_depth = 0
    short_pullback_depth = 0
    
    # 突破模式冷却（独立于回踩）
    last_long_bo_bar = -999999
    last_short_bo_bar = -999999
    
    # 信号等级捕获
    long_signal_grade = ""
    short_signal_grade = ""
    long_signal_score = 0
    short_signal_score = 0
    long_band_reason = ""
    short_band_reason = ""
    
    for i in range(len(df)):
        # 先检测信号（用上一根bar的状态，与TV的state[1]==2一致）
        if state_long == 2:
            signals[i] = 1
            signal_types[i] = "回踩"
            signal_grades[i] = long_signal_grade
            signal_scores[i] = long_signal_score
            signal_bands[i] = long_band_reason
            signal_pullback_extremes[i] = long_pullback_high
        
        if state_short == 2:
            signals[i] = -1
            signal_types[i] = "回踩"
            signal_grades[i] = short_signal_grade
            signal_scores[i] = short_signal_score
            signal_bands[i] = short_band_reason
            signal_pullback_extremes[i] = short_pullback_low
        
        # 突破模式检测（独立于状态机）
        if df["long_bo_signal"].iloc[i] and (i - last_long_bo_bar) > cooldown:
            if signals[i] == 0:  # 不覆盖回踩信号
                signals[i] = 1
                signal_types[i] = "突破"
                # 统一评分（0-8）
                score, grade = _score_signal(df.iloc[i], 1, "突破")
                signal_grades[i] = grade
                signal_scores[i] = score
                signal_bands[i] = "突破"
            last_long_bo_bar = i
        
        if df["short_bo_signal"].iloc[i] and (i - last_short_bo_bar) > cooldown:
            if signals[i] == 0:
                signals[i] = -1
                signal_types[i] = "突破"
                # 统一评分（0-8）
                score, grade = _score_signal(df.iloc[i], -1, "突破")
                signal_grades[i] = grade
                signal_scores[i] = score
                signal_bands[i] = "突破"
            last_short_bo_bar = i
        
        # 破位重置
        if df["trend_broken_long"].iloc[i]:
            state_long = 0
            band_long = ""
            long_confirm_window = 0
            long_pullback_depth = 0
        elif df["is_uptrend"].iloc[i]:
            if state_long == 0:
                if i - last_long_bar > cooldown and df["long_trend_ok"].iloc[i]:
                    if long_trig_short.iloc[i]:
                        state_long = 1
                        band_long = "SHORT"
                        last_long_bar = i
                        long_pullback_high = df["High"].iloc[i]
                        long_confirm_window = 0
                        long_pullback_depth = 1
                    elif long_trig_mid.iloc[i]:
                        state_long = 1
                        band_long = "MID"
                        last_long_bar = i
                        long_pullback_high = df["High"].iloc[i]
                        long_confirm_window = 0
                        long_pullback_depth = 2
            elif state_long == 1:
                long_confirm_window += 1
                long_confirmed = False
                
                if use_pullback_confirm:
                    # 确认K线：绿色+收盘>回踩高点
                    long_confirmed = (df["is_green"].iloc[i] and 
                                     df["Close"].iloc[i] > long_pullback_high and 
                                     long_confirm_window <= pullback_confirm_bars)
                else:
                    # 不使用确认，直接再次触发
                    if band_long == "SHORT":
                        long_confirmed = long_trig_short.iloc[i] or long_trig_mid.iloc[i]
                    else:
                        long_confirmed = long_trig_mid.iloc[i]
                
                if long_confirmed:
                    long_band_reason = "短带" if band_long == "SHORT" else "中带"
                    if band_long == "SHORT" and (long_trig_mid.iloc[i] or df["Close"].iloc[i] < df["ema50"].iloc[i]):
                        long_band_reason = "短→中"
                    state_long = 2
                    last_long_bar = i
                    
                    # 统一评分（0-8，替代原先含"假设结构过滤"假分的 7 项）
                    long_signal_score, long_signal_grade = _score_signal(
                        df.iloc[i], 1, "回踩",
                        pullback_depth=long_pullback_depth,
                        band_reason=long_band_reason,
                    )
                elif long_confirm_window > pullback_confirm_bars:
                    state_long = 0
                    band_long = ""
                    long_confirm_window = 0
            elif state_long == 2:
                # 第三根bar：信号已确认，重置状态
                state_long = 0
                band_long = ""
        
        if df["trend_broken_short"].iloc[i]:
            state_short = 0
            band_short = ""
            short_confirm_window = 0
            short_pullback_depth = 0
        elif df["is_downtrend"].iloc[i]:
            if state_short == 0:
                if i - last_short_bar > cooldown and df["short_trend_ok"].iloc[i]:
                    if short_trig_short.iloc[i]:
                        state_short = 1
                        band_short = "SHORT"
                        last_short_bar = i
                        short_pullback_low = df["Low"].iloc[i]
                        short_confirm_window = 0
                        short_pullback_depth = 1
                    elif short_trig_mid.iloc[i]:
                        state_short = 1
                        band_short = "MID"
                        last_short_bar = i
                        short_pullback_low = df["Low"].iloc[i]
                        short_confirm_window = 0
                        short_pullback_depth = 2
            elif state_short == 1:
                short_confirm_window += 1
                short_confirmed = False
                
                if use_pullback_confirm:
                    # 确认K线：红色+收盘<回踩低点
                    short_confirmed = (df["is_red"].iloc[i] and 
                                      df["Close"].iloc[i] < short_pullback_low and 
                                      short_confirm_window <= pullback_confirm_bars)
                else:
                    if band_short == "SHORT":
                        short_confirmed = short_trig_short.iloc[i] or short_trig_mid.iloc[i]
                    else:
                        short_confirmed = short_trig_mid.iloc[i]
                
                if short_confirmed:
                    short_band_reason = "短带" if band_short == "SHORT" else "中带"
                    if band_short == "SHORT" and (short_trig_mid.iloc[i] or df["Close"].iloc[i] > df["ema50"].iloc[i]):
                        short_band_reason = "短→中"
                    state_short = 2
                    last_short_bar = i
                    
                    # 统一评分（0-8，替代原先含"假设结构过滤"假分的 7 项）
                    short_signal_score, short_signal_grade = _score_signal(
                        df.iloc[i], -1, "回踩",
                        pullback_depth=short_pullback_depth,
                        band_reason=short_band_reason,
                    )
                elif short_confirm_window > pullback_confirm_bars:
                    state_short = 0
                    band_short = ""
                    short_confirm_window = 0
            elif state_short == 2:
                # 第三根bar：信号已确认，重置状态
                state_short = 0
                band_short = ""
    
    df["signal"] = signals
    df["signal_type"] = signal_types
    df["signal_grade"] = signal_grades
    df["signal_score"] = signal_scores
    df["signal_band"] = signal_bands
    df["signal_pullback_extreme"] = signal_pullback_extremes
    return df


# ============================================================
# 企业微信推送
# ============================================================
def send_wecom(webhook: str, title: str, content: str):
    """发送企业微信机器人消息（复用连接 + 异常防护，避免推送失败拖垮整轮检查）"""
    payload = {
        "msgtype": "markdown",
        "markdown": {
            "content": f"## {title}\n{content}"
        }
    }
    try:
        resp = _session.post(webhook, json=payload, timeout=10)
        resp.raise_for_status()
        result = resp.json()
    except Exception as e:
        log.error(f"[推送] 异常: {e}")
        return
    if result.get("errcode") == 0:
        log.info(f"[推送] 成功: {title}")
    else:
        log.error(f"[推送] 失败: {result}")


def calc_sl_tp(sig: int, row: pd.Series, cfg: dict, tf: str) -> tuple:
    """统一计算止损/止盈，返回 (entry, sl, r_size, r1, r2, tp1, tp2)。
    原逻辑在 format_signal_msg 与 check_signals 中重复，抽成单一来源避免漂移。"""
    entry = row["Close"]
    ema70 = row["ema70"]
    atr = row["atr"]
    sl_cushion = cfg.get("sl_cushion", 0.3)
    pullback_extreme = row.get("signal_pullback_extreme", np.nan)
    sig_type = row.get("signal_type", "")
    bo_sl_atr = cfg.get("breakout_sl_atr", 1.5)

    if sig_type == "突破":
        # 突破模式：价格已远离 EMA70(≥2×ATR)，用 ema70 做止损过宽 → 改用固定 ATR 倍数
        sl = entry - atr * bo_sl_atr if sig > 0 else entry + atr * bo_sl_atr
        r_size = max(abs(entry - sl), atr * 0.1)
    elif sig > 0:
        sl = max(ema70, pullback_extreme - atr * sl_cushion) if not np.isnan(pullback_extreme) else ema70
        r_size = max(abs(entry - sl), atr * 0.1)
    else:
        sl = min(ema70, pullback_extreme + atr * sl_cushion) if not np.isnan(pullback_extreme) else ema70
        r_size = max(abs(sl - entry), atr * 0.1)

    tf_min_map = {"5m": 5, "15m": 15, "1h": 60, "4h": 240, "1d": 1440}
    tf_min = tf_min_map.get(tf, 60)
    if tf_min <= 15:
        r1, r2 = 1.0, 2.0
    elif tf_min <= 60:
        r1, r2 = 1.5, 3.0
    elif tf_min <= 240:
        r1, r2 = 2.0, 4.0
    else:
        r1, r2 = 3.0, 6.0

    tp1 = entry + (r_size if sig > 0 else -r_size) * r1
    tp2 = entry + (r_size if sig > 0 else -r_size) * r2
    return entry, sl, r_size, r1, r2, tp1, tp2


def format_signal_msg(tf: str, signal: int, row: pd.Series, cfg: dict) -> tuple:
    """格式化信号消息，返回 (title, content) 方便标题也精简"""
    entry, sl, _r_size, r1, r2, tp1, tp2 = calc_sl_tp(signal, row, cfg, tf)

    # 方向标签
    d = "多" if signal > 0 else "空"
    sig_type = row.get("signal_type", "")
    grade = row.get("signal_grade", "")
    
    # 标题：一眼看到方向+周期+价格（手机通知预览可见）
    title = f"{'🟢' if signal > 0 else '🔴'}{d} {tf} {entry:.2f}"
    if sig_type:
        title += f"[{sig_type}]"
    if grade:
        title += f"[{grade}]"
    title += f" SL:{sl:.2f} TP1:{tp1:.2f}"
    
    # 内容：补充TP2和时间
    content = (
        f"> 入场: **{entry:.2f}**\n"
        f"> 止损: **{sl:.2f}**\n"
        f"> TP1: **{tp1:.2f}**\n"
        f"> TP2: **{tp2:.2f}**\n"
        f"> {row.name.strftime('%H:%M')}"
    )
    
    return title, content


# ============================================================
# 状态管理（去重）
# ============================================================
def _state_path(state_file: str) -> Path:
    """解析状态文件路径：相对路径基于脚本目录，兼容Windows/Linux服务器"""
    p = Path(state_file)
    if p.is_absolute():
        return p
    return Path(__file__).parent / p


def load_state(state_file: str) -> dict:
    p = _state_path(state_file)
    if p.exists():
        try:
            with open(p, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.error(f"[状态] 状态文件损坏，已重置: {e}")
            return {}
    return {}


def save_state(state_file: str, state: dict):
    p = _state_path(state_file)
    try:
        with open(p, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2, ensure_ascii=False)
    except OSError as e:
        log.error(f"[状态] 保存失败: {e}")


# ============================================================
# 主循环
# ============================================================
def check_signals(cfg: dict, state: dict) -> dict:
    """检查所有周期的信号，返回更新后的状态"""
    ticker = cfg.get("ticker_td", cfg.get("ticker_av", cfg["ticker"]))
    
    for idx, (tf, params) in enumerate(cfg["timeframes"].items()):
        try:
            # 实时抓取：每次循环都拉最新行情
            df = fetch_data(ticker, tf, params["interval"])
            df = calc_signals(df, cfg)
            
            # 取最后一根K线
            last = df.iloc[-1]
            sig = int(last["signal"])

            # A5 信号等级门槛：低于 min_signal_grade 的信号不推送（治理信号过频）
            min_grade = cfg.get("min_signal_grade", "C")
            grade_now = last.get("signal_grade", "")
            if sig != 0 and _GRADE_ORDER.get(grade_now, 0) < _GRADE_ORDER.get(min_grade, 0):
                log.info(f"[过滤] {tf} {grade_now}级 低于门槛 {min_grade} → 跳过推送")
                continue

            # 去重：同一周期同一方向不重复推送
            key = f"{tf}_{sig}"
            if sig != 0 and state.get(key) != last.name.isoformat():
                title, content = format_signal_msg(tf, sig, last, cfg)
                send_wecom(cfg["wecom_webhook"], title, content)
                state[key] = last.name.isoformat()
                log.info(f"[信号] {title} @ {last['Close']:.2f}")
                
                # 记录到信号专用日志（详细版）
                direction = "买入" if sig > 0 else "卖出"
                sig_type = last.get("signal_type", "")
                type_tag = f"[{sig_type}]" if sig_type else ""
                grade = last.get("signal_grade", "")
                score = last.get("signal_score", 0)
                grade_tag = f"[{grade}级{score}/8]" if grade else ""
                band = last.get("signal_band", "")
                band_tag = f"({band})" if band else ""
                
                # 计算止损和TP（统一走 calc_sl_tp，与 format_signal_msg 同源）
                entry, sl, r_size, r1, r2, tp1, tp2 = calc_sl_tp(sig, last, cfg, tf)
                atr = last["atr"]

                vol_status = "量✓" if last.get("vol_pass", True) else "量✗"
                trend_status = "多" if last["is_uptrend"] else "空" if last["is_downtrend"] else "震荡"
                
                signal_log.info(
                    f"{tf} {direction}{type_tag}{grade_tag}{band_tag} | "
                    f"入:{entry:.2f} | SL:{sl:.2f}(R{r_size:.2f}) | "
                    f"TP1:{tp1:.2f}({r1}R) TP2:{tp2:.2f}({r2}R) | "
                    f"RSI:{last['rsi']:.1f} ATR:{atr:.2f} {vol_status} | "
                    f"趋势:{trend_status}"
                )
            elif sig != 0:
                log.info(f"[信号] {tf} 已有推送，跳过")
            # 无信号时不记录日志
                
        except Exception as e:
            log.error(f"[错误] {tf} 检查失败: {e}")
    
    return state


def main():
    parser = argparse.ArgumentParser(description="DPB 二次回踩信号监控")
    parser.add_argument("--loop", action="store_true", help="循环监控模式")
    args = parser.parse_args()
    
    cfg = load_config()
    state = load_state(cfg["state_file"])

    # 启动时打印实际生效的风险参数，避免"改了 config 却不生效"的黑箱
    _eff = _apply_freq_preset(cfg)
    log.info(
        f"[配置] trade_freq={cfg.get('trade_freq', '激进')} | "
        f"生效: trend_stability={_eff['trend_stability']}, "
        f"signal_cooldown={_eff['signal_cooldown']}, "
        f"breakout_tolerance={_eff['breakout_tolerance']}"
    )

    if args.loop:
        interval = cfg.get("check_interval_minutes", 5) * 60
        log.info(f"[启动] 循环监控模式, 间隔 {interval//60} 分钟")
        while True:
            try:
                state = check_signals(cfg, state)
                save_state(cfg["state_file"], state)
            except Exception as e:
                log.error(f"[错误] 循环检查异常: {e}")
            log.info(f"[等待] {interval//60} 分钟后下次检查...")
            time.sleep(interval)
    else:
        log.info("[启动] 单次检查模式")
        state = check_signals(cfg, state)
        save_state(cfg["state_file"], state)
        log.info("[完成] 检查结束")


if __name__ == "__main__":
    main()
