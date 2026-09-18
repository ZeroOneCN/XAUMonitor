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
import sqlite3
from datetime import datetime, timedelta
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
        "wecom_webhooks": [],   # B10 额外告警通道（多机器人冗余，任一成功即可）
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
        "resonance_min_count": 2,  # 多少个周期同向才算多周期共振
        "rsi_len": 14,
        "rsi_long_min": 40,
        "rsi_short_max": 60,
        "risk1": 1.5,
        "risk2": 3.0,
        "check_interval_minutes": 5,
        "state_file": str(Path(__file__).parent / "dpb_state.json"),
        "db_file": "signals.db",   # B8 信号持久化数据库
        "status_file": "dpb_status.json",  # B9 Web 仪表盘状态快照
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
        # 做单策略：仓位管理（把「下多少手」变成信号的一部分）
        "use_position_sizing": True,
        "account_equity": 700,         # 账户资金（USDT/USD）
        "risk_per_trade_pct": 1.0,     # 单笔风险占总资金 %
        "contract_oz": 100,            # 黄金 1 标准手 = 100 盎司 → 0.01手=1盎司, $1波动=$1
        "leverage": 1000,              # 杠杆倍数（用于估算保证金占用）
        "min_lot": 0.01,               # 最小可下单手数
        "max_lot": 10.0,               # 手数安全上限（防手滑重仓）
        "require_resonance": False,    # 是否只推送「多周期共振」的信号
        # 止损距离约束（修复「止损跑到入场价另一侧」的致命 bug）
        "min_sl_atr": 0.3,             # 止损最近：至少 0.3×ATR
        "max_sl_atr": 4.0,             # 止损最远：最多 4×ATR（限制单笔敞口）
        "fallback_sl_atr": 1.5,        # 结构失效（止损落到入场价另一侧）时的兜底倍数
        "outcome_max_age_days": 30,    # 结果追踪回看天数
        # 斐波那契
        "fib_lookback": 50,            # 计算摆动高低点回看的K线数
        "show_fib": True,              # 做单卡片是否展示斐波那契回撤区/扩展目标
        # ADX 趋势强度（信号逻辑精修）
        "adx_len": 14,
        "min_adx": 20,                 # 评分要求的最低 ADX（<20 视为震荡）
        "min_adx_filter": 0,           # >0 时作为硬门槛：ADX 低于该值直接不推送
        # 结果追踪 / 分批止盈模型
        "tp1_exit_pct": 0.5,           # TP1 了结的仓位比例，其余留到 TP2
        "be_after_tp1": True,          # TP1 后止损是否移到保本（剩余仓位按 0R 计）
        "outcome_max_bars": {},        # 各周期扫描窗口(根)，{} 用内置按周期默认
        # 财经数据发布黑名单（动态拉取 ForexFactory 周历）
        "news_filter_enabled": True,
        "news_impact_levels": ["high"],        # high / medium / low
        "news_currencies": ["USD"],            # 关注币种
        "news_blackout_before_min": 30,        # 数据公布前多少分钟禁开新仓
        "news_blackout_after_min": 30,         # 数据公布后多少分钟禁开新仓
        "news_manual_blackouts": [],           # 手工窗口 [{"name","start","end"}]
        # 各周期行情刷新间隔（分钟），省 API 额度；设为 {} 用内置默认
        "fetch_ttl_minutes": {},       # 例 {"5m":4,"15m":8,"1h":15,"4h":30,"1d":60}
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
            # 推送到所有告警通道（B10 冗余）：避免额度耗尽后监控静默停摆却无人知晓
            try:
                cfg = load_config()
                send_alert(
                    cfg,
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
_FIB_RETR = (0.382, 0.5, 0.618)     # 黄金回撤区
_FIB_EXT = (1.272, 1.618)           # 扩展目标位
_SCORE_MAX = 10                     # 信号满分（含斐波那契 + ADX 维度）


def calc_adx(df: pd.DataFrame, length: int = 14):
    """Wilder ADX / +DI / -DI。

    ADX 衡量「趋势有多强」（与方向无关）：
      - ADX < 20  → 震荡/黏合，均线排列常常是假信号
      - ADX > 25  → 明确趋势
    +DI/-DI 给出方向：+DI 在上 = 多头占优。

    这是「信号逻辑精修」的关键——原策略只看 EMA 排列（滞后且易被黏合骗），
    ADX 用真实的定向动量确认趋势是否值得跟。
    """
    high, low, close = df["High"], df["Low"], df["Close"]
    up = high.diff()
    dn = -low.diff()
    plus_dm = np.where((up > dn) & (up > 0), up, 0.0)
    minus_dm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low - close.shift()).abs(),
    ], axis=1).max(axis=1)
    alpha = 1.0 / length
    atr_w = tr.ewm(alpha=alpha, adjust=False).mean().replace(0, np.nan)
    plus_di = 100 * pd.Series(plus_dm, index=df.index).ewm(alpha=alpha, adjust=False).mean() / atr_w
    minus_di = 100 * pd.Series(minus_dm, index=df.index).ewm(alpha=alpha, adjust=False).mean() / atr_w
    dx = ((plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)) * 100
    adx = dx.ewm(alpha=alpha, adjust=False).mean()
    return adx, plus_di, minus_di


def _grade_of(score: int, n: int = _SCORE_MAX) -> str:
    """按比例折算等级，使新增评分维度时门槛自动等比缩放（不破坏原有区分度）。"""
    r = score / n if n else 0
    return "S" if r >= 0.875 else "A" if r >= 0.625 else "B" if r >= 0.375 else "C"


def calc_fib(df: pd.DataFrame, lookback: int = 50) -> pd.DataFrame:
    """斐波那契回撤/扩展位（基于最近 lookback 根K线的摆动高/低点）。

    上升趋势：自高点向下回撤 → 支撑区；扩展位向上 → 止盈目标。
    下降趋势：自低点向上反弹 → 阻力区；扩展位向下 → 止盈目标。
    """
    hi = df["High"].rolling(lookback, min_periods=5).max()
    lo = df["Low"].rolling(lookback, min_periods=5).min()
    rng = (hi - lo).replace(0, np.nan)

    df["fib_hi"], df["fib_lo"] = hi, lo
    for r in _FIB_RETR:
        df[f"fib_up_{r}"] = hi - rng * r       # 上升趋势回撤支撑
        df[f"fib_dn_{r}"] = lo + rng * r       # 下降趋势反弹阻力
    for e in _FIB_EXT:
        df[f"fib_ext_up_{e}"] = lo + rng * e   # 上升趋势扩展目标（在高点之上）
        df[f"fib_ext_dn_{e}"] = hi - rng * e   # 下降趋势扩展目标（在低点之下）
    return df


def _fib_zone(row, long: bool, atr: float, tol_atr: float = 0.2):
    """返回该方向斐波那契回撤区的 (下沿, 上沿)，无效时返回 None。"""
    if long:
        a, b = row.get("fib_up_0.382"), row.get("fib_up_0.618")
    else:
        a, b = row.get("fib_dn_0.382"), row.get("fib_dn_0.618")
    if a is None or b is None or np.isnan(a) or np.isnan(b):
        return None
    tol = (atr * tol_atr) if (atr and not np.isnan(atr)) else 0.0
    return min(a, b) - tol, max(a, b) + tol


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
    """统一信号评分（0-9 分），回踩与突破模式共用；direction: 1=多, -1=空。

    评分维度（每项 0/1，共 9 项）：
      1 趋势稳定  2 趋势强度(EMA分离)  3 RSI 合理区  4 动能确认
      5 模式项    6 形态项             7 带位/稳定性  8 结构确认
      9 斐波那契回撤区（入场落在 0.382~0.618 黄金带）
    等级按比例折算（见 _grade_of）：S≥87.5% / A≥62.5% / B≥37.5%

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
    # 9 斐波那契回撤区确认：入场落在 0.382~0.618 黄金回撤带
    #   （DPB 的「二次回踩」本质就是等价格回到支撑，和斐波那契回撤区天然同源）
    zone = _fib_zone(row, long, atr)
    if zone and zone[0] <= row["Close"] <= zone[1]:
        s += 1
    # 10 ADX 趋势强度 + DI 方向一致性
    #   （均线排列是滞后指标，ADX<阈值说明只是黏合震荡，DI 反向说明动能不支持）
    adx = row.get("adx")
    pdi, mdi = row.get("plus_di"), row.get("minus_di")
    min_adx = extra.get("min_adx", 20)
    if adx is not None and not np.isnan(adx) and adx >= min_adx:
        di_ok = True
        if pdi is not None and mdi is not None and not (np.isnan(pdi) or np.isnan(mdi)):
            di_ok = (pdi > mdi) if long else (mdi > pdi)
        if di_ok:
            s += 1
    return s, _grade_of(s)


def calc_signals(df: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """
    计算 DPB 信号，返回带 signal 列的 DataFrame
    signal: 0=无, 1=买入, -1=卖出
    """
    # config 显式参数优先，trade_freq 仅在缺省时兜底
    c = _apply_freq_preset(cfg)
    min_adx = c.get("min_adx", 20)   # ADX 趋势强度门槛（精修项）

    df = df.copy()
    
    # EMA
    df["ema15"] = calc_ema(df["Close"], c["ema"]["short1"])
    df["ema25"] = calc_ema(df["Close"], c["ema"]["short2"])
    df["ema50"] = calc_ema(df["Close"], c["ema"]["mid1"])
    df["ema70"] = calc_ema(df["Close"], c["ema"]["mid2"])
    
    # ATR, RSI
    df["atr"] = calc_atr(df, 14)
    df["rsi"] = calc_rsi(df["Close"], c["rsi_len"])

    # 斐波那契回撤/扩展位
    df = calc_fib(df, c.get("fib_lookback", 50))

    # ADX 趋势强度（精修：区分「真趋势」与「均线黏合震荡」）
    df["adx"], df["plus_di"], df["minus_di"] = calc_adx(df, c.get("adx_len", 14))
    
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
                score, grade = _score_signal(df.iloc[i], 1, "突破", min_adx=min_adx)
                signal_grades[i] = grade
                signal_scores[i] = score
                signal_bands[i] = "突破"
            last_long_bo_bar = i
        
        if df["short_bo_signal"].iloc[i] and (i - last_short_bo_bar) > cooldown:
            if signals[i] == 0:
                signals[i] = -1
                signal_types[i] = "突破"
                # 统一评分（0-8）
                score, grade = _score_signal(df.iloc[i], -1, "突破", min_adx=min_adx)
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
                        min_adx=min_adx,
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
                        min_adx=min_adx,
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


def _webhook_list(cfg: dict) -> list:
    """返回所有已配置的告警 webhook（B10 多通道冗余，去重）。"""
    whs = list(cfg.get("wecom_webhooks") or [])
    single = cfg.get("wecom_webhook")
    if single and single not in whs:
        whs.insert(0, single)
    return [w for w in whs if w and "YOUR" not in w]


def send_alert(cfg: dict, title: str, content: str) -> bool:
    """向所有配置的告警通道推送，任一成功即返回 True（B10 告警通道冗余）。

    单通道故障（如单个企业微信机器人被限/失效）不再导致告警丢失。
    """
    whs = _webhook_list(cfg)
    if not whs:
        log.error("[推送] 未配置任何告警 webhook")
        return False
    ok = False
    for wh in whs:
        try:
            payload = {"msgtype": "markdown", "markdown": {"content": f"## {title}\n{content}"}}
            resp = _session.post(wh, json=payload, timeout=10)
            resp.raise_for_status()
            result = resp.json()
            if result.get("errcode") == 0:
                log.info(f"[推送] 成功: {title} → ...{wh[-8:]}")
                ok = True
            else:
                log.error(f"[推送] 失败({wh[-8:]}): {result}")
        except Exception as e:
            log.error(f"[推送] 异常({wh[-8:]}): {e}")
    return ok


def calc_position_size(entry: float, sl: float, cfg: dict) -> dict:
    """按「账户资金 × 单笔风险%」与止损距离计算建议手数。

    黄金 1 标准手 = contract_oz 盎司（默认 100），价格每变动 $1
    对应盈亏 = contract_oz 美元。于是：

        风险金额 = 账户资金 × 单笔风险%
        手数     = 风险金额 / (|入场 - 止损| × contract_oz)

    这是「做单策略」最关键的一环：把下单量从手感/情绪，变成可计算的数字。
    两轮爆仓的根因都是「方向看对但仓位远超账户承受」——本函数直接堵住它。

    当按风险%算出的手数低于最小手数时，标记 undersized 并在推送里明说
    「止损过宽 / 账户偏小」，而不是让用户稀里糊涂下重仓。
    """
    equity = float(cfg.get("account_equity", 1000) or 0)
    risk_pct = float(cfg.get("risk_per_trade_pct", 1.0) or 0)
    contract_oz = float(cfg.get("contract_oz", 100) or 0)
    min_lot = float(cfg.get("min_lot", 0.01) or 0)
    max_lot = float(cfg.get("max_lot", 10.0) or 0)

    sl_dist = abs(entry - sl)
    if sl_dist <= 0 or contract_oz <= 0 or equity <= 0:
        return {"ok": False, "reason": "参数无效"}

    risk_amount = equity * risk_pct / 100.0
    raw_lots = risk_amount / (sl_dist * contract_oz)

    # 注意：必须用「未取整」的手数判断是否低于最小手数，
    # 否则 0.0069 手会被四舍五入成 0.01 而逃过「止损过宽」告警。
    capped = None
    if raw_lots < min_lot:
        lots = min_lot
        capped = "undersized"     # 想按风险%下也下不了 → 止损过宽 / 账户偏小
    else:
        lots = round(raw_lots, 2)
        if max_lot > 0 and lots > max_lot:
            lots = max_lot
            capped = "capped"     # 安全上限截断

    actual_risk = lots * sl_dist * contract_oz
    leverage = float(cfg.get("leverage", 1000) or 1)
    # 保证金占用 ≈ 名义价值 / 杠杆 = 入场价 × 盎司数 / 杠杆
    margin = entry * contract_oz * lots / leverage if leverage > 0 else 0.0

    return {
        "ok": True,
        "lots": lots,
        "raw_lots": raw_lots,
        "risk_amount": risk_amount,       # 按风险%应承担
        "actual_risk": actual_risk,       # 实际下单后的风险
        "actual_pct": actual_risk / equity * 100 if equity else 0.0,
        "sl_dist": sl_dist,
        "risk_pct": risk_pct,
        "equity": equity,
        "margin": margin,
        "leverage": leverage,
        "capped": capped,
        "point_value": contract_oz,
    }


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
    min_sl_atr = cfg.get("min_sl_atr", 0.3)   # 止损最近距离（ATR倍数）
    max_sl_atr = cfg.get("max_sl_atr", 4.0)   # 止损最远距离，限制单笔敞口
    fallback_sl_atr = cfg.get("fallback_sl_atr", 1.5)  # 结构失效时的兜底止损

    if sig_type == "突破":
        # 突破模式：价格已远离 EMA70(≥2×ATR)，用 ema70 做止损过宽 → 改用固定 ATR 倍数
        sl = entry - atr * bo_sl_atr if sig > 0 else entry + atr * bo_sl_atr
    elif sig > 0:
        cand = [ema70]
        if not np.isnan(pullback_extreme):
            cand.append(pullback_extreme - atr * sl_cushion)
        sl = max(cand)
    else:
        cand = [ema70]
        if not np.isnan(pullback_extreme):
            cand.append(pullback_extreme + atr * sl_cushion)
        sl = min(cand)

    # ---- 修正（由结果追踪暴露）----
    # 原逻辑下「回踩做多」可能算出 sl > entry（止损跑到入场价上方），
    # 后果是下一根K线必然判定"已止损"，信号从诞生就注定亏损。
    # 结构失效时退回 1.5×ATR 的稳健止损（而非夹到 0.3×ATR 被噪音扫掉）。
    if sig > 0 and not (sl < entry):
        sl = entry - atr * fallback_sl_atr
    if sig < 0 and not (sl > entry):
        sl = entry + atr * fallback_sl_atr

    # 统一夹紧止损距离到 [min_sl_atr, max_sl_atr]×ATR
    if atr and not np.isnan(atr) and atr > 0:
        if sig > 0:
            sl = min(sl, entry - atr * min_sl_atr)   # 不能贴太近，更不能在上方
            sl = max(sl, entry - atr * max_sl_atr)   # 不能太远
        else:
            sl = max(sl, entry + atr * min_sl_atr)
            sl = min(sl, entry + atr * max_sl_atr)

    r_size = max(abs(entry - sl), (atr * 0.1) if (atr and not np.isnan(atr)) else 0.01)

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


def format_signal_msg(tf: str, signal: int, row: pd.Series, cfg: dict, resonance_tfs=None) -> tuple:
    """格式化「做单卡片」，返回 (title, content)。

    除入场/止损/止盈外，新增仓位管理信息（建议手数 + 风险金额），
    让信号从「看方向」升级为「可直接照着下单」。
    resonance_tfs: 同向共振的周期列表；>=2 时加 🔥 共振标记。
    """
    entry, sl, r_size, r1, r2, tp1, tp2 = calc_sl_tp(signal, row, cfg, tf)

    # 方向标签
    d = "多" if signal > 0 else "空"
    sig_type = row.get("signal_type", "")
    grade = row.get("signal_grade", "")
    resonance = bool(resonance_tfs) and len(resonance_tfs) >= 2

    # 做单策略：仓位管理
    ps = calc_position_size(entry, sl, cfg) if cfg.get("use_position_sizing", True) else None
    lot_txt = f" {ps['lots']:.2f}手" if ps and ps.get("ok") else ""

    # 标题：手机通知预览里就能看到 方向/周期/价格/手数/止损止盈
    title = f"{'🟢' if signal > 0 else '🔴'}{d} {tf} {entry:.2f}"
    if sig_type:
        title += f"[{sig_type}]"
    if grade:
        title += f"[{grade}]"
    if resonance:
        title = "🔥" + title
    title += f"{lot_txt} SL:{sl:.2f} TP1:{tp1:.2f}"

    # 内容：完整做单卡片
    lines = [
        f"> 入场: **{entry:.2f}**",
        f"> 止损: **{sl:.2f}**  (距离 {r_size:.2f})",
        f"> TP1: **{tp1:.2f}**  ({r1}R)",
        f"> TP2: **{tp2:.2f}**  ({r2}R)",
    ]
    # 斐波那契：回撤区（入场位置质量）+ 扩展目标（另一组止盈参考）
    if cfg.get("show_fib", True):
        z = _fib_zone(row, signal > 0, row.get("atr"))
        if z:
            lines.append(
                f"> 📐 斐波回撤区: {z[0]:.1f} ~ {z[1]:.1f} "
                f"{'✓ 现价在区内' if z[0] <= entry <= z[1] else '— 现价在区外'}"
            )
        k1, k2 = (("fib_ext_up_1.272", "fib_ext_up_1.618") if signal > 0
                  else ("fib_ext_dn_1.272", "fib_ext_dn_1.618"))
        v1, v2 = row.get(k1), row.get(k2)
        if v1 is not None and v2 is not None and not np.isnan(v1) and not np.isnan(v2):
            lines.append(f"> 🎯 斐波扩展目标: {v1:.1f} / {v2:.1f}")
    # ADX 趋势强度（精修项：判断是真趋势还是黏合震荡）
    adx = row.get("adx")
    if adx is not None and not np.isnan(adx):
        strength = "强趋势" if adx >= 25 else ("偏弱/震荡" if adx < 20 else "中等")
        pdi, mdi = row.get("plus_di"), row.get("minus_di")
        di_txt = ""
        if pdi is not None and mdi is not None and not (np.isnan(pdi) or np.isnan(mdi)):
            di_txt = f"  +DI {pdi:.0f} / -DI {mdi:.0f}"
        lines.append(f"> 📈 ADX **{adx:.0f}**（{strength}）{di_txt}")
    if ps and ps.get("ok"):
        lines.append(
            f"> 💰 **手数: {ps['lots']:.2f} 手**"
            f" | 保证金 ~${ps['margin']:.2f} (占用 {ps['margin'] / ps['equity'] * 100:.1f}%)"
        )
        over = ps["actual_pct"] > ps["risk_pct"] * 1.5
        lines.append(
            f"> 风险: **${ps['actual_risk']:.2f}** = 账户 **{ps['actual_pct']:.2f}%**"
            f" (目标 {ps['risk_pct']:.1f}%){' ⚠️超目标' if over else ''}"
        )
        if ps["capped"] == "undersized" and over:
            # 仅在「最小手数导致实际风险明显超标」时才提示，避免每单都刷
            lines.append(
                f"> ⚠️ 止损偏宽：按目标风险仅需 {ps['raw_lots']:.4f} 手，受最小手数限制"
            )
        elif ps["capped"] == "capped":
            lines.append(f"> ⚠️ 已达手数上限，按 {ps['lots']:.2f} 手截断")
    if resonance:
        lines.append(f"> 🔥 **多周期共振**({len(resonance_tfs)}个): {', '.join(resonance_tfs)}")
    lines.append(f"> {row.name.strftime('%H:%M')}")

    return title, "\n".join(lines) + "\n"


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
# B8 信号持久化（SQLite，便于复盘 / 统计胜率）
# ============================================================
def _db_path(db_file: str) -> Path:
    p = Path(db_file)
    return p if p.is_absolute() else Path(__file__).parent / p


def init_db(db_file: str):
    """建表（幂等），启动时调用一次"""
    try:
        with sqlite3.connect(_db_path(db_file)) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, pushed_at TEXT, timeframe TEXT, direction INTEGER,
                    sig_type TEXT, grade TEXT, score INTEGER, band TEXT,
                    entry REAL, sl REAL, tp1 REAL, tp2 REAL, r_size REAL,
                    rsi REAL, atr REAL, resonance INTEGER, close REAL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_tf_ts ON signals(timeframe, ts)")
            # 结果追踪表（做单策略复盘：记录每笔信号最终先碰 SL 还是 TP）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS signal_outcomes (
                    signal_id INTEGER PRIMARY KEY,
                    timeframe TEXT, direction INTEGER,
                    outcome TEXT,        -- SL / TP1 / TP2 / open / timeout
                    r_multiple REAL,     -- 最终R倍数(-1=止损, +r1=TP1, +r2=TP2)
                    bars INTEGER,        -- 多少根K线出结果
                    mfe REAL,            -- 最大有利偏移(R)
                    mae REAL,            -- 最大不利偏移(R)
                    resolved_at TEXT,
                    updated_at TEXT
                )
            """)
            conn.commit()
    except Exception as e:
        log.error(f"[DB] 建表失败: {e}")


def save_signal_db(db_file: str, tf: str, sig: int, row, entry, sl, tp1, tp2, r_size, resonance):
    """写入一条信号记录（失败仅记日志，不影响推送）"""
    try:
        with sqlite3.connect(_db_path(db_file)) as conn:
            conn.execute(
                "INSERT INTO signals (ts, pushed_at, timeframe, direction, sig_type, grade,"
                " score, band, entry, sl, tp1, tp2, r_size, rsi, atr, resonance, close)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row.name.isoformat(),
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    tf,
                    int(sig),
                    row.get("signal_type", ""),
                    row.get("signal_grade", ""),
                    int(row.get("signal_score", 0) or 0),
                    row.get("signal_band", ""),
                    float(entry), float(sl), float(tp1), float(tp2), float(r_size),
                    float(row["rsi"]), float(row["atr"]),
                    int(resonance or 0),
                    float(row["Close"]),
                ),
            )
            conn.commit()
    except Exception as e:
        log.error(f"[DB] 信号写入失败: {e}")


def _already_pushed(db_file: str, tf: str, bar_iso: str, direction: int) -> bool:
    """DB 级去重：同一「周期 + K线 + 方向」是否已推送过。

    第二道防线——去重状态文件丢失/损坏、或异常重启导致内存状态丢失时，
    防止同一根K线的信号被重复推送（会把用户刷屏）。
    """
    try:
        with sqlite3.connect(_db_path(db_file)) as conn:
            row = conn.execute(
                "SELECT 1 FROM signals WHERE timeframe=? AND ts=? AND direction=? LIMIT 1",
                (tf, bar_iso, int(direction)),
            ).fetchone()
            return bool(row)
    except Exception as e:
        log.error(f"[DB] 去重检查失败: {e}")
        return False


# ============================================================
# 结果追踪：验证闭环的地基
# ============================================================
def _r_of(price: float, entry: float, r_size: float, direction: int) -> float:
    """把价格折算成 R 倍数（+1R = 赚一个止损距离）"""
    return (price - entry) * direction / r_size if r_size else 0.0


# 各周期的结果追踪扫描窗口（根K线）。窗口走满才允许把「TP1 已到但未到 TP2」
# 的单子结案；窗口没走满就继续挂着追踪，否则会把还在跑的单子提前锁死。
_OUTCOME_MAX_BARS = {
    "5m": 288,    # 24 小时
    "15m": 96,    # 24 小时
    "1h": 72,     # 3 天
    "4h": 30,     # 5 天
    "1d": 14,
}


def _evaluate_one(df, direction, entry, sl, tp1, tp2, r_size, ts, max_bars, cfg=None):
    """在信号之后的 K 线里逐根扫描，判断先碰止损还是止盈。

    保守原则：同一根 K 线内同时触及 SL 与 TP 时，按「先止损」处理
    （避免高估胜率——回测里最常见的自欺来源）。

    分批止盈模型（tp1_exit_pct 默认 0.5 = TP1 平一半，之后止损移到保本）：
      只碰 SL               → SL，-1.00R
      TP1 后碰 TP2          → TP2，+1.50R（0.5×1 + 0.5×2）
      TP1 后碰 SL(保本)      → TP1，+0.50R（0.5×1 + 0.5×0）
      TP1 后窗口走完未再触发  → TP1，+0.50R（剩余按保本离场计）

    关键：`done` 表示是否**真正结案**。只有终局事件发生、或扫描窗口走满
    （len(bars) >= max_bars）才算结案；否则返回 done=False 继续追踪。
    早期版本只要碰到 TP1 就立刻结案——哪怕当时只有 3 根 K 线——
    结果单子还在跑却已锁死在 TP1，之后到了 TP2 也永远记不到。
    """
    long = direction > 0
    try:
        ts_dt = pd.Timestamp(ts)
    except Exception:
        return None
    if getattr(df.index, "tz", None) is not None and ts_dt.tzinfo is None:
        ts_dt = ts_dt.tz_localize(df.index.tz)

    idx = int(df.index.searchsorted(ts_dt))
    if idx >= len(df):
        return None          # 信号太新，数据里还没有它之后的 K 线
    bars = df.iloc[idx + 1: idx + 1 + max_bars]
    if bars.empty:
        return {"outcome": "open", "r": 0.0, "bars": 0,
                "mfe": 0.0, "mae": 0.0, "done": False}

    cfg = cfg or {}
    pct = float(cfg.get("tp1_exit_pct", 0.5))          # TP1 了结的仓位比例
    be = bool(cfg.get("be_after_tp1", True))            # TP1 后止损是否移到保本
    r1 = _r_of(tp1, entry, r_size, direction)
    r2 = _r_of(tp2, entry, r_size, direction)

    def blend(rest_r: float) -> float:
        """TP1 部分止盈 + 剩余仓位的综合 R 倍数"""
        return pct * r1 + (1 - pct) * rest_r

    tp1_hit = False
    mfe = mae = 0.0
    for n, (_, b) in enumerate(bars.iterrows(), start=1):
        hi, lo = float(b["High"]), float(b["Low"])
        # 有利/不利偏移（统一用方向折算，long/short 都取正）
        fav = (hi - entry) / r_size if long else (entry - lo) / r_size
        adv = (entry - lo) / r_size if long else (hi - entry) / r_size
        mfe, mae = max(mfe, fav), max(mae, adv)

        # 止损判定价：TP1 之后若把止损移到保本，实际出场价就是「入场价」，
        # 不能还按原始止损价判——否则「TP1→回到入场(保本出场)→再冲TP2」这种情况
        # 会被误判成还能拿到 TP2，高估收益。
        eff_sl = entry if (tp1_hit and be) else sl
        # 1) 先看止损/保本出场（保守）
        if (long and lo <= eff_sl) or ((not long) and hi >= eff_sl):
            if tp1_hit:
                # TP1 已平一半；剩余一半在保本(0R) 或 原止损(-1R) 出场
                return {"outcome": "TP1", "r": blend(0.0 if be else -1.0),
                        "bars": n, "mfe": mfe, "mae": mae, "done": True}
            return {"outcome": "SL", "r": -1.0, "bars": n,
                    "mfe": mfe, "mae": mae, "done": True}
        # 2) TP2（能到 TP2 必然已越过 TP1，仓位已是一半）
        if (long and hi >= tp2) or ((not long) and lo <= tp2):
            return {"outcome": "TP2", "r": blend(r2), "bars": n,
                    "mfe": mfe, "mae": mae, "done": True}
        # 3) TP1（先记达标，继续扫描看能否到 TP2 / 是否回撤）
        if (long and hi >= tp1) or ((not long) and lo <= tp1):
            tp1_hit = True

    if tp1_hit:
        # 窗口真正走满才算结案，否则继续追踪（可能还要到 TP2）
        return {"outcome": "TP1", "r": blend(0.0), "bars": len(bars),
                "mfe": mfe, "mae": mae, "done": len(bars) >= max_bars}
    return {"outcome": "open", "r": 0.0, "bars": len(bars),
            "mfe": mfe, "mae": mae, "done": False}


def evaluate_outcomes(cfg: dict, max_bars: int = 300, df_cache: dict = None) -> dict:
    """追踪已推送信号的实际结果（先碰 SL 还是 TP、最终 R 倍数）。

    没有这一层，任何参数调整都是盲猜——这是「验证闭环」的地基。
    按周期分组取数，同一周期只抓一次，节省 API 配额。

    df_cache: check_signals 本轮已抓好的 {tf: df}。命中时**零额外 API 消耗**
    （否则每轮会为每个周期再抓一次，把额度消耗翻倍）。
    """
    db_file = cfg.get("db_file", "signals.db")
    df_cache = df_cache or {}
    max_age_days = int(cfg.get("outcome_max_age_days", 30))
    cutoff = (datetime.now() - timedelta(days=max_age_days)).strftime("%Y-%m-%d %H:%M:%S")

    try:
        with sqlite3.connect(_db_path(db_file)) as conn:
            rows = conn.execute(
                "SELECT s.id, s.timeframe, s.direction, s.entry, s.sl, s.tp1, s.tp2, s.r_size, s.ts"
                " FROM signals s LEFT JOIN signal_outcomes o ON o.signal_id = s.id"
                # 未结案的都要继续追踪：除 open 外，TP1 也可能还在往 TP2 走，
                # 所以判据是 resolved_at 为空，而不是 outcome='open'
                " WHERE (o.signal_id IS NULL OR o.resolved_at IS NULL) AND s.pushed_at >= ?"
                " ORDER BY s.id", (cutoff,)
            ).fetchall()
    except Exception as e:
        log.error(f"[追踪] 读取待追踪信号失败: {e}")
        return {}

    if not rows:
        return {}

    by_tf = {}
    for r in rows:
        by_tf.setdefault(r[1], []).append(r)

    stats = {"open": 0, "SL": 0, "TP1": 0, "TP2": 0}
    now_s = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    mb_cfg = cfg.get("outcome_max_bars") or _OUTCOME_MAX_BARS

    for tf, sigs in by_tf.items():
        params = (cfg.get("timeframes") or {}).get(tf)
        if not params:
            continue
        # 优先复用本轮已抓好的数据（零 API 消耗）；只有缺失时才真正抓取
        df = df_cache.get(tf)
        if df is None:
            try:
                ticker = cfg.get("ticker_td", cfg.get("ticker_av", cfg["ticker"]))
                df = fetch_data(ticker, tf, params["interval"])
                log.info(f"[追踪] {tf} 缓存未命中，单独抓取一次")
            except Exception as e:
                log.error(f"[追踪] {tf} 取数失败: {e}")
                continue

        for (sid, _tf, direction, entry, sl, tp1, tp2, r_size, ts) in sigs:
            try:
                res = _evaluate_one(df, direction, entry, sl, tp1, tp2, r_size, ts,
                                    int(mb_cfg.get(tf, max_bars)), cfg)
            except Exception as e:
                log.error(f"[追踪] 信号#{sid} 评估失败: {e}")
                continue
            if not res:
                continue
            stats[res["outcome"]] = stats.get(res["outcome"], 0) + 1
            # 只有「真正结案」才写 resolved_at；TP1 未到 TP2 且窗口没走满时
            # 仍然挂为追踪中，这样之后到了 TP2 还能把结果升级上去
            resolved = now_s if res.get("done") else None
            try:
                with sqlite3.connect(_db_path(db_file)) as conn:
                    conn.execute(
                        "INSERT INTO signal_outcomes (signal_id, timeframe, direction, outcome,"
                        " r_multiple, bars, mfe, mae, resolved_at, updated_at)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?)"
                        " ON CONFLICT(signal_id) DO UPDATE SET outcome=excluded.outcome,"
                        " r_multiple=excluded.r_multiple, bars=excluded.bars, mfe=excluded.mfe,"
                        " mae=excluded.mae, resolved_at=excluded.resolved_at,"
                        " updated_at=excluded.updated_at",
                        (sid, tf, direction, res["outcome"], res["r"], res["bars"],
                         res["mfe"], res["mae"], resolved, now_s),
                    )
                    conn.commit()
            except Exception as e:
                log.error(f"[追踪] 信号#{sid} 写库失败: {e}")

    log.info(
        f"[追踪] 本轮评估 {len(rows)} 笔 → 止损{stats.get('SL', 0)} "
        f"TP1:{stats.get('TP1', 0)} TP2:{stats.get('TP2', 0)} 追踪中:{stats.get('open', 0)}"
    )
    return stats


def outcome_stats(db_file: str) -> dict:
    """汇总已结案信号的胜率与期望值（供仪表盘/日志复盘）

    只统计 `resolved_at` 已落地的单子。仍在追踪的（open / TP1 未到 TP2）
    单独计入 tracking，不混进胜率——否则「还在跑的单子」会被算成输赢，
    数字就不可信了。
    """
    try:
        with sqlite3.connect(_db_path(db_file)) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT o.outcome, o.r_multiple, o.bars, s.grade, s.sig_type, s.timeframe"
                " FROM signal_outcomes o JOIN signals s ON s.id = o.signal_id"
                " WHERE o.resolved_at IS NOT NULL"
            ).fetchall()
            row = conn.execute(
                "SELECT COUNT(*) FROM signal_outcomes WHERE resolved_at IS NULL"
            ).fetchone()
            tracking = int(row[0]) if row else 0
    except Exception as e:
        log.error(f"[统计] 读取失败: {e}")
        return {"closed": 0}

    if not rows:
        return {"closed": 0, "tracking": tracking}

    rs = [float(r["r_multiple"] or 0) for r in rows]
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x <= 0]
    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    by_grade = {}
    by_outcome = {}
    for r in rows:
        g = r["grade"] or "?"
        d = by_grade.setdefault(g, {"n": 0, "wins": 0, "r": 0.0})
        d["n"] += 1
        d["r"] += float(r["r_multiple"] or 0)
        if float(r["r_multiple"] or 0) > 0:
            d["wins"] += 1
        by_outcome[r["outcome"]] = by_outcome.get(r["outcome"], 0) + 1

    return {
        "closed": len(rows),
        "tracking": tracking,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": len(wins) / len(rows) * 100,
        "expectancy_r": sum(rs) / len(rows),
        "total_r": sum(rs),
        "avg_win_r": (gross_win / len(wins)) if wins else 0.0,
        "avg_loss_r": (-gross_loss / len(losses)) if losses else 0.0,
        "profit_factor": (gross_win / gross_loss) if gross_loss else float("inf"),
        "by_grade": by_grade,
        "by_outcome": by_outcome,
    }


# ============================================================
# B9 状态快照（供 Web 仪表盘读取，避免仪表盘重复消耗 API 配额）
# ============================================================
def _write_status(cfg: dict, results: dict):
    """把各周期最新状态写入 JSON 快照"""
    try:
        snap = {
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "ticker": cfg.get("ticker_td", cfg.get("ticker")),
            "timeframes": {},
        }
        for tf, (sig, last) in results.items():
            snap["timeframes"][tf] = {
                "signal": int(sig),
                "bar_time": last.name.strftime("%Y-%m-%d %H:%M"),
                "close": round(float(last["Close"]), 2),
                "rsi": round(float(last["rsi"]), 1),
                "atr": round(float(last["atr"]), 2),
                "trend": "多" if last["is_uptrend"] else "空" if last["is_downtrend"] else "震荡",
                "grade": last.get("signal_grade", ""),
                "score": int(last.get("signal_score", 0) or 0),
                "type": last.get("signal_type", ""),
                "band": last.get("signal_band", ""),
                "vol_ok": bool(last.get("vol_pass", True)),
                "adx": round(float(last.get("adx", 0) or 0), 1),
                "plus_di": round(float(last.get("plus_di", 0) or 0), 1),
                "minus_di": round(float(last.get("minus_di", 0) or 0), 1),
                "fib_zone": [
                    round(float(z), 2) for z in (_fib_zone(last, bool(last["is_uptrend"]), last["atr"]) or ())
                ],
            }
        # 数据发布窗口状态（供仪表盘展示；news_blackout 定义在本文件后面，
        # 运行时才调用，前向引用没问题）
        try:
            blocked, ev_name, upcoming = news_blackout(cfg)
            snap["news"] = {"blocked": bool(blocked), "event": ev_name,
                            "upcoming": upcoming, "enabled": bool(cfg.get("news_filter_enabled", True))}
        except Exception as e:
            snap["news"] = {"blocked": False, "event": "", "upcoming": "",
                            "enabled": True, "error": str(e)}
        p = _state_path(cfg.get("status_file", "dpb_status.json"))
        with open(p, "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2, ensure_ascii=False)
    except Exception as e:
        log.error(f"[状态快照] 写入失败: {e}")


# ============================================================
# 主循环
# ============================================================
# 各周期行情数据的刷新间隔（分钟）。高周期 K 线变化慢，
# 每 4 分钟重拉一次纯属浪费额度：4h 的 K 线 4 小时才变一次。
# 设为 0 表示每轮都抓（关闭该周期的缓存）。
_FETCH_TTL_DEFAULTS = {
    "5m": 4, "15m": 8, "1h": 15, "4h": 30, "1d": 60,
}
_fetch_cache = {}   # tf -> (抓取时间戳, 已算好信号的 DataFrame)


# ============================================================
# 财经数据发布黑名单（重大数据前后禁止开新仓）
# ============================================================
# 为什么必须有：用户历史上两次爆仓，其中一次正是「06-05 非农插针」
# （$680 → $0）。系统此前完全不认识非农/CPI/FOMC —— 而数据发布瞬间的插针
# 能在一分钟内直接吃掉整个止损距离。
#
# 「动态」而不是把日期写死在代码里：每次自动拉取 ForexFactory 周历
# （免费、无需 key），按 impact 等级 + 币种过滤后算出禁开仓窗口。
# 源站故障时退回落盘缓存；两者都没有则放行（fail-open，宁可漏过也不误封全天）。
NEWS_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
NEWS_CACHE_FILE = Path(__file__).parent / "news_calendar.json"
NEWS_TTL_SEC = 6 * 3600          # 6 小时刷新一次（周历每周更新）

_news_cache = {"t": 0.0, "events": [], "err": "", "notified": ""}


def _parse_news(raw, cfg: dict) -> list:
    """过滤并解析日历：只保留关注币种 + 达到 impact 等级的事件"""
    levels = {str(x).strip().lower() for x in (cfg.get("news_impact_levels") or ["high"])}
    curr = {str(x).strip().upper() for x in (cfg.get("news_currencies") or ["USD"])}
    out = []
    for e in raw or []:
        try:
            if str(e.get("impact", "")).strip().lower() not in levels:
                continue
            if str(e.get("country", "")).strip().upper() not in curr:
                continue
            dt = datetime.fromisoformat(str(e["date"]))
            out.append({
                "title": e.get("title", ""), "country": e.get("country", ""),
                "impact": e.get("impact", ""),
                "dt": dt.isoformat(), "ts": dt.timestamp(),
            })
        except Exception:
            continue
    return sorted(out, key=lambda x: x["ts"])


def news_events(cfg: dict) -> list:
    """取财经日历事件。远程 → 落盘缓存 → 空列表（fail-open）"""
    now = time.time()
    if _news_cache["events"] and (now - _news_cache["t"]) < NEWS_TTL_SEC:
        return _news_cache["events"]

    raw = None
    try:
        r = _session.get(NEWS_URL, timeout=15)
        if r.status_code == 200:
            raw = r.json()
        else:
            _news_cache["err"] = f"HTTP {r.status_code}"
    except Exception as e:
        _news_cache["err"] = str(e)

    if raw is not None:
        evs = _parse_news(raw, cfg)
        _news_cache.update({"t": now, "events": evs, "err": ""})
        try:      # 落盘：源站故障时仍能保护
            NEWS_CACHE_FILE.write_text(
                json.dumps({"fetched_at": now, "events": evs}, ensure_ascii=False),
                encoding="utf-8")
        except Exception:
            pass
        log.info(f"[新闻] 财经日历已更新: {len(evs)} 个待规避事件")
        return evs

    try:          # 远程失败 → 落盘缓存
        d = json.loads(NEWS_CACHE_FILE.read_text(encoding="utf-8"))
        evs = d.get("events") or []
        if evs:
            _news_cache.update({"t": now, "events": evs})
            log.warning(f"[新闻] 远程日历取数失败({_news_cache['err']})，"
                        f"使用落盘缓存 {len(evs)} 个事件")
            return evs
    except Exception:
        pass

    _news_cache.update({"t": now, "events": []})
    log.warning(f"[新闻] 财经日历不可用({_news_cache['err']})，本次不做数据窗口过滤")
    _notify_news_down(cfg)
    return []


def _notify_news_down(cfg: dict):
    """日历取不到时告警一次（每天一次），避免保护静默失效无人知晓"""
    today = datetime.now().strftime("%Y-%m-%d")
    if _news_cache.get("notified") == today:
        return
    _news_cache["notified"] = today
    try:
        send_alert(cfg, "⚠️ 财经日历不可用",
                   f"> 数据发布黑名单**暂时失效**\n"
                   f"> 原因: {_news_cache['err']}\n"
                   f"> 影响: 非农/CPI/FOMC 等窗口内仍会正常推送信号\n"
                   f"> 时间: {datetime.now():%Y-%m-%d %H:%M:%S}\n")
    except Exception:
        pass


def news_blackout(cfg: dict):
    """当前是否处于数据发布禁开仓窗口 → (是否禁用, 事件名, 下一事件描述)

    窗口 = 事件时间 ± news_blackout_before/after_min（默认 30/30 分钟）。
    另支持 news_manual_blackouts 手工加一次性窗口（可配置）。
    """
    if not cfg.get("news_filter_enabled", True):
        return False, "", ""
    now_ts = time.time()
    before = int(cfg.get("news_blackout_before_min", 30)) * 60
    after = int(cfg.get("news_blackout_after_min", 30)) * 60

    # 手工一次性窗口（可配置）：[{"name":"非农","start":"2026-10-02 20:30","end":"2026-10-02 21:30"}]
    for m in (cfg.get("news_manual_blackouts") or []):
        try:
            s = datetime.strptime(str(m["start"]), "%Y-%m-%d %H:%M").timestamp()
            e = datetime.strptime(str(m["end"]), "%Y-%m-%d %H:%M").timestamp()
            if s <= now_ts <= e:
                return True, m.get("name", "手工窗口"), ""
        except Exception:
            continue

    evs = news_events(cfg)
    if not evs:
        return False, "", ""

    upcoming = ""
    for ev in evs:
        ts = float(ev["ts"])
        if ts + after < now_ts:
            continue                      # 已过去
        if not upcoming:
            mins = (ts - now_ts) / 60
            when = "刚刚" if mins < 0 else f"{mins:.0f}分钟后"
            upcoming = f"{ev['title']} ({ev['country']}) {when}"
        if ts - before <= now_ts <= ts + after:
            return True, f"{ev['title']} ({ev['country']})", upcoming
    return False, "", upcoming


def check_signals(cfg: dict, state: dict, df_cache: dict = None) -> dict:
    """检查所有周期的信号，返回更新后的状态。

    A4 改造：先算完所有周期，再按「同向周期数」判定多周期共振，
    推送时带 🔥 标记，帮助区分"单周期噪音"与"多周期共振"信号。

    df_cache: 传入 dict 时，会把本轮已抓取并计算好的各周期 DataFrame 存进去，
    供「结果追踪」直接复用 —— 否则结果追踪会为每个周期再抓一次数据，
    在有信号追踪时把 API 消耗翻倍（3 key × 800/天 的额度扛不住）。

    额度优化：按 _FETCH_TTL_DEFAULTS（可用 config 的 fetch_ttl_minutes 覆盖）
    对每个周期设置刷新间隔，未到期的直接复用上一轮结果。
    注意 calc_signals 是纯函数、每次都从完整 K 线序列重算，
    所以复用不会丢状态机进度，只影响「新信号被发现的延迟」。
    """
    ticker = cfg.get("ticker_td", cfg.get("ticker_av", cfg["ticker"]))
    min_grade = cfg.get("min_signal_grade", "C")
    resonance_min = cfg.get("resonance_min_count", 2)

    # ---- 阶段 1：抓取并计算所有周期（按 TTL 决定是否复用缓存）----
    ttl_map = cfg.get("fetch_ttl_minutes") or _FETCH_TTL_DEFAULTS
    now = time.time()
    results = {}  # tf -> (sig, last_row)
    reused_tfs, fetched_tfs = [], []
    for tf, params in cfg["timeframes"].items():
        try:
            ttl = float(ttl_map.get(tf, _FETCH_TTL_DEFAULTS.get(tf, 4)) or 0) * 60
            hit = _fetch_cache.get(tf)
            if ttl > 0 and hit and (now - hit[0]) < ttl:
                df = hit[1]                       # 复用，零 API 消耗
                reused_tfs.append(tf)
            else:
                df = calc_signals(fetch_data(ticker, tf, params["interval"]), cfg)
                _fetch_cache[tf] = (now, df)
                fetched_tfs.append(tf)
            last = df.iloc[-1]
            results[tf] = (int(last["signal"]), last)
            if df_cache is not None:
                df_cache[tf] = df      # 复用给结果追踪，避免二次抓取
        except Exception as e:
            log.error(f"[错误] {tf} 检查失败: {e}")
    log.info(f"[数据] 本轮抓取 {fetched_tfs or '无'} | 复用缓存 {reused_tfs or '无'}")

    # ---- 阶段 2：统计各方向信号所属周期（用于共振判定）----
    long_tfs = [tf for tf, (s, _) in results.items() if s == 1]
    short_tfs = [tf for tf, (s, _) in results.items() if s == -1]
    if long_tfs or short_tfs:
        log.info(f"[共振] 同向周期 — 多: {long_tfs or '无'} | 空: {short_tfs or '无'}")

    # B9 写状态快照（供 Web 仪表盘读取，零 API 消耗）
    _write_status(cfg, results)

    # ---- 阶段 3：逐周期推送 ----
    for tf, (sig, last) in results.items():
        try:
            if sig == 0:
                continue

            # A5 信号等级门槛：低于 min_signal_grade 的信号不推送（治理信号过频）
            grade_now = last.get("signal_grade", "")
            if _GRADE_ORDER.get(grade_now, 0) < _GRADE_ORDER.get(min_grade, 0):
                log.info(f"[过滤] {tf} {grade_now}级 低于门槛 {min_grade} → 跳过推送")
                continue

            # ADX 硬门槛（可选，min_adx_filter>0 时生效）：震荡行情直接不推
            adx_gate = float(cfg.get("min_adx_filter", 0) or 0)
            if adx_gate > 0 and float(last.get("adx", 0) or 0) < adx_gate:
                log.info(f"[过滤] {tf} ADX {float(last.get('adx', 0) or 0):.1f} < {adx_gate} (震荡) → 跳过推送")
                continue

            # 去重：同一周期同一方向不重复推送
            key = f"{tf}_{sig}"
            if state.get(key) == last.name.isoformat():
                log.info(f"[信号] {tf} 已有推送，跳过")
                continue

            # DB 级去重（第二道防线）：状态文件丢失/损坏时也能挡住重复推送
            if _already_pushed(cfg.get("db_file", "signals.db"), tf, last.name.isoformat(), sig):
                state[key] = last.name.isoformat()
                log.info(f"[信号] {tf} 同K线已在库中(DB去重) → 跳过推送")
                continue

            # 共振判定：同向周期数 >= 阈值
            same_tfs = long_tfs if sig > 0 else short_tfs
            res_tfs = same_tfs if len(same_tfs) >= resonance_min else None

            # 做单策略：可选「只做多周期共振」硬门槛（默认关闭，
            # 打开后单周期孤立信号不再推送，显著降低噪音）
            if cfg.get("require_resonance", False) and not res_tfs:
                log.info(f"[过滤] {tf} 无多周期共振(需≥{resonance_min}个同向) → 跳过推送")
                continue

            # 数据发布黑名单：重大数据前后不开新仓
            # （用户历史爆仓之一正是 06-05 非农插针 $680→$0）
            blocked, ev_name, _upcoming = news_blackout(cfg)
            if blocked:
                state[key] = last.name.isoformat()
                log.info(f"[新闻] ⛔ 处于数据发布窗口「{ev_name}」→ 不推送 "
                         f"{tf} {'多' if sig > 0 else '空'}单")
                continue

            title, content = format_signal_msg(tf, sig, last, cfg, resonance_tfs=res_tfs)
            send_alert(cfg, title, content)
            state[key] = last.name.isoformat()
            log.info(f"[信号] {title} @ {last['Close']:.2f}")

            # 记录到信号专用日志（详细版）
            direction = "买入" if sig > 0 else "卖出"
            sig_type = last.get("signal_type", "")
            type_tag = f"[{sig_type}]" if sig_type else ""
            grade = last.get("signal_grade", "")
            score = last.get("signal_score", 0)
            grade_tag = f"[{grade}级{score}/{_SCORE_MAX}]" if grade else ""
            band = last.get("signal_band", "")
            band_tag = f"({band})" if band else ""
            res_tag = f" 🔥共振{len(res_tfs)}" if res_tfs else ""

            # 计算止损和TP（统一走 calc_sl_tp，与 format_signal_msg 同源）
            entry, sl, r_size, r1, r2, tp1, tp2 = calc_sl_tp(sig, last, cfg, tf)
            atr = last["atr"]

            # 做单策略：仓位管理（与推送内容同源，避免两处算法漂移）
            ps = calc_position_size(entry, sl, cfg) if cfg.get("use_position_sizing", True) else None
            lot_tag = (
                f" | 手数:{ps['lots']:.2f}(风险${ps['actual_risk']:.2f})"
                if ps and ps.get("ok") else ""
            )

            vol_status = "量✓" if last.get("vol_pass", True) else "量✗"
            trend_status = "多" if last["is_uptrend"] else "空" if last["is_downtrend"] else "震荡"

            signal_log.info(
                f"{tf} {direction}{type_tag}{grade_tag}{band_tag}{res_tag} | "
                f"入:{entry:.2f} | SL:{sl:.2f}(R{r_size:.2f}) | "
                f"TP1:{tp1:.2f}({r1}R) TP2:{tp2:.2f}({r2}R){lot_tag} | "
                f"RSI:{last['rsi']:.1f} ATR:{atr:.2f} {vol_status} | "
                f"趋势:{trend_status}"
            )

            # B8 持久化到 SQLite（便于后续复盘/统计胜率）
            save_signal_db(
                cfg.get("db_file", "signals.db"), tf, sig, last,
                entry, sl, tp1, tp2, r_size,
                len(res_tfs) if res_tfs else 0,
            )
        except Exception as e:
            log.error(f"[错误] {tf} 推送失败: {e}")

    return state


def main():
    parser = argparse.ArgumentParser(description="DPB 二次回踩信号监控")
    parser.add_argument("--loop", action="store_true", help="循环监控模式")
    args = parser.parse_args()
    
    cfg = load_config()
    state = load_state(cfg["state_file"])
    init_db(cfg.get("db_file", "signals.db"))   # B8 初始化信号库（幂等）

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
                # 同一轮内共享已抓取的数据，避免结果追踪重复消耗 API 额度
                df_cache = {}
                state = check_signals(cfg, state, df_cache)
                save_state(cfg["state_file"], state)
                # 结果追踪：评估已推送信号的实际结果（验证闭环的地基，复用上面的数据 → 零额外消耗）
                evaluate_outcomes(cfg, df_cache=df_cache)
                st = outcome_stats(cfg.get("db_file", "signals.db"))
                if st.get("closed"):
                    log.info(
                        f"[胜率] 已结案{st['closed']}笔 | 胜率{st['win_rate']:.1f}% | "
                        f"期望{st['expectancy_r']:+.2f}R | 盈亏因子{st['profit_factor']:.2f} | "
                        f"累计{st['total_r']:+.2f}R"
                    )
            except Exception as e:
                log.error(f"[错误] 循环检查异常: {e}")
            log.info(f"[等待] {interval//60} 分钟后下次检查...")
            time.sleep(interval)
    else:
        log.info("[启动] 单次检查模式")
        state = check_signals(cfg, state)
        save_state(cfg["state_file"], state)
        evaluate_outcomes(cfg)
        st = outcome_stats(cfg.get("db_file", "signals.db"))
        if st.get("closed"):
            log.info(
                f"[胜率] 已结案{st['closed']}笔 | 胜率{st['win_rate']:.1f}% | "
                f"期望{st['expectancy_r']:+.2f}R | 盈亏因子{st['profit_factor']:.2f}"
            )
        log.info("[完成] 检查结束")


if __name__ == "__main__":
    main()
