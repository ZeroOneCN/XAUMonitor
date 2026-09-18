#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Binance PAXG/USDT 实时行情旁路采集器

用途
----
独立于现有监控（DPB 信号系统）运行的「旁路」采集进程，只负责采集，
不参与任何信号计算，不影响也不依赖 dpb_monitor.py。

目标：持续采集 Binance PAXG/USDT（1 盎司实物黄金背书代币，与 XAU/USD
实测价差约 0.03%）的实时成交与 1 分钟 K 线，用于：

  1. 与 Twelve Data 轮询数据做对比，评估是否值得切换数据源；
  2. 为「盘中价格触发」类逻辑做数据准备。

数据落库：SQLite（独立库 paxg_stream.db，WAL 模式，可并发读）

用法
----
  python paxg_collector.py                 # 常驻采集
  python paxg_collector.py --symbol paxgusdt
  python paxg_collector.py --db /path/to.db
"""
import argparse
import asyncio
import json
import logging
import signal
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import websockets

# ============================================================
# 配置
# ============================================================
BASE = Path(__file__).parent
DEFAULT_SYMBOL = "paxgusdt"                 # Binance 交易对（小写）
DEFAULT_DB = BASE / "paxg_stream.db"
WS_BASE = "wss://stream.binance.com:9443/stream?streams="
CST = timezone(timedelta(hours=8))          # 北京时间

TRADE_FLUSH_ROWS = 200                      # 成交攒够多少行落库
TRADE_FLUSH_SECS = 5                         # 或最多等多少秒
STAT_INTERVAL = 60                           # 统计日志间隔（秒）
PING_INTERVAL = 20                           # 心跳（Binance 20 分钟发 ping，这里主动保活）
PING_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(BASE / "paxg_collector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("PAXG")

# ============================================================
# 数据库
# ============================================================
def init_db(db_path: Path):
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_ms INTEGER,          -- 交易所成交时间(ms)
                recv_ms INTEGER,        -- 本地接收时间(ms)
                latency_ms INTEGER,     -- 本地接收 - 交易所时间
                price REAL, qty REAL,
                is_buyer_maker INTEGER  -- 1=主动卖(买方是挂单), 0=主动买
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(ts_ms)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS klines_1m (
                open_ms INTEGER PRIMARY KEY,
                close_ms INTEGER,
                open REAL, high REAL, low REAL, close REAL,
                volume REAL, quote_volume REAL,
                trades INTEGER,
                closed INTEGER,         -- 1=该分钟已收盘
                updated_ms INTEGER
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS stream_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT, kind TEXT, detail TEXT
            )
        """)
        conn.commit()


def log_event(db_path: Path, kind: str, detail: str):
    try:
        with sqlite3.connect(db_path) as conn:
            conn.execute(
                "INSERT INTO stream_events (ts, kind, detail) VALUES (?,?,?)",
                (datetime.now(CST).strftime("%Y-%m-%d %H:%M:%S"), kind, detail),
            )
            conn.commit()
    except Exception as e:
        log.error(f"[事件] 写入失败: {e}")


# ============================================================
# 采集
# ============================================================
class Collector:
    def __init__(self, db_path: Path, symbol: str):
        self.db = db_path
        self.symbol = symbol
        self.trade_buf = []
        self.last_flush = time.time()
        # 统计
        self.stat_ticks = 0
        self.stat_lat_sum = 0
        self.stat_lat_n = 0
        self.stat_klines = 0
        self.stat_start = time.time()
        self.last_price = None
        self.running = True

    # ---------- 落库 ----------
    def flush(self, force: bool = False):
        now = time.time()
        if not force and len(self.trade_buf) < TRADE_FLUSH_ROWS and (now - self.last_flush) < TRADE_FLUSH_SECS:
            return
        if not self.trade_buf:
            self.last_flush = now
            return
        rows, self.trade_buf = self.trade_buf, []
        try:
            with sqlite3.connect(self.db) as conn:
                conn.executemany(
                    "INSERT INTO trades (ts_ms, recv_ms, latency_ms, price, qty, is_buyer_maker)"
                    " VALUES (?,?,?,?,?,?)",
                    rows,
                )
                conn.commit()
        except Exception as e:
            log.error(f"[落库] 成交写入失败({len(rows)}行): {e}")
        self.last_flush = now

    def save_kline(self, k: dict):
        try:
            with sqlite3.connect(self.db) as conn:
                conn.execute(
                    "INSERT INTO klines_1m (open_ms, close_ms, open, high, low, close, volume,"
                    " quote_volume, trades, closed, updated_ms)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?,?)"
                    " ON CONFLICT(open_ms) DO UPDATE SET"
                    " high=excluded.high, low=excluded.low, close=excluded.close,"
                    " volume=excluded.volume, quote_volume=excluded.quote_volume,"
                    " trades=excluded.trades, closed=excluded.closed, updated_ms=excluded.updated_ms",
                    (
                        k["t"], k["T"],
                        float(k["o"]), float(k["h"]), float(k["l"]), float(k["c"]),
                        float(k["v"]), float(k.get("q", 0) or 0),
                        int(k.get("n", 0)), 1 if k.get("x") else 0,
                        int(time.time() * 1000),
                    ),
                )
                conn.commit()
            self.stat_klines += 1
        except Exception as e:
            log.error(f"[落库] K线写入失败: {e}")

    # ---------- 消息处理 ----------
    def on_message(self, raw: str):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        stream = msg.get("stream", "")
        data = msg.get("data", {})
        recv_ms = int(time.time() * 1000)

        if stream.endswith("@trade"):
            ts = int(data.get("T", 0))
            price = float(data.get("p", 0))
            self.trade_buf.append((
                ts, recv_ms, recv_ms - ts, price,
                float(data.get("q", 0)), 1 if data.get("m") else 0,
            ))
            self.stat_ticks += 1
            self.stat_lat_sum += recv_ms - ts
            self.stat_lat_n += 1
            self.last_price = price
            self.flush()

        elif stream.endswith("@kline_1m"):
            k = data.get("k", {})
            if k:
                self.save_kline(k)
                self.last_price = float(k.get("c", 0)) or self.last_price

    # ---------- 统计日志 ----------
    def log_stats(self):
        elapsed = time.time() - self.stat_start
        tpm = self.stat_ticks * 60 / elapsed if elapsed else 0
        avg_lat = (self.stat_lat_sum / self.stat_lat_n) if self.stat_lat_n else 0
        log.info(
            f"[统计] 近{elapsed:.0f}s: 成交{self.stat_ticks}笔({tpm:.0f}/分) "
            f"K线更新{self.stat_klines}次 平均延迟{avg_lat:.0f}ms 最新价={self.last_price}"
        )
        self.stat_ticks = self.stat_lat_sum = self.stat_lat_n = self.stat_klines = 0
        self.stat_start = time.time()

    # ---------- 主循环 ----------
    async def run(self):
        streams = f"{self.symbol}@trade/{self.symbol}@kline_1m"
        url = WS_BASE + streams
        backoff = 1
        last_stat = time.time()
        log.info(f"[启动] 旁路采集器 | 交易对={self.symbol.upper()} | 库={self.db}")
        log.info(f"[启动] 订阅: {streams}")

        while self.running:
            try:
                async with websockets.connect(
                    url, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT, open_timeout=15
                ) as ws:
                    log.info("[连接] Binance WS 已连接")
                    log_event(self.db, "connect", url)
                    backoff = 1
                    async for raw in ws:
                        if not self.running:
                            break
                        self.on_message(raw)
                        now = time.time()
                        if now - last_stat >= STAT_INTERVAL:
                            self.flush(force=True)
                            self.log_stats()
                            last_stat = now
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"[连接] 异常: {type(e).__name__}: {e} → {backoff}s 后重连")
                log_event(self.db, "disconnect", f"{type(e).__name__}: {e}")
                self.flush(force=True)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

        self.flush(force=True)
        log.info("[停止] 采集器已退出")

    def stop(self):
        self.running = False


def main():
    ap = argparse.ArgumentParser(description="Binance PAXG/USDT 旁路行情采集器")
    ap.add_argument("--symbol", default=DEFAULT_SYMBOL, help="Binance 交易对(小写), 默认 paxgusdt")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 库路径")
    args = ap.parse_args()

    db_path = Path(args.db)
    init_db(db_path)

    c = Collector(db_path, args.symbol)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, c.stop)
        except NotImplementedError:
            pass

    try:
        loop.run_until_complete(c.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
