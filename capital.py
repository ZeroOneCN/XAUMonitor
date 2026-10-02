#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""入金台账（capital flows）—— 让「权益上涨」和「我赚了钱」不再是同一件事。

存在的理由：
    没有台账时，权益曲线只有一个基线（account_equity 常数）。
    一旦中途入金，曲线会**凭空跳高一截**，被误读成"这笔赚回来了"。
    实测测试里这会造成系统性自我欺骗：亏了再入金，看起来像在盈利。

本模块把资金流动单独记账，于是可以算出：
    真实盈亏 = 当前权益 − 累计入金
这才是能拿来判断策略好坏的数。
"""
from __future__ import annotations

import sqlite3
import os
from datetime import date, datetime

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

KINDS = ("deposit", "withdrawal")   # 入金 / 出金


def _conn(db_file: str | None = None) -> sqlite3.Connection:
    c = sqlite3.connect(db_file or DB)
    c.row_factory = sqlite3.Row
    return c


def init_table(db_file: str | None = None) -> None:
    """建表。幂等，可重复调用。"""
    c = _conn(db_file)
    c.execute("""
        CREATE TABLE IF NOT EXISTS capital_flows (
            id       INTEGER PRIMARY KEY AUTOINCREMENT,
            flow_date TEXT NOT NULL,          -- YYYY-MM-DD
            amount   REAL NOT NULL,           -- 正=入金，负=出金
            kind     TEXT NOT NULL DEFAULT 'deposit',
            note     TEXT,
            created_at TEXT
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_cf_date ON capital_flows(flow_date)")
    c.commit()
    c.close()


def add(flow_date: str, amount: float, kind: str = "deposit",
        note: str = "", db_file: str | None = None) -> int:
    """记一笔资金流动。kind='withdrawal' 时金额可传正数，这里自动转负。"""
    init_table(db_file)
    amt = abs(float(amount))
    if kind == "withdrawal":
        amt = -amt
    c = _conn(db_file)
    cur = c.execute(
        "INSERT INTO capital_flows (flow_date, amount, kind, note, created_at) "
        "VALUES (?,?,?,?,?)",
        (str(flow_date)[:10], amt, kind, note,
         datetime.now().isoformat(timespec="seconds")))
    c.commit()
    rid = cur.lastrowid
    c.close()
    return rid


def all_flows(db_file: str | None = None) -> list:
    init_table(db_file)
    c = _conn(db_file)
    rs = [dict(r) for r in c.execute(
        "SELECT * FROM capital_flows ORDER BY flow_date, id")]
    c.close()
    return rs


def total(db_file: str | None = None) -> float:
    """累计净入金。"""
    return sum(float(r["amount"]) for r in all_flows(db_file))


def cumulative_by_date(db_file: str | None = None) -> dict:
    """每个日期结束时的累计入金。用于画「资金流量调整后」的权益曲线。"""
    out, run = {}, 0.0
    for r in all_flows(db_file):
        run += float(r["amount"])
        out[str(r["flow_date"])[:10]] = run
    return out


def deposits_up_to(day: str, db_file: str | None = None) -> float:
    """截至某日（含）的累计入金。"""
    d = str(day)[:10]
    return sum(float(r["amount"]) for r in all_flows(db_file)
               if str(r["flow_date"])[:10] <= d)


if __name__ == "__main__":
    import sys
    init_table()
    if len(sys.argv) > 1 and sys.argv[1] == "add":
        _, _, d, a = sys.argv[:4]
        note = sys.argv[4] if len(sys.argv) > 4 else ""
        print("已记录 id =", add(d, float(a), note=note))
    print(f"累计净入金: ¥/{total():,.2f}")
    for r in all_flows():
        print(f"  {r['flow_date']}  {r['amount']:>+10,.2f}  {r['kind']:<10} {r['note'] or ''}")
