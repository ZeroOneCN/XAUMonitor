#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""XAUMonitor 看门狗 —— 检测「进程还活着，但内部已经静默卡死」。

为什么必须独立成进程
--------------------
监控自己卡死时，它既无法自救、也无法告警。tg-monitor 就是活生生的例子：
进程在、systemd 显示已在线 18 小时、Telegram TCP 也是 ESTABLISHED，
但内部循环已经 3.5 小时零输出，没有任何告警，用户白白丢数据。

本脚本由 systemd timer 每 5 分钟拉起一次（跑完就退出，不常驻），检查：
  1. 心跳新鲜度   —— dpb_heartbeat.txt，监控主循环每轮都刷新
  2. 服务存活     —— xaumonitor / xaumonitor-web / paxg-collector
  3. 旁路采集     —— paxg_stream.db 里最新 tick 距今多久（WS 断了会立刻暴露）
  4. 额度耗尽风险 —— 当日 API 用量是否逼近上限

发现问题 → 推送告警（同一问题 1 小时只报 1 次，避免刷屏）
恢复正常 → 推送恢复通知
可选自动重启卡死的服务（watchdog_auto_restart，默认开）
"""
import json
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
import dpb_monitor as m          # noqa: E402  （共用配置与推送通道）

BASE = Path(__file__).parent
STATE_FILE = BASE / "watchdog_state.json"

SERVICES = ["xaumonitor", "xaumonitor-web", "paxg-collector"]
ALERT_COOLDOWN_SEC = 3600        # 同一问题 1 小时只报一次
PAXG_STALE_MIN = 5               # 旁路 tick 超过该分钟数视为断流


def _sys(cmd: list) -> str:
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=20)
        return (r.stdout or "").strip()
    except Exception as e:
        return f"<执行失败: {e}>"


def _load_state() -> dict:
    try:
        return json.loads(STATE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {"alerted": {}}


def _save_state(st: dict):
    try:
        st["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        STATE_FILE.write_text(json.dumps(st, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[看门狗] 状态落盘失败: {e}")


def check_heartbeat(cfg: dict):
    """→ (是否正常, 说明)"""
    p = m._state_path(cfg.get("heartbeat_file", "dpb_heartbeat.txt"))
    if not p.exists():
        return False, f"心跳文件不存在（{p.name}）——监控可能从未成功跑完一轮"
    age = time.time() - p.stat().st_mtime
    interval_min = int(cfg.get("check_interval_minutes", 4))
    limit = max(15, interval_min * 3) * 60      # 3 个周期，最少 15 分钟
    txt = ""
    try:
        txt = p.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    if age > limit:
        return False, (f"心跳已 {age / 60:.0f} 分钟未更新"
                       f"（阈值 {limit / 60:.0f} 分钟）| 最后: {txt}")
    return True, f"心跳正常（{age / 60:.1f} 分钟前 | {txt}）"


def check_services():
    bad = []
    for s in SERVICES:
        st = _sys(["systemctl", "is-active", s])
        if st != "active":
            bad.append(f"{s}={st or '未知'}")
    return (not bad), ("全部 active" if not bad else "异常: " + ", ".join(bad))


def check_paxg(db_path: Path):
    if not db_path.exists():
        return False, "paxg_stream.db 不存在"
    try:
        with sqlite3.connect(str(db_path)) as c:
            row = c.execute("SELECT MAX(ts_ms) FROM trades").fetchone()
        if not row or not row[0]:
            return False, "paxg_stream.db 里没有任何 tick"
        ts = float(row[0]) / 1000.0
        age_min = (time.time() - ts) / 60
        if age_min > PAXG_STALE_MIN:
            return False, f"旁路采集已断流 {age_min:.0f} 分钟（最后 tick {datetime.fromtimestamp(ts):%H:%M:%S}）"
        return True, f"采集正常（最新 tick {age_min:.1f} 分钟前）"
    except Exception as e:
        return False, f"读取 paxg_stream.db 失败: {e}"


def check_quota(cfg: dict):
    """当日 API 用量（从监控日志的额度统计里读不到就跳过，仅作参考）"""
    try:
        usage = m._quota_usage_today(cfg)          # 若不存在会被 except 兜住
        if not usage:
            return True, ""
        used, limit = usage
        if limit and used / limit >= 0.9:
            return False, f"API 额度已用 {used}/{limit}（{used / limit * 100:.0f}%）"
        return True, f"额度 {used}/{limit}"
    except Exception:
        return True, ""


def main():
    cfg = m.load_config()
    stt = _load_state()
    alerted = stt.setdefault("alerted", {})
    now = time.time()

    problems, oks = [], []

    ok, msg = check_heartbeat(cfg)
    (oks if ok else problems).append(("心跳", msg))
    ok, msg = check_services()
    (oks if ok else problems).append(("服务", msg))
    ok, msg = check_paxg(BASE / "paxg_stream.db")
    (oks if ok else problems).append(("旁路采集", msg))
    ok, msg = check_quota(cfg)
    if msg:
        (oks if ok else problems).append(("API额度", msg))

    for title, msg in oks:
        print(f"[OK]   {title}: {msg}")
    for title, msg in problems:
        print(f"[FAIL] {title}: {msg}")

    # ---- 告警（同一问题 1 小时只报一次）----
    if problems:
        fresh = []
        for title, msg in problems:
            last = float(alerted.get(title, 0) or 0)
            if now - last >= ALERT_COOLDOWN_SEC:
                alerted[title] = now
                fresh.append((title, msg))
        if fresh:
            body = "".join(f"> **{t}**: {msg}\n" for t, msg in fresh)
            m.send_alert(
                cfg,
                f"🚨 XAUMonitor 异常（{len(problems)}项）",
                f"> 时间: {datetime.now():%Y-%m-%d %H:%M:%S}\n{body}"
                f"> 处置: 看门狗"
                + ("已尝试自动重启\n" if cfg.get("watchdog_auto_restart", True) else "仅告警\n"),
            )
        # 自动重启（默认开）—— 只在服务层异常时重启，避免无脑重启
        if cfg.get("watchdog_auto_restart", True):
            for title, msg in problems:
                if title != "服务":
                    continue
                for s in SERVICES:
                    if s not in msg:
                        continue
                    if _sys(["systemctl", "is-active", s]) != "active":
                        print(f"[看门狗] 自动重启 {s}")
                        _sys(["systemctl", "restart", s])
    else:
        # ---- 恢复通知（之前报过的都清掉）----
        recovered = [k for k in list(alerted.keys())]
        if recovered:
            m.send_alert(cfg, "✅ XAUMonitor 已恢复正常",
                         f"> 时间: {datetime.now():%Y-%m-%d %H:%M:%S}\n"
                         f"> 恢复项: {', '.join(recovered)}\n")
            alerted.clear()

    stt["alerted"] = alerted
    stt["last_check"] = {
        "at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ok": not problems,
        "problems": [f"{t}: {m_}" for t, m_ in problems],
        "checks": [f"{t}: {m_}" for t, m_ in oks],
    }
    _save_state(stt)
    return 0 if not problems else 1


if __name__ == "__main__":
    sys.exit(main())
