#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""凭证轮换助手 —— 交互式替换泄露的 Twelve Data key 与 企业微信 webhook。

为什么要这个脚本：泄露事故后必须换掉两个凭证，但密钥【绝不能出现在聊天里】。
在服务器上运行本脚本，输入只进内存和本地文件，不进对话记录。

它会：
  1. 备份现有配置（.bak-时间戳，已被 .gitignore 挡住）
  2. 交互式读取新值（不回显到 shell 历史）
  3. 先验证 key 能真实取到行情、webhook 能真发消息 —— 验证不过就不写入
  4. 写入后提示重启命令（本脚本不自动重启，避免打断正在跑的信号）
"""
import json
import shutil
import sys
import getpass
import time
from pathlib import Path

import requests

CONFIG = Path(__file__).parent / "dpb_config.json"


def mask(s):
    s = str(s)
    return s[:6] + "…" + s[-4:] + f" ({len(s)}字符)" if len(s) > 12 else s


def check_key(k):
    """真实取一次行情，确认 key 有效且额度可用"""
    try:
        r = requests.get("https://api.twelvedata.com/time_series",
                         params={"symbol": "XAU/USD", "interval": "5min",
                                 "outputsize": 1, "timezone": "Asia/Shanghai",
                                 "apikey": k}, timeout=25)
        d = r.json()
        if d.get("code") or d.get("status") == "error":
            return False, str(d.get("message", d))[:140]
        v = d.get("values") or []
        if not v:
            return False, f"无数据返回: {str(d)[:120]}"
        return True, f"取到最新K线 {v[0]['datetime']} 收 {v[0]['close']}"
    except Exception as e:
        return False, f"请求异常: {e}"


def check_webhook(url):
    """真发一条测试消息，确认 webhook 可用"""
    try:
        r = requests.post(url, json={
            "msgtype": "text",
            "text": {"content": "✅ 凭证轮换验证：新的企业微信 webhook 已生效。"}
        }, timeout=20)
        d = r.json() if r.content else {}
        if d.get("errcode", 0) == 0:
            return True, "测试消息已发送成功"
        return False, f"接口返回: {str(d)[:140]}"
    except Exception as e:
        return False, f"请求异常: {e}"


def main():
    print("=" * 62)
    print("凭证轮换助手（泄露事故后必做）")
    print("=" * 62)
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    print(f"  当前 key:     {mask(cfg.get('twelve_data_key', ''))}")
    print(f"  当前 webhook: {mask(cfg.get('wecom_webhook', ''))}")
    print()

    print("【1/2】新的 Twelve Data API Key")
    print("      （登录 twelvedata.com → API Keys 生成；直接回车 = 跳过不改）")
    try:
        new_key = getpass.getpass("      粘贴新 key（输入不回显）: ").strip()
    except Exception:
        new_key = input("      粘贴新 key: ").strip()

    new_wh = ""
    if new_key:
        ok, msg = check_key(new_key)
        print(f"      验证: {'✅' if ok else '❌'} {msg}")
        if not ok:
            print("\n  ❌ 新 key 验证失败，未做任何修改。请确认 key 正确后重跑。")
            return 1
        if new_key == cfg.get("twelve_data_key"):
            print("\n  ⚠️ 这与当前 key 完全相同 —— 未真正轮换。未做修改。")
            return 1

    print()
    print("【2/2】新的 企业微信 webhook")
    print("      （群机器人 → 设置 → 移除旧机器人 → 新建；直接回车 = 跳过不改）")
    try:
        new_wh = getpass.getpass("      粘贴新 webhook（输入不回显）: ").strip()
    except Exception:
        new_wh = input("      粘贴新 webhook: ").strip()
    if new_wh:
        if not new_wh.startswith("http"):
            print("      ❌ 格式不对（应以 http 开头），未做修改。")
            return 1
        ok, msg = check_webhook(new_wh)
        print(f"      验证: {'✅' if ok else '❌'} {msg}")
        if not ok:
            print("\n  ❌ 新 webhook 验证失败，未做任何修改。")
            return 1
        if new_wh == (cfg.get("webhook") or cfg.get("wecom_webhook")):
            print("\n  ⚠️ 这与当前 webhook 完全相同 —— 未真正轮换。未做修改。")
            return 1

    if not new_key and not new_wh:
        print("\n  两个都没输入，未做任何修改。")
        return 1

    # 备份（.bak-时间戳 已被 .gitignore 的 *.bak.* 规则挡住）
    bak = CONFIG.with_suffix(f".json.bak-{time.strftime('%Y%m%d-%H%M%S')}")
    shutil.copy2(CONFIG, bak)
    print(f"\n  已备份: {bak.name}")

    if new_key:
        cfg["twelve_data_key"] = new_key
    if new_wh:
        for f in ("wecom_webhook", "webhook"):
            if f in cfg:
                cfg[f] = new_wh
        if "wecom_webhook" not in cfg and "webhook" not in cfg:
            cfg["wecom_webhook"] = new_wh

    tmp = CONFIG.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(CONFIG)
    print(f"  已写入: {CONFIG}")
    print(f"  新 key:     {mask(cfg.get('twelve_data_key', ''))}")
    print(f"  新 webhook: {mask(cfg.get('wecom_webhook') or cfg.get('webhook') or '')}")
    print()
    print("=" * 62)
    print("下一步：重启服务让新凭证生效")
    print("  systemctl restart xaumonitor paxg-collector")
    print("=" * 62)
    return 0


if __name__ == "__main__":
    sys.exit(main())
