# XAUMonitor 代码审查与优化报告

> 审查日期：2026-09-18 · 审查对象：`dpb_monitor.py`（DPB 二次回踩黄金信号监控）

---

## 一、致命缺陷（已修复）

### 1. 配置文件 JSON 缺失开括号 `{`
- **现象**：`dpb_config.json` 首行是空行，缺少对象开头的 `{`，导致 `json.load()` 抛 `Extra data: line 2 column 18`，程序**无法启动**。
- **影响**：这是部署环境本地配置文件（已 gitignore，不入库）损坏，示例文件 `dpb_config.example.json` 正常。
- **修复**：为本地配置补上 `{` 并校验 JSON 合法。

### 2. 时区错误：信号时间错 10 小时
- **现象**：Twelve Data 的 `time_series` 接口对 `XAU/USD` **默认返回 UTC+10**（不是 UTC）。原代码 `pd.to_datetime(df["Date"])` 未做时区处理，推送的 `strftime('%H:%M')` 时间会偏 10 小时。
- **修复**：在 API 请求中显式传入 `timezone=Asia/Shanghai`（`cfg.get("timezone", TZ_NAME)`），数据落地即为北京时间，无需二次转换。
- **验证**：修复前最新K线 `10:25:00`，修复后 `00:30:00`（与当前北京时间一致）。

---

## 二、健壮性与性能优化（已实施）

| # | 问题 | 修复 |
|---|------|------|
| 1 | `load_config()` 每次请求/节流都重新读盘+解析 JSON | 增加基于 `mtime` 的配置缓存 |
| 2 | `send_wecom()` 未做异常防护，推送失败会拖垮整轮检查 | `try/except` + `raise_for_status()`，失败只记日志不中断 |
| 3 | `send_wecom()` 每次 `requests.post` 新建连接 | 复用模块级 `_session`（已有连接池） |
| 4 | SL/TP 计算逻辑在 `format_signal_msg` 与 `check_signals` 中重复 40+ 行 | 抽取统一函数 `calc_sl_tp()`，单一来源避免两处漂移 |
| 5 | `load_state()`/`save_state()` 无异常处理，状态文件损坏即崩溃 | 包裹 `try/except`，损坏时重置为空字典 |
| 6 | `load_config()` 默认字典硬编码 webhook 占位 | 统一为 `YOUR_KEY_HERE` 占位符 |

---

## 三、遗留问题与优化建议（未改动，供后续决策）

### 1. 「结构过滤」逻辑未实现（策略一致性风险）
- **位置**：`calc_signals()` 中信号评分 `score += 1  # 假设结构过滤通过（Python未实现）`（做多/做空各一处）。
- **说明**：Pine Script 中该评分项对应结构过滤，Python 版本直接假设通过，导致**信号等级被固定高估 1 分**。
- **建议**：若需与 Pine 完全对齐，需补齐结构过滤逻辑；否则应在评分说明中明确标注该偏差。

### 2. 状态机为多重 Python 循环（性能）
- `calc_signals()` 内有 4 段 `for i in range(len(df))` 循环（破位计数、连续K线计数、趋势冷却、主状态机），1000 根K线 × 5 周期约为 2 万次迭代。
- **影响**：当前可接受（每 4 分钟一轮，秒级完成）；若未来提高轮询频率或增加币种，建议用 `numpy`/`numba` 向量化。

### 3. 信号去重仅按「周期+方向」
- 去重 key 为 `f"{tf}_{sig}"`，若同一周期内先出「回踩」再出「突破」同方向信号，后者会被去重跳过。
- **建议**：若希望两种信号类型都推送，key 应加入 `signal_type`。

### 4. 每日额度告警阈值 —— ✅ 已修复（见第五节 B7）
- 免费版每 key 800 次/日，原代码 80% 告警仅写日志、未推送；且 `used >= 80%` 在 640 次后恒真，**每次请求都告警（刷屏）**。
- **已修复**：改为阈值化推送企业微信（80/90/95/100%），每 key 每阈值每天仅一次。

### 5. 依赖版本
- `requirements.txt` 仅声明 `pandas>=2.0.0` 等下限，实际安装到 `pandas 3.0.5 / numpy 2.5.3`（2026 年最新）。建议后续锁定版本避免 breaking change。

---

## 四、部署说明

- **目录**：`/root/www/xaumonitor/`（已从 `/root/www/xaumonitor/XAUMonitor/` 扁平化）
- **虚拟环境**：`/root/www/xaumonitor/venv`（Python 3.12，依赖已装）
- **自启动**：`systemd` 服务 `xaumonitor.service`
  - 启动：`systemctl start xaumonitor`
  - 状态：`systemctl status xaumonitor`
  - 日志：`tail -f /root/www/xaumonitor/dpb_signals.log`
  - 开机自启：`systemctl enable xaumonitor`（已配置）
- **信号日志**：`dpb_signals.txt`（仅信号，供复盘）；运行日志 `dpb_signals.log`

---

## 五、第二轮改造（2026-09-18 追加）

### A2. 修复 trade_freq 静默覆盖 config 参数 —— ✅ 已修复
- **问题**：`calc_signals()` 中无论 config 写什么，`trend_stability` / `signal_cooldown` / `breakout_tolerance` 都被 `trade_freq` 强制覆盖。
  - 实测：config 写 `20/10/5`，实际生效 `10/3/2` → **用户改这三个参数完全无效**。
- **修复**：新增 `_FREQ_PRESETS` + `_apply_freq_preset()`，config 显式参数优先，`trade_freq` 预设仅在参数缺省时兜底；启动时打印实际生效参数。
- **验证**：修复后 config 的 `20/10/5` 真正生效（启动日志：`生效: trend_stability=20...`）。

### B7. 额度告警推送企业微信 —— ✅ 已修复
- **问题**：额度告警只写日志不推送，key 打满后监控**静默停摆无人知晓**；且 `used >= 80%` 在 640 次后恒真，每次请求都告警（刷屏）。
- **修复**：阈值化（80%/90%/95%/100%）推送企业微信，每 key 每阈值每天仅告警一次，跨天重置。
- **验证**：800 次模拟请求仅触发 4 次告警；真实推送端到端成功。

### A/B 全层改造 —— ✅ 全部完成（2026-09-18）

| 项 | 说明 | 状态 |
|----|------|------|
| A1 | 用 K线实体/ATR 动能代理替代失效的成交量过滤 | ✅ 信号量降 17~42% |
| A3 | 重构信号评分（统一 0-8 分，突破补全评分项） | ✅ S级 0→34 个 |
| A4 | 多周期共振标记（🔥） | ✅ 三阶段重构 |
| A5 | 信号等级门槛过滤（min_signal_grade） | ✅ 可按 A/S 过滤 |
| A6 | 突破专用止损（1.5×ATR） | ✅ 精确 1.50×ATR |
| B8 | SQLite 信号持久化 | ✅ 17 字段落库 |
| B9 | Web 仪表盘（FastAPI，端口 1689 / Nginx 8088） | ✅ 运行中 |
| B10 | 告警通道冗余（多 webhook） | ✅ 实测容错 |

**新增配置项**：`min_signal_grade`、`resonance_min_count`、`breakout_sl_atr`、
`use_momentum_filter`、`momentum_body_ratio`、`db_file`、`status_file`、`wecom_webhooks`

**新增服务**：`xaumonitor-web`（Web 仪表盘，systemd，开机自启）

**仪表盘地址**：`http://178.239.117.189:8088/`
