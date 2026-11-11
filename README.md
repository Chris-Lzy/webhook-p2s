# 拓竹打印机钉钉监控 (bambu-dingtalk-monitor)

轻量级拓竹（Bambu Lab）打印机局域网监控服务：通过打印机本地 MQTT 接口实时获取打印状态，将开始 / 完成 / 失败 / 暂停 / 进度等事件推送到钉钉群机器人。自带 Web 配置页，Docker 一键部署，无需注册云服务、数据不出局域网（推送除外）。

## 功能

- **Web 配置页**：打印机 IP / 序列号 / 访问码、钉钉 Webhook / 加签密钥 / 关键词，全部在页面上填写；密钥类配置脱敏显示、留空保持不变
- **实时状态**：打印进度条、层数、剩余时间、喷嘴 / 热床温度、任务文件名、已用时
- **事件推送（可按需勾选）**：开始打印 / 打印完成 / 打印失败 / 暂停 / 恢复 / 打印中离线 / 进度节点（每 N% 推一次）
- **连通性测试**：一键测试打印机 MQTT 连接、一键测试钉钉推送
- **事件日志**：最近 50 条推送与连接事件
- **Docker Compose 部署**：配置持久化到 `./data`，容器自动重启

## 快速开始

```bash
git clone https://github.com/liuzy-chris/bambu-dingtalk-monitor.git
cd bambu-dingtalk-monitor
docker compose up -d --build
```

浏览器打开 `http://NAS的IP:8787`，按页面提示填写配置即可。

## 配置说明

### 打印机（拓竹）

1. 打印机屏幕：**设置 → 局域网 / LAN 模式**，开启后页面会显示**访问码（Access Code）**
2. 序列号（SN）可在同一页面或「关于机器」中查看
3. 打印机与 NAS 需在同一局域网

### 钉钉群机器人

群设置 → 智能群助手 → 添加机器人 → **自定义**（Webhook 接入）：

| 安全设置 | 填写方式 |
|---|---|
| 加签 | 填入 `SEC` 开头的密钥 |
| 自定义关键词 | 填入关键词，推送时会自动加在消息标题前 |
| IP 白名单 | 把 NAS 的内网 IP 加入白名单 |

### 通知规则

| 开关 | 默认 | 说明 |
|---|---|---|
| 通知总开关 | 开 | 关闭后仅记录事件、不推送 |
| 开始打印 | 开 | 空闲 / 完成 / 失败 → 打印中 |
| 打印完成 | 开 | 含耗时统计 |
| 打印失败 | 开 | 含错误码 |
| 暂停 / 恢复 | 关 | |
| 打印中离线 | 开 | MQTT 断连且正在打印时提醒 |
| 进度节点推送 | 开 | 每推进 25% 推一次（可改 5–100） |

## 工作原理

服务以 MQTT over TLS（8883 端口，用户名 `bblp`，密码为访问码）连接打印机，订阅 `device/<SN>/report` 主题，解析 `gcode_state`、`percent`、`layer_num`、`remaining_time`、`nozzle_temper` 等字段；状态变化时按钉钉自定义机器人规范推送 markdown 消息（加签方式为 HMAC-SHA256）。

适配拓竹 X1 / P1 / A1 全系列（含 A1 mini），需开启局域网模式。

## 目录结构

```
├── app.py               # 服务主体（FastAPI + paho-mqtt + 钉钉推送）
├── static/index.html    # Web 配置与状态页
├── Dockerfile
├── docker-compose.yml
└── data/                # 运行时生成，保存 config.json
```

## License

[MIT](LICENSE)
