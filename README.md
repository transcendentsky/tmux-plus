# tmux 多服务器运行状态监控器

这是一个轻量的中心化监控器：中心机通过 SSH 周期性读取多个服务器上的 tmux pane，比较 pane 页面内容的 SHA-256 指纹，并在浏览器中展示状态。

状态含义：

- `running`：本次采样与上次不同，说明页面正在变化。
- `idle`：页面连续保持不变，达到 `idle_after_seconds`。
- `unknown`：刚发现 pane，还没有足够的样本。
- `offline`：SSH 或 tmux 查询失败。

## 快速启动

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
cp config.example.yaml config.yaml
# 编辑 config.yaml，填入 SSH 主机和用户名
TMUX_MONITOR_TOKEN='change-me' .venv/bin/python tmux_monitor.py --config config.yaml
```

浏览器访问 `http://中心机地址:8080`。默认监听 `0.0.0.0`，可用 `TMUX_MONITOR_HOST` 和 `TMUX_MONITOR_PORT` 覆盖。

SSH 建议使用密钥和 `~/.ssh/config`，不要把私钥内容写入配置文件。中心机上的运行用户必须能无交互执行：

```bash
ssh user@server tmux list-panes -a
```

## 外部访问安全

设置 `TMUX_MONITOR_TOKEN` 后，API 需要 `Authorization: Bearer <token>`。由于页面本身是静态页面，生产环境建议放在 Tailscale/ZeroTier、VPN 或 Nginx/Caddy HTTPS 反向代理之后，并在代理层做认证。不要直接把未设置 token 的服务暴露到公网。

## Agent 查询接口

`POST /api/query` 可按需查询一台服务器的 tmux 窗口。接口只接受来自本机 loopback 地址的请求，不需要设置 `TMUX_MONITOR_TOKEN` 或发送 Bearer token；监控服务本身仍建议只绑定本机地址。监控中心运行用户需要能通过 SSH 密钥或 `~/.ssh/config` 无交互登录目标主机。

```bash
curl -X POST http://127.0.0.1:8080/api/query \
  -H 'Content-Type: application/json' \
  -d '{"username":"ubuntu","host":"192.168.1.101","port":22}'
```

可选字段：`name`、`idle_after_seconds`、`capture_lines`、`ssh_timeout_seconds`。返回值按 session 和 window 汇总 `status` 与 `is_running`，并附带每个 pane 的命令和状态。相同连接参数的查询会复用采样历史；首次查询返回 `unknown`，再次查询后再根据页面变化判断 `running` 或 `idle`。`running` 表示 pane 页面在最近 `idle_after_seconds` 内有变化，不代表操作系统层面的进程健康。

接口返回示例：

```json
{
  "host": "192.168.1.101",
  "username": "ubuntu",
  "status_method": "pane_screen_changes",
  "is_running": true,
  "windows": [
    {
      "session": "train",
      "window_index": "0",
      "window_name": "bash",
      "status": "running",
      "is_running": true,
      "panes": [
        {"pane_index": "0", "pane_id": "%0", "command": "python", "status": "running", "idle_for_seconds": 2.1}
      ]
    }
  ],
  "error": null
}
```

## 当前边界

这是“基于页面变化”的活动性监控，不等价于进程健康检查：等待输入、下载无终端输出、后台任务无刷新都会被判为 `idle`。后续可以增加命令级心跳、进程 CPU、任务日志和告警渠道。
