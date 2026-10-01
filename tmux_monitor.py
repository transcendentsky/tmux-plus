from __future__ import annotations

import argparse
import asyncio
import hashlib
import html
import ipaddress
import os
import re
import shlex
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import uvicorn
import yaml
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator


LIST_FORMAT = "#{session_name}\t#{window_index}\t#{window_name}\t#{pane_index}\t#{pane_id}\t#{pane_current_command}"


@dataclass
class PaneState:
    server_id: str
    server_name: str
    session: str
    window_index: str
    window_name: str
    pane_index: str
    pane_id: str
    command: str
    content_hash: str = ""
    last_changed_at: float | None = None
    last_seen_at: float | None = None
    status: str = "unknown"
    error: str | None = None

    def as_dict(self, now: float, idle_after: float) -> dict[str, Any]:
        idle_for = None if self.last_changed_at is None else max(0, now - self.last_changed_at)
        return {
            "id": f"{self.server_id}:{self.pane_id}",
            "server_id": self.server_id,
            "server_name": self.server_name,
            "session": self.session,
            "window_index": self.window_index,
            "window_name": self.window_name,
            "pane_index": self.pane_index,
            "pane_id": self.pane_id,
            "command": self.command,
            "status": self.status,
            "idle_for_seconds": None if idle_for is None else round(idle_for, 1),
            "last_seen_at": self.last_seen_at,
            "last_changed_at": self.last_changed_at,
            "error": self.error,
            "idle_after_seconds": idle_after,
        }


class QueryRequest(BaseModel):
    host: str = Field(min_length=1, max_length=253, description="服务器 IP 地址或主机名")
    username: str = Field(min_length=1, max_length=64)
    port: int = Field(default=22, ge=1, le=65535)
    name: str | None = Field(default=None, max_length=100)
    idle_after_seconds: float = Field(default=15, gt=0, le=3600)
    capture_lines: int = Field(default=40, ge=1, le=500)
    ssh_timeout_seconds: int = Field(default=8, ge=1, le=60)

    @field_validator("host")
    @classmethod
    def validate_host(cls, value: str) -> str:
        value = value.strip()
        if not value or value.startswith("-") or any(char.isspace() for char in value):
            raise ValueError("host must be an IP address or hostname")
        try:
            ipaddress.ip_address(value)
        except ValueError:
            if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?", value):
                raise ValueError("host must be an IP address or hostname")
        return value

    @field_validator("username")
    @classmethod
    def validate_username(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.-]*[$]?", value):
            raise ValueError("username contains unsupported characters")
        return value


@dataclass
class Monitor:
    config: dict[str, Any]
    panes: dict[str, PaneState] = field(default_factory=dict)
    server_errors: dict[str, str | None] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    stop_event: threading.Event = field(default_factory=threading.Event)

    @property
    def idle_after(self) -> float:
        return float(self.config.get("idle_after_seconds", 15))

    def ssh_command(self, server: dict[str, Any], remote: str) -> list[str]:
        args = ["ssh", "-o", "BatchMode=yes", "-o", f"ConnectTimeout={int(self.config.get('ssh_timeout_seconds', 8))}"]
        if server.get("port"):
            args += ["-p", str(server["port"])]
        if server.get("identity_file"):
            args += ["-i", os.path.expanduser(str(server["identity_file"]))]
        target = f"{server.get('user', '')}@{server['host']}" if server.get("user") else server["host"]
        return args + [target, remote]

    def run_ssh(self, server: dict[str, Any], remote: str) -> str:
        result = subprocess.run(
            self.ssh_command(server, remote), capture_output=True, text=True,
            timeout=float(self.config.get("ssh_timeout_seconds", 8)) + 2,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError((result.stderr or f"ssh exited {result.returncode}").strip()[-500:])
        return result.stdout

    def sample_server(self, server: dict[str, Any]) -> None:
        sid = str(server["id"])
        try:
            listing = self.run_ssh(server, f"tmux list-panes -a -F {shlex.quote(LIST_FORMAT)}")
            seen: set[str] = set()
            now = time.time()
            for line in listing.splitlines():
                parts = line.split("\t", 5)
                if len(parts) != 6:
                    continue
                session, wi, wn, pi, pane_id, command = parts
                key = f"{sid}:{pane_id}"
                seen.add(key)
                content = self.run_ssh(server, f"tmux capture-pane -p -J -t {shlex.quote(pane_id)} -S -{int(self.config.get('capture_lines', 40))}")
                fingerprint = hashlib.sha256(content.rstrip().encode("utf-8", "replace")).hexdigest()
                with self.lock:
                    old = self.panes.get(key)
                    if old is None:
                        state = PaneState(sid, str(server.get("name", sid)), session, wi, wn, pi, pane_id, command)
                        state.last_changed_at = now
                        self.panes[key] = state
                    else:
                        state = old
                        state.session, state.window_index, state.window_name = session, wi, wn
                        state.pane_index, state.command = pi, command
                        if fingerprint != state.content_hash:
                            state.last_changed_at = now
                        state.error = None
                    state.content_hash = fingerprint
                    state.last_seen_at = now
                    state.status = "unknown" if old is None else (
                        "running" if state.last_changed_at == now else
                        "idle" if now - (state.last_changed_at or now) >= self.idle_after else
                        "running"
                    )
            with self.lock:
                for key in list(self.panes):
                    if key.startswith(f"{sid}:") and key not in seen:
                        self.panes[key].status = "offline"
                self.server_errors[sid] = None
        except Exception as exc:
            with self.lock:
                self.server_errors[sid] = str(exc)
                for key, state in self.panes.items():
                    if key.startswith(f"{sid}:"):
                        state.status, state.error = "offline", str(exc)

    def poll_once(self) -> None:
        for server in self.config.get("servers", []):
            self.sample_server(server)

    def loop(self) -> None:
        interval = max(1.0, float(self.config.get("poll_interval_seconds", 5)))
        while not self.stop_event.is_set():
            started = time.monotonic()
            self.poll_once()
            self.stop_event.wait(max(0.1, interval - (time.monotonic() - started)))


def load_config(path: str) -> dict[str, Any]:
    with open(path, encoding="utf-8") as fh:
        config = yaml.safe_load(fh) or {}
    if not isinstance(config.get("servers"), list) or not config["servers"]:
        raise ValueError("config must contain a non-empty servers list")
    for server in config["servers"]:
        if not server.get("id") or not server.get("host"):
            raise ValueError("each server needs id and host")
    return config


def create_app(monitor: Monitor) -> FastAPI:
    app = FastAPI(title="tmux Monitor")
    token = os.environ.get("TMUX_MONITOR_TOKEN")
    query_monitors: dict[tuple[Any, ...], tuple[Monitor, dict[str, Any]]] = {}
    query_cache_lock = threading.Lock()
    query_sampling_lock = threading.Lock()

    def auth(authorization: str | None) -> None:
        if token and authorization != f"Bearer {token}":
            raise HTTPException(status_code=401, detail="missing or invalid bearer token")

    @app.get("/", response_class=HTMLResponse)
    def index() -> str:
        return INDEX_HTML

    @app.get("/api/health")
    def health(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        auth(authorization)
        return {"ok": True, "servers": len(monitor.config.get("servers", []))}

    @app.get("/api/panes")
    def panes(authorization: str | None = Header(default=None)) -> dict[str, Any]:
        auth(authorization)
        now = time.time()
        with monitor.lock:
            rows = [p.as_dict(now, monitor.idle_after) for p in monitor.panes.values()]
            errors = dict(monitor.server_errors)
        return {"generated_at": now, "panes": sorted(rows, key=lambda x: (x["server_name"], x["session"], x["window_index"], x["pane_index"])), "server_errors": errors}

    @app.post("/api/query")
    def query(body: QueryRequest, http_request: Request) -> dict[str, Any]:
        client_host = http_request.client.host if http_request.client else ""
        try:
            is_local_request = ipaddress.ip_address(client_host).is_loopback
        except ValueError:
            is_local_request = False
        if not is_local_request:
            raise HTTPException(status_code=403, detail="this endpoint is available only from localhost")

        server = {
            "id": "query",
            "name": body.name or body.host,
            "host": body.host,
            "user": body.username,
            "port": body.port,
        }
        config = {
            "servers": [server],
            "idle_after_seconds": body.idle_after_seconds,
            "capture_lines": body.capture_lines,
            "ssh_timeout_seconds": body.ssh_timeout_seconds,
        }
        cache_key = (
            body.username,
            body.host,
            body.port,
            body.idle_after_seconds,
            body.capture_lines,
            body.ssh_timeout_seconds,
        )
        with query_sampling_lock:
            with query_cache_lock:
                cached = query_monitors.get(cache_key)
                if cached is None:
                    monitor_for_query = Monitor(config)
                    query_monitors[cache_key] = (monitor_for_query, server)
                else:
                    monitor_for_query, server = cached
            monitor_for_query.sample_server(server)

        now = time.time()
        with monitor_for_query.lock:
            panes = [
                pane.as_dict(now, monitor_for_query.idle_after)
                for pane in monitor_for_query.panes.values()
                if pane.server_id == "query"
            ]
            error = monitor_for_query.server_errors.get("query")

        windows_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
        for pane in panes:
            windows_by_key.setdefault((pane["session"], pane["window_index"]), []).append(pane)

        windows = []
        for (session, window_index), window_panes in sorted(windows_by_key.items()):
            statuses = {pane["status"] for pane in window_panes}
            status = (
                "running" if "running" in statuses else
                "unknown" if "unknown" in statuses else
                "idle" if statuses == {"idle"} else
                "offline"
            )
            windows.append({
                "session": session,
                "window_index": window_index,
                "window_name": window_panes[0]["window_name"],
                "status": status,
                "is_running": status == "running",
                "panes": [
                    {
                        "pane_index": pane["pane_index"],
                        "pane_id": pane["pane_id"],
                        "command": pane["command"],
                        "status": pane["status"],
                        "idle_for_seconds": pane["idle_for_seconds"],
                    }
                    for pane in window_panes
                ],
            })

        return {
            "host": body.host,
            "username": body.username,
            "queried_at": now,
            "status_method": "pane_screen_changes",
            "idle_after_seconds": body.idle_after_seconds,
            "is_running": any(window["is_running"] for window in windows),
            "windows": windows,
            "error": error,
        }

    return app


INDEX_HTML = """<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>tmux 运行状态</title><style>
body{font-family:system-ui,-apple-system,sans-serif;background:#f5f7fb;color:#172033;margin:0;padding:24px}h1{margin:0 0 8px}.muted{color:#697586}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:14px;margin-top:20px}.card{background:white;border:1px solid #e5e9f0;border-radius:12px;padding:16px;box-shadow:0 2px 8px #1522380b}.top{display:flex;justify-content:space-between;gap:10px}.badge{border-radius:99px;padding:3px 9px;font-size:12px;font-weight:700}.running{background:#d9fbe8;color:#087443}.idle{background:#fff0c2;color:#885a00}.offline{background:#ffe0e0;color:#a51d2d}.unknown{background:#e8edf5;color:#556070}.meta{font-size:13px;color:#526174;margin-top:10px;line-height:1.7}.cmd{font-family:ui-monospace,monospace;background:#f1f3f7;padding:2px 5px;border-radius:4px}#error{color:#a51d2d;margin-top:12px}
</style></head><body><h1>tmux 运行状态</h1><div class="muted">按 pane 页面变化判断，自动刷新：<span id="updated">-</span></div><div id="error"></div><main class="grid" id="panes"></main>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const labels={running:'运行中',idle:'静止',offline:'离线',unknown:'等待采样'};
async function refresh(){try{let token=localStorage.getItem('tmux_monitor_token')||'';let opt=token?{headers:{Authorization:'Bearer '+token}}:{};let r=await fetch('/api/panes',opt);if(r.status===401){token=prompt('请输入 TMUX_MONITOR_TOKEN');if(!token)return;localStorage.setItem('tmux_monitor_token',token);r=await fetch('/api/panes',{headers:{Authorization:'Bearer '+token}})}if(!r.ok)throw Error('API '+r.status);const d=await r.json();document.querySelector('#updated').textContent=new Date(d.generated_at*1000).toLocaleTimeString();document.querySelector('#error').textContent=Object.entries(d.server_errors||{}).filter(([,v])=>v).map(([k,v])=>k+': '+v).join(' | ');document.querySelector('#panes').innerHTML=d.panes.map(p=>`<article class="card"><div class="top"><strong>${esc(p.server_name)} / ${esc(p.session)}:${esc(p.window_index)}.${esc(p.pane_index)}</strong><span class="badge ${esc(p.status)}">${labels[p.status]||p.status}</span></div><div class="meta">窗口：${esc(p.window_name)}<br>命令：<span class="cmd">${esc(p.command)}</span><br>${p.idle_for_seconds==null?'等待首次变化':'页面已 '+p.idle_for_seconds+' 秒未变化'}</div></article>`).join('')||'<div class="muted">暂未发现 tmux pane</div>'}catch(e){document.querySelector('#error').textContent='无法读取监控数据：'+e.message}}refresh();setInterval(refresh,5000);
</script></body></html>"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()
    monitor = Monitor(load_config(args.config))
    thread = threading.Thread(target=monitor.loop, daemon=True)
    thread.start()
    app = create_app(monitor)
    uvicorn.run(app, host=os.environ.get("TMUX_MONITOR_HOST", "0.0.0.0"), port=int(os.environ.get("TMUX_MONITOR_PORT", "8080")), log_level="info")


if __name__ == "__main__":
    main()
