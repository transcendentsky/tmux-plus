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
        servers = [
            {"id": str(server["id"]), "name": str(server.get("name", server["id"]))}
            for server in monitor.config.get("servers", [])
        ]
        return {
            "generated_at": now,
            "servers": servers,
            "panes": sorted(rows, key=lambda x: (x["server_name"], x["session"], x["window_index"], x["pane_index"])),
            "server_errors": errors,
        }

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
:root{color-scheme:light;--ink:#172033;--muted:#718096;--line:#e5eaf1;--paper:#f4f6fa;--green:#16834b;--green-bg:#e5f7ed;--gray:#8792a2;--gray-bg:#eef1f5}
*{box-sizing:border-box}body{margin:0;padding:32px;background:var(--paper);color:var(--ink);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.page{max-width:1440px;margin:auto}.header{display:flex;align-items:flex-end;justify-content:space-between;gap:24px;margin-bottom:28px}.eyebrow{font-size:11px;letter-spacing:.16em;text-transform:uppercase;color:#7b8798;font-weight:750}.title{font-size:clamp(28px,4vw,40px);letter-spacing:-.04em;margin:7px 0 5px}.subtitle{color:var(--muted);font-size:14px}.refresh{color:var(--muted);font-size:12px;white-space:nowrap}.refresh strong{display:block;color:var(--ink);font-size:14px;margin-top:4px;text-align:right}
.server-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(100%,420px),1fr));align-items:start;gap:20px}.server-card{--accent:#5369d9;--tint:#eef0ff;position:relative;overflow:hidden;background:#fff;border:1px solid var(--line);border-radius:18px;box-shadow:0 10px 30px #1d2b4510}.server-card:before{content:"";position:absolute;inset:0 0 auto;height:5px;background:var(--accent)}.server-head{padding:22px 22px 18px;display:flex;justify-content:space-between;gap:16px;align-items:flex-start}.server-label{color:var(--accent);font-size:10px;font-weight:800;letter-spacing:.14em;text-transform:uppercase}.server-name{font-size:20px;line-height:1.25;font-weight:750;margin-top:6px;overflow-wrap:anywhere}.server-count{font-size:12px;color:var(--muted);margin-top:6px}.server-state{white-space:nowrap;border-radius:999px;padding:6px 10px;background:var(--gray-bg);color:#667184;font-size:11px;font-weight:750}.server-state.active{background:var(--green-bg);color:var(--green)}.server-state.failed{background:#fff0f0;color:#b44848}
.server-error{margin:0 22px 14px;padding:10px 12px;border-radius:10px;background:#fff2f2;color:#a74444;font-size:12px;line-height:1.5;overflow-wrap:anywhere}.window-list{padding:0 14px 14px}.window-item{padding:14px 9px;border-top:1px solid #edf0f4}.window-title{display:flex;align-items:center;gap:10px;min-width:0}.dot{width:9px;height:9px;border-radius:50%;flex:0 0 9px;background:var(--gray);box-shadow:0 0 0 4px var(--gray-bg)}.window-item.active .dot{background:#20a35e;box-shadow:0 0 0 4px #e2f6ea}.window-main{min-width:0;flex:1}.window-name{font-size:14px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}.window-path{font-size:11px;color:var(--muted);margin-top:3px}.window-badge{font-size:11px;color:#788394;white-space:nowrap}.window-item.active .window-badge{color:var(--green);font-weight:700}.pane-list{display:flex;flex-wrap:wrap;gap:6px;margin:11px 0 0 19px}.pane-chip{max-width:100%;padding:5px 8px;border:1px solid #e9edf2;border-radius:7px;background:#fafbfc;color:#687487;font:11px ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}.window-item.active .pane-chip{border-color:#d8eee0;background:#f4fbf6;color:#31734c}.empty{margin:0 22px 20px;padding:20px 14px;border:1px dashed #dce2ea;border-radius:11px;color:#8993a2;text-align:center;font-size:13px}.global-error{margin:0 0 18px;color:#aa4444;font-size:13px}.no-servers{padding:48px 20px;border:1px dashed #d4dce7;border-radius:16px;text-align:center;color:var(--muted);background:#ffffff80}
@media(max-width:600px){body{padding:20px 14px}.header{align-items:flex-start;flex-direction:column;margin-bottom:20px}.refresh strong{text-align:left}.server-head{padding:19px 18px 15px}.server-error{margin:0 18px 12px}.window-list{padding:0 11px 10px}.empty{margin:0 18px 18px}}
</style></head><body><div class="page"><header class="header"><div><div class="eyebrow">TMUX FLEET MONITOR</div><h1 class="title">运行状态</h1><div class="subtitle">按终端页面变化跟踪各服务器窗口</div></div><div class="refresh">最近刷新<strong id="updated">等待数据</strong></div></header><div id="global-error" class="global-error"></div><main class="server-grid" id="servers"></main></div>
<script>
const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const palette=[['#5369d9','#eef0ff'],['#168b83','#e7f6f3'],['#c26a35','#fff1e7'],['#8259b5','#f3edfb'],['#bd5471','#fff0f3'],['#557d3e','#eff6e9'],['#3979a8','#eaf4fb'],['#9a7a2d','#faf4e4']];
const statusText={running:'有程序运行',idle:'无活动',unknown:'等待采样',offline:'离线'};
function render(d){
 const grouped=new Map();
 for(const server of (d.servers||[]))grouped.set(String(server.id),{id:String(server.id),name:server.name||server.id,panes:[],error:(d.server_errors||{})[server.id]||null});
 for(const pane of (d.panes||[])){let group=grouped.get(String(pane.server_id));if(!group){group={id:String(pane.server_id),name:pane.server_name||pane.server_id,panes:[],error:(d.server_errors||{})[pane.server_id]||null};grouped.set(group.id,group)}group.panes.push(pane)}
 const cards=[...grouped.values()].sort((a,b)=>a.name.localeCompare(b.name)).map((server,index)=>{
   const windows=new Map();for(const pane of server.panes){const key=JSON.stringify([pane.session,pane.window_index]);if(!windows.has(key))windows.set(key,[]);windows.get(key).push(pane)}
   const windowRows=[...windows.values()].map(items=>{items.sort((a,b)=>Number(a.pane_index)-Number(b.pane_index));const first=items[0],active=items.some(p=>p.status==='running');const status=active?'running':items.some(p=>p.status==='unknown')?'unknown':items.some(p=>p.status==='offline')?'offline':'idle';const commands=[...new Set(items.map(p=>p.command).filter(Boolean))];return `<section class="window-item ${active?'active':''}"><div class="window-title"><span class="dot"></span><div class="window-main"><div class="window-name">${esc(first.window_name||'窗口')}</div><div class="window-path">${esc(first.session)} · window ${esc(first.window_index)}</div></div><span class="window-badge">${statusText[status]}</span></div><div class="pane-list">${items.map(p=>`<span class="pane-chip">${esc(p.pane_id)} · ${esc(p.command||'shell')}${p.idle_for_seconds==null?'':` · ${Math.round(p.idle_for_seconds)}s`}</span>`).join('')}</div>${commands.length>1?`<div class="window-path" style="margin:8px 0 0 19px">进程：${commands.map(esc).join('、')}</div>`:''}</section>`}).join('');
   const activeCount=[...windows.values()].filter(items=>items.some(p=>p.status==='running')).length;const colors=palette[index%palette.length];const state=server.error?'failed':activeCount?'active':'';const stateText=server.error?'采样失败':activeCount?`${activeCount} 个窗口运行中`:server.panes.length?'暂无活动':'暂无窗口';
   return `<article class="server-card" style="--accent:${colors[0]};--tint:${colors[1]}"><div class="server-head"><div><div class="server-label">SERVER ${String(index+1).padStart(2,'0')}</div><div class="server-name">${esc(server.name)}</div><div class="server-count">${windows.size} 个 tmux 窗口 · ${server.panes.length} 个 pane</div></div><span class="server-state ${state}">${stateText}</span></div>${server.error?`<div class="server-error">${esc(server.error)}</div>`:''}${windowRows?`<div class="window-list">${windowRows}</div>`:`<div class="empty">${server.error?'暂时无法读取该服务器的 tmux 状态':'当前没有 tmux 窗口'}</div>`}</article>`
 }).join('');
 document.querySelector('#servers').innerHTML=cards||'<div class="no-servers">尚未配置服务器</div>';
}
async function refresh(){try{const r=await fetch('/api/panes');if(!r.ok)throw Error('API '+r.status);const data=await r.json();document.querySelector('#updated').textContent=new Date(data.generated_at*1000).toLocaleTimeString();document.querySelector('#global-error').textContent='';render(data)}catch(e){document.querySelector('#global-error').textContent='无法读取监控数据：'+e.message}}
refresh();setInterval(refresh,5000);
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
