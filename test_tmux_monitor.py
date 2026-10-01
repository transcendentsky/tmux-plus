import time

from tmux_monitor import Monitor, PaneState, create_app


def test_pane_state_serialization():
    state = PaneState("s", "Server", "main", "0", "shell", "0", "%1", "bash")
    state.last_changed_at = time.time() - 20
    state.last_seen_at = time.time()
    state.status = "idle"
    row = state.as_dict(time.time(), 15)
    assert row["status"] == "idle"
    assert row["idle_for_seconds"] >= 19


def test_app_has_api_routes():
    app = create_app(Monitor({"servers": []}))
    paths = {route.path for route in app.routes}
    assert "/api/panes" in paths
    assert "/" in paths
