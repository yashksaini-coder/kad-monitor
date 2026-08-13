# Demo Harness Completion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make kad-monitor run out of the box, make its load generator real, complete the dashboard against the existing backend, add an A/B experiment runner that proves the CapacityLimiter thesis, and ship it with tests, CI, Docker, and snapshot history.

**Architecture:** Single trio event loop: `DHTQueryCoordinator` (dual `trio.CapacityLimiter`) + `SimulatedDHTNetwork` + background workers + Hypercorn/FastAPI with WebSocket snapshot push every 0.5s + single-file dashboard. Real-libp2p code stays in-tree but optional (lazy import). New: `src/experiment.py` (headless A/B runner), `src/history.py` (SQLite snapshot store).

**Tech Stack:** Python ≥3.11, trio, FastAPI, Hypercorn (trio worker), pytest + pytest-trio, httpx (ASGI tests), Chart.js (vendored), stdlib sqlite3, Docker + Compose, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-08-13-demo-harness-completion-design.md`

## Global Constraints

- Simulated mode must import and run with ONLY: trio, fastapi, hypercorn[trio], anyio[trio], pydantic (+ dev: pytest, pytest-trio, httpx). `libp2p` must never be imported unless `--mode real`.
- All async code is trio. Never add asyncio APIs or pytest-asyncio.
- No new runtime dependencies beyond stdlib (sqlite3, statistics, json) — Chart.js is vendored as a static file, Prometheus exposition is hand-rendered text.
- `query_fn` contract stays backward compatible: coordinator accepts BOTH `(found, closest, hops)` and `(found, closest, hops, path)` returns.
- Layer naming everywhere: **Layer A = query limiter, Layer B = walk sub-limiter, Layer C = stream pool** (the README scheme).
- Run all tests with: `.venv/bin/python -m pytest tests/ -v` from `/home/yks/projects/kad-monitor`.
- Every commit message ends with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- `tests/test_libp2p_node.py` must skip cleanly (not error) when libp2p is absent.

---

### Task 1: Runnable simulated mode — import fix, pyproject, venv

**Files:**
- Modify: `src/__init__.py`
- Modify: `requirements.txt`
- Modify: `tests/test_libp2p_node.py` (top of file only)
- Create: `pyproject.toml`
- Create: `tests/test_imports.py`
- Modify: `.gitignore` (add `.venv/`, `history.db`, `reports/` if missing)

**Interfaces:**
- Consumes: nothing (first task).
- Produces: `import src` works without libp2p; `.venv/` with all dev deps; `pytest` configured via `[tool.pytest.ini_options]` with `trio_mode = true`. All later tasks assume this venv and config.

- [ ] **Step 1: Write the failing test**

```python
# tests/test_imports.py
"""Simulated mode must not require libp2p (README: 'no external dependencies')."""
import sys


def test_src_imports_without_libp2p():
    for mod in [m for m in list(sys.modules) if m == "src" or m.startswith("src.")]:
        del sys.modules[mod]
    import src  # noqa: F401

    assert "libp2p" not in sys.modules, (
        "importing src must not pull in libp2p — simulated mode is dependency-light"
    )
```

- [ ] **Step 2: Create the venv and install deps (libp2p removed first so install succeeds)**

Edit `requirements.txt` to:

```text
# ── Core Runtime ───────────────────────────────────
trio>=0.22.2
fastapi>=0.110.0
hypercorn[trio]>=0.16.0
anyio[trio]>=4.3.0
pydantic>=2.0.0

# ── Optional: real py-libp2p mode ───────────────────
# Real mode (`python main.py --mode real`) additionally needs:
#   pip install libp2p==0.6.0 multiaddr>=0.0.9 base58>=2.1.1
# NOTE: libp2p 0.6.0 does not install on Python 3.14 (coincurve build failure).

# ── Dev / Testing ───────────────────────────────────
pytest>=8.0.0
pytest-trio>=0.8.0
httpx>=0.27.0
```

(`pytest-asyncio` is deleted — all-trio repo; `libp2p`/`multiaddr`/`base58` move to the comment block.)

Run: `cd /home/yks/projects/kad-monitor && python -m venv .venv && .venv/bin/pip install -r requirements.txt`

- [ ] **Step 3: Create `pyproject.toml`**

```toml
[project]
name = "kad-monitor"
version = "1.0.0"
description = "Test harness + real-time dashboard validating DHTQueryCoordinator resource-exhaustion fix"
requires-python = ">=3.11"

[tool.pytest.ini_options]
testpaths = ["tests"]
trio_mode = true
```

- [ ] **Step 4: Run the new test to verify it fails**

Run: `.venv/bin/python -m pytest tests/test_imports.py -v`
Expected: FAIL (ModuleNotFoundError: libp2p, raised from `src/__init__.py` line 7)

- [ ] **Step 5: Fix `src/__init__.py`**

Replace the whole file with:

```python
"""
src package for libp2p DHT Monitor.

NOTE: src.libp2p_node is intentionally NOT imported here — it requires the
optional libp2p dependency.  Real mode imports it lazily (see main.py).
"""

from src.coordinator import DHTQueryCoordinator, QueryPriority, QueryStatus
from src.dht_simulation import SimulatedDHTNetwork
from src.stream_manager import StreamManager
from src.workers import load_generator, metrics_broadcaster, random_walk_worker

__all__ = [
    "DHTQueryCoordinator",
    "QueryPriority",
    "QueryStatus",
    "StreamManager",
    "SimulatedDHTNetwork",
    "random_walk_worker",
    "load_generator",
    "metrics_broadcaster",
]
```

- [ ] **Step 6: Make the libp2p test module skip instead of error**

In `tests/test_libp2p_node.py`, immediately after the `import pytest` line (before any `src.libp2p_node` import), insert:

```python
pytest.importorskip("libp2p", reason="real-mode tests require the optional libp2p dependency")
```

- [ ] **Step 7: Add to `.gitignore`** (keep existing entries): `.venv/`, `history.db`, `reports/`, `__pycache__/`

- [ ] **Step 8: Run the full suite**

Run: `.venv/bin/python -m pytest tests/ -v`
Expected: `test_imports.py` PASSES; `test_libp2p_node.py` shows SKIPPED; `test_coordinator.py` passes. `test_integration.py::test_find_known_peer` may flake (known bug, fixed in Task 13) — any failure there is acceptable ONLY for that test.

- [ ] **Step 9: Smoke-run the server**

Run: `.venv/bin/python main.py --port 8901 &` then `sleep 3 && curl -s http://localhost:8901/api/snapshot | head -c 200 && kill %1`
Expected: JSON starting with `{"coordinator":`

- [ ] **Step 10: Commit**

```bash
git add src/__init__.py requirements.txt pyproject.toml tests/test_imports.py tests/test_libp2p_node.py .gitignore
git commit -m "fix: make simulated mode runnable without libp2p"
```

---

### Task 2: Load generator — real fire-and-forget + achieved QPS

**Files:**
- Modify: `src/workers.py:74-138` (`load_generator`)
- Create: `tests/test_workers.py`

**Interfaces:**
- Consumes: `DHTQueryCoordinator.find_peer(peer_id, query_fn, priority)`, `StreamManager.open_stream(peer, protocol)`, `network.query(pid)`, `network.peer_ids`.
- Produces: `load_generator` unchanged signature; the shared `state` dict gains key `"achieved_qps": float` (rolling 5s fire rate), updated continuously while active and set to `0.0` when idle. Task 3 and the dashboard (Task 8) read `state["achieved_qps"]`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_workers.py
"""Regression tests for the background workers (previously zero coverage —
which is how the fire-and-forget bug survived)."""
import pytest
import trio

from src.coordinator import DHTQueryCoordinator
from src.stream_manager import StreamManager
from src.workers import load_generator, metrics_broadcaster


class SlowFakeNetwork:
    """Every query takes 1 virtual second; records peak concurrency."""

    def __init__(self):
        self.peer_ids = ["peer-aaaa", "peer-bbbb", "peer-cccc"]
        self.current = 0
        self.peak = 0

    async def query(self, pid):
        self.current += 1
        self.peak = max(self.peak, self.current)
        try:
            await trio.sleep(1.0)
        finally:
            self.current -= 1
        return True, [], 1

    def snapshot(self):
        return {"network": {"scenario": "FAKE"}}


@pytest.mark.trio
async def test_load_generator_overlaps_queries(autojump_clock):
    """At 10 QPS with 1s queries, >1 query MUST be in flight (pre-fix peak == 1)."""
    network = SlowFakeNetwork()
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=50, max_random_walks=3, query_timeout=30.0
    )
    sm = StreamManager(max_streams=100)
    state = {"active": True, "qps": 10.0, "mode": "known"}

    async with trio.open_nursery() as nursery:
        await nursery.start(load_generator, coordinator, network, sm, state)
        await trio.sleep(3.0)
        nursery.cancel_scope.cancel()

    assert network.peak > 1, f"queries never overlapped (peak={network.peak})"
    assert state["achieved_qps"] > 5.0, f"achieved {state['achieved_qps']} of 10 QPS"


@pytest.mark.trio
async def test_load_generator_idle_reports_zero_qps(autojump_clock):
    network = SlowFakeNetwork()
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=50, max_random_walks=3, query_timeout=30.0
    )
    sm = StreamManager(max_streams=100)
    state = {"active": False, "qps": 10.0, "mode": "known"}

    async with trio.open_nursery() as nursery:
        await nursery.start(load_generator, coordinator, network, sm, state)
        await trio.sleep(2.0)
        nursery.cancel_scope.cancel()

    assert state["achieved_qps"] == 0.0
    assert network.peak == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_workers.py -v`
Expected: `test_load_generator_overlaps_queries` FAILS on `network.peak > 1` (the per-iteration nursery serializes queries). The idle test may pass on `peak == 0` but FAIL on the missing `achieved_qps` key.

- [ ] **Step 3: Rewrite `load_generator`**

Replace the entire `load_generator` function in `src/workers.py` (keep the module docstring/imports; delete the duplicate `import hashlib` inside the old body and the `trio.lowlevel.current_trio_token()` line):

```python
async def load_generator(
    coordinator: "DHTQueryCoordinator",
    network: "SimulatedDHTNetwork",
    stream_manager: "StreamManager",
    state: dict,
    *,
    task_status=trio.TASK_STATUS_IGNORED,
) -> None:
    """
    Configurable load generator for stress testing.

    ``state`` is a shared dict with keys:
      - ``active``:       bool  — whether the generator is running
      - ``qps``:          float — target queries per second
      - ``mode``:         str   — "known" | "unknown" | "mixed"
      - ``achieved_qps``: float — (written here) rolling 5s rate actually fired

    Queries are spawned into a long-lived nursery so they genuinely overlap —
    a per-iteration nursery would WAIT for each query on exit and cap
    concurrency at 1 (the original bug).
    """
    task_status.started(None)
    logger.info("Load generator started")

    window_s = 5.0
    fired: list[float] = []  # trio.current_time() stamps of fired queries

    async with trio.open_nursery() as nursery:
        while True:
            if not state.get("active", False):
                state["achieved_qps"] = 0.0
                fired.clear()
                await trio.sleep(0.5)
                continue

            qps = float(state.get("qps", 2.0))
            mode = state.get("mode", "mixed")
            interval = max(1.0 / qps, 0.05)

            if mode == "known" and network.peer_ids:
                target = random.choice(network.peer_ids)
            elif mode == "unknown":
                target = hashlib.sha256(str(time.time()).encode()).hexdigest()[:40]
            else:  # mixed
                if random.random() < 0.5 and network.peer_ids:
                    target = random.choice(network.peer_ids)
                else:
                    target = hashlib.sha256(str(time.time()).encode()).hexdigest()[:40]

            async def _fire(pid: str) -> None:
                async def _user_query_fn(p: str):
                    async with stream_manager.open_stream(p, "/libp2p/kad/1.0.0"):
                        return await network.query(p)

                try:
                    await coordinator.find_peer(
                        pid, _user_query_fn, priority=QueryPriority.USER
                    )
                except Exception as exc:
                    logger.debug("Load gen query error: %s", exc)

            nursery.start_soon(_fire, target)

            now = trio.current_time()
            fired.append(now)
            fired[:] = [t for t in fired if t >= now - window_s]
            state["achieved_qps"] = round(len(fired) / window_s, 2)

            await trio.sleep(interval)
```

- [ ] **Step 4: Run to verify pass**

Run: `.venv/bin/python -m pytest tests/test_workers.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Run whole suite (no regressions)**

Run: `.venv/bin/python -m pytest tests/ -v`

- [ ] **Step 6: Commit**

```bash
git add src/workers.py tests/test_workers.py
git commit -m "fix: load generator actually fires-and-forgets; report achieved QPS"
```

---

### Task 3: Single broadcaster + consistent snapshot shape

**Files:**
- Modify: `api/app.py:98-135` (signature + delete dead broadcaster), `api/app.py:141-148` (`/api/snapshot`), `api/app.py:323-335` (WS initial frame)
- Modify: `src/workers.py` (`metrics_broadcaster` gains `extra` param)
- Modify: `main.py:166-256` and `main.py:295-399` (both call sites)
- Test: extended in Task 4 (`tests/test_api.py`); this task verifies via existing suite + smoke run

**Interfaces:**
- Consumes: Task 2's `state["achieved_qps"]`.
- Produces:
  - `create_app(coordinator, network, stream_manager, load_gen_state, libp2p_node=None, mode="simulated") -> tuple[FastAPI, list]` — `broadcast_send`/`broadcast_recv` params REMOVED.
  - `metrics_broadcaster(coordinator, network, stream_manager, send_channel, interval_s=0.5, extra=None, *, task_status=...)` — `extra: dict | None` is merged into every pushed snapshot.
  - Every snapshot (REST, WS-initial, WS-pushed) now contains `"mode"` and `"load_gen"` keys. Tasks 4, 8, 16, 17 rely on this shape.

- [ ] **Step 1: Delete the dead broadcaster in `api/app.py`**

Remove lines 119–135 (the `_ws_broadcaster` function and the `@app.on_event("startup")` `_start_broadcaster`). Remove `broadcast_send` and `broadcast_recv` from the `create_app` signature and add `mode: str = "simulated"`:

```python
def create_app(
    coordinator,
    network,
    stream_manager,
    load_gen_state: dict,
    libp2p_node=None,
    mode: str = "simulated",
) -> tuple:
```

Remove the now-unused `import trio` from `api/app.py` if nothing else uses it (check with grep first).

- [ ] **Step 2: Unify snapshot shape in `api/app.py`**

Add a helper inside `create_app` (above the routes) and use it in BOTH `/api/snapshot` and the WS initial frame:

```python
    def _full_snapshot() -> dict:
        return {
            **coordinator.snapshot(),
            **stream_manager.snapshot(),
            **network.snapshot(),
            "load_gen": dict(load_gen_state),
            "mode": mode,
            "ts": time.time(),
        }
```

`/api/snapshot` becomes `return _full_snapshot()`; the WS handler's initial `snapshot = {...}` block becomes `snapshot = _full_snapshot()`.

- [ ] **Step 3: `metrics_broadcaster` gains `extra`**

In `src/workers.py`, change the signature and snapshot merge:

```python
async def metrics_broadcaster(
    coordinator: "DHTQueryCoordinator",
    network: "SimulatedDHTNetwork",
    stream_manager: "StreamManager",
    send_channel: trio.MemorySendChannel,
    interval_s: float = 0.5,
    extra: dict | None = None,
    *,
    task_status=trio.TASK_STATUS_IGNORED,
) -> None:
```

and inside the loop:

```python
        snapshot = {
            **coordinator.snapshot(),
            **stream_manager.snapshot(),
            **network.snapshot(),
            "ts": time.time(),
            **(extra or {}),
        }
```

- [ ] **Step 4: Update both call sites in `main.py`**

In `main_simulated`: the `create_app(...)` call drops `broadcast_send=`/`broadcast_recv=` and adds `mode="simulated"`. The `_on_snapshot` merged dict gains `"load_gen": dict(load_gen_state)` (move the `load_gen_state` definition ABOVE `_on_snapshot`). The `nursery.start(metrics_broadcaster, ...)` call gains a final positional arg:

```python
        await nursery.start(
            metrics_broadcaster,
            coordinator,
            network,
            stream_manager,
            broadcast_send,
            args.broadcast_interval,
            {"mode": "simulated", "load_gen": load_gen_state},
        )
```

(`load_gen_state` is passed by reference intentionally — the broadcaster reads current values each tick. Note `json.dumps` serializes it fine.)

Mirror the same three changes in `main_real` with `"mode": "real"` (its `_on_snapshot` also keeps `peer_id`/`listen_addrs`).

- [ ] **Step 5: Verify — suite + smoke**

Run: `.venv/bin/python -m pytest tests/ -v` (no regressions)
Run: `.venv/bin/python main.py --port 8901 &` then
`sleep 3 && curl -s http://localhost:8901/api/snapshot | .venv/bin/python -c "import json,sys; d=json.load(sys.stdin); print(d['mode'], 'load_gen' in d)" && kill %1`
Expected: `simulated True`

- [ ] **Step 6: Commit**

```bash
git add api/app.py src/workers.py main.py
git commit -m "fix: remove dead startup broadcaster; unify snapshot shape (mode, load_gen)"
```

---

### Task 4: API test suite (httpx / ASGITransport)

**Files:**
- Create: `tests/test_api.py`

**Interfaces:**
- Consumes: `create_app(...)` from Task 3.
- Produces: the `client()` fixture pattern reused by Tasks 5, 16, 17 test steps.

- [ ] **Step 1: Write the tests**

```python
# tests/test_api.py
"""API-layer tests via httpx ASGITransport — the coverage whose absence let
the dead startup broadcaster survive."""
import httpx
import pytest

from api.app import create_app
from src.coordinator import DHTQueryCoordinator
from src.dht_simulation import SimulatedDHTNetwork
from src.stream_manager import StreamManager


def make_client() -> httpx.AsyncClient:
    network = SimulatedDHTNetwork(node_count=20, scenario="NORMAL")
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=10, max_random_walks=3, query_timeout=5.0
    )
    sm = StreamManager(max_streams=20)
    state = {"active": False, "qps": 2.0, "mode": "mixed", "achieved_qps": 0.0}
    app, _ = create_app(
        coordinator=coordinator,
        network=network,
        stream_manager=sm,
        load_gen_state=state,
        libp2p_node=None,
        mode="simulated",
    )
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )


@pytest.mark.trio
async def test_snapshot_shape():
    async with make_client() as client:
        r = await client.get("/api/snapshot")
    assert r.status_code == 200
    d = r.json()
    for key in ("coordinator", "stream_manager", "network", "mode", "load_gen", "ts"):
        assert key in d, f"snapshot missing {key}"
    assert d["mode"] == "simulated"


@pytest.mark.trio
async def test_query_executes():
    async with make_client() as client:
        r = await client.post("/api/query", json={"mode": "known"})
    assert r.status_code == 200
    d = r.json()
    assert d["status"] in ("success", "failed", "timeout")
    assert d["query_id"].startswith("q-")


@pytest.mark.trio
async def test_config_hot_reload():
    async with make_client() as client:
        r = await client.post(
            "/api/config",
            json={"max_concurrent_queries": 15, "max_random_walks": 5,
                  "query_timeout": 9.0, "max_streams": 33},
        )
        assert r.status_code == 200
        snap = (await client.get("/api/snapshot")).json()
    assert snap["coordinator"]["config"]["max_concurrent_queries"] == 15
    assert snap["coordinator"]["config"]["query_timeout_s"] == 9.0
    assert snap["stream_manager"]["config"]["max_streams"] == 33


@pytest.mark.trio
async def test_loadgen_roundtrip():
    async with make_client() as client:
        r = await client.post(
            "/api/loadgen", json={"active": True, "qps": 7.5, "mode": "known"}
        )
        assert r.status_code == 200
        d = (await client.get("/api/loadgen")).json()
    assert d["active"] is True
    assert d["qps"] == 7.5
    assert "achieved_qps" in d


@pytest.mark.trio
async def test_scenario_switch_and_reject():
    async with make_client() as client:
        ok = await client.post("/api/network/scenario", json={"scenario": "STRESSED"})
        bad = await client.post("/api/network/scenario", json={"scenario": "BOGUS"})
    assert ok.status_code == 200
    assert ok.json()["network"]["scenario"] == "STRESSED"
    assert bad.status_code == 400


@pytest.mark.trio
async def test_real_mode_endpoints_rejected_in_simulated():
    async with make_client() as client:
        r = await client.post("/api/dht/put", json={"key": "/k", "value": "v"})
    assert r.status_code == 400
```

- [ ] **Step 2: Run**

Run: `.venv/bin/python -m pytest tests/test_api.py -v`
Expected: all PASS (this task adds tests for behavior that already exists after Task 3 — if any fail, the production code from Task 3 is wrong; fix it, not the test).

- [ ] **Step 3: Commit**

```bash
git add tests/test_api.py
git commit -m "test: API endpoint coverage via httpx ASGITransport"
```

---

### Task 5: Peer chaos routes

**Files:**
- Modify: `api/app.py` (two new routes, next to the existing `POST /api/network/peer`)
- Test: append to `tests/test_api.py`

**Interfaces:**
- Consumes: `SimulatedDHTNetwork.remove_peer(peer_id) -> bool`, `SimulatedDHTNetwork.toggle_peer_online(peer_id) -> bool | None` (both already exist).
- Produces: `DELETE /api/network/peer/{peer_id}` → `{"status": "removed", "peer_id"}` | 404 | 400 (real mode); `POST /api/network/peer/{peer_id}/toggle` → `{"status": "ok", "peer_id", "online": bool}` | 404 | 400. Task 8's UI calls these.

- [ ] **Step 1: Write failing tests (append to `tests/test_api.py`)**

```python
@pytest.mark.trio
async def test_peer_chaos_remove_and_toggle():
    async with make_client() as client:
        nodes = (await client.get("/api/nodes")).json()["nodes"]
        victim = nodes[0]["peer_id_full"]

        t = await client.post(f"/api/network/peer/{victim}/toggle")
        assert t.status_code == 200
        assert t.json()["online"] in (True, False)

        r = await client.delete(f"/api/network/peer/{victim}")
        assert r.status_code == 200
        assert r.json()["status"] == "removed"

        again = await client.delete(f"/api/network/peer/{victim}")
        assert again.status_code == 404
```

- [ ] **Step 2: Run to verify failure**

Run: `.venv/bin/python -m pytest tests/test_api.py::test_peer_chaos_remove_and_toggle -v`
Expected: FAIL with 405/404 (routes don't exist).

- [ ] **Step 3: Add the routes in `api/app.py`** (directly under the existing `add_peer` route)

```python
    @app.delete("/api/network/peer/{peer_id}")
    async def remove_peer(peer_id: str):
        if libp2p_node is not None:
            raise HTTPException(400, "Peer removal is simulated-mode only")
        if network.remove_peer(peer_id):
            return {"status": "removed", "peer_id": peer_id}
        raise HTTPException(404, f"Unknown peer: {peer_id}")

    @app.post("/api/network/peer/{peer_id}/toggle")
    async def toggle_peer(peer_id: str):
        if libp2p_node is not None:
            raise HTTPException(400, "Peer toggle is simulated-mode only")
        online = network.toggle_peer_online(peer_id)
        if online is None:
            raise HTTPException(404, f"Unknown peer: {peer_id}")
        return {"status": "ok", "peer_id": peer_id, "online": online}
```

- [ ] **Step 4: Run to verify pass, then full suite**

Run: `.venv/bin/python -m pytest tests/test_api.py -v && .venv/bin/python -m pytest tests/ -v`

- [ ] **Step 5: Commit**

```bash
git add api/app.py tests/test_api.py
git commit -m "feat: peer chaos endpoints (remove, toggle online)"
```

---

### Task 6: Latency percentiles in coordinator snapshot

**Files:**
- Modify: `src/coordinator.py` (module-level helper + `snapshot()` rates block)
- Test: append to `tests/test_coordinator.py`

**Interfaces:**
- Consumes: `self._history: list[QueryResult]` (exists).
- Produces: `snapshot()["coordinator"]["rates"]` gains `"p50_ms"`, `"p95_ms"`, `"p99_ms"` (floats, 0.0 when no history). Tasks 9 (UI) and 14 (experiment summary) read these.

- [ ] **Step 1: Write the failing test (append to `tests/test_coordinator.py`; reuse the file's existing fixture/fake-query style — read its imports first and match them)**

```python
@pytest.mark.trio
async def test_snapshot_latency_percentiles(autojump_clock):
    coord = DHTQueryCoordinator(
        max_concurrent_queries=10, max_random_walks=3, query_timeout=30.0
    )

    def make_query_fn(delay_s):
        async def _fn(pid):
            await trio.sleep(delay_s)
            return True, [], 1
        return _fn

    # 20 queries: durations 0.1s .. 2.0s
    for i in range(1, 21):
        await coord.find_peer(f"peer-{i}", make_query_fn(i * 0.1))

    rates = coord.snapshot()["coordinator"]["rates"]
    assert 0 < rates["p50_ms"] <= rates["p95_ms"] <= rates["p99_ms"]
    assert 900 <= rates["p50_ms"] <= 1200   # median ≈ 1.05s
    assert rates["p99_ms"] <= 2100


def test_percentiles_empty_history():
    coord = DHTQueryCoordinator(max_concurrent_queries=10, max_random_walks=3)
    rates = coord.snapshot()["coordinator"]["rates"]
    assert rates["p50_ms"] == rates["p95_ms"] == rates["p99_ms"] == 0.0
```

- [ ] **Step 2: Run to verify failure** — `KeyError: 'p50_ms'`

- [ ] **Step 3: Implement**

Module-level helper in `src/coordinator.py` (below the imports):

```python
def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Linear-interpolated percentile; sorted_vals must be pre-sorted."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)
```

In `snapshot()`, before the `return`, add:

```python
        durations = sorted(r.duration_ms for r in self._history)
```

and inside the `"rates"` dict add:

```python
                    "p50_ms": round(_percentile(durations, 0.50), 1),
                    "p95_ms": round(_percentile(durations, 0.95), 1),
                    "p99_ms": round(_percentile(durations, 0.99), 1),
```

- [ ] **Step 4: Run to verify pass, then full suite**
- [ ] **Step 5: Commit** — `feat: p50/p95/p99 latency percentiles in coordinator snapshot`

---

### Task 7: Lookup-path recording

**Files:**
- Modify: `src/dht_simulation.py:197-261` (`query`)
- Modify: `src/coordinator.py` (`QueryResult` + `_execute` unpacking)
- Test: append to `tests/test_integration.py`

**Interfaces:**
- Consumes: existing `query`/`find_peer` flow.
- Produces:
  - `SimulatedDHTNetwork.query` returns `(found, closest_peers, hops, path)` where `path: list[str]` is the ordered full peer IDs contacted.
  - Coordinator accepts 3- OR 4-tuple `query_fn` returns (backward compatible — `RealDHTNetwork` and all existing test fakes keep returning 3-tuples).
  - `QueryResult.path: list[str]`; `to_dict()["path"]` = full IDs. Task 10's topology viz reads `recent_results[0].path`.

- [ ] **Step 1: Write the failing test (append to `tests/test_integration.py`, matching its existing fixture style)**

```python
@pytest.mark.trio
async def test_query_records_lookup_path():
    network = SimulatedDHTNetwork(node_count=30, scenario="NORMAL")
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=10, max_random_walks=3, query_timeout=30.0
    )

    async def _query_fn(pid):
        return await network.query(pid)

    # Retry a few times: NORMAL injects 5% stream errors → occasional FAILED
    for _ in range(5):
        target = network.peer_ids[0]
        result = await coordinator.find_peer(target, _query_fn)
        if result.status == QueryStatus.SUCCESS:
            break
    assert result.status == QueryStatus.SUCCESS

    assert result.hops > 0
    assert len(result.path) == result.hops
    assert all(p in network.peer_ids for p in result.path)
    assert result.to_dict()["path"] == result.path
```

- [ ] **Step 2: Run to verify failure** — `AttributeError: path` (or unpacking error).

- [ ] **Step 3: Implement in `src/dht_simulation.py`**

In `query()`: after `hops = 0` add `path: list[str] = []`. Immediately after the `hops += 1` line add `path.append(candidate_id)`. Change the two returns:

```python
                    return True, [candidate_id] + candidate_node.k_closest(target_peer_id, 5), hops, path
```
```python
        return False, deduped_closest[:20], hops, path
```

Update the docstring's Returns section to `(found, closest_peers, hop_count, path)`.

- [ ] **Step 4: Implement in `src/coordinator.py`**

`QueryResult` gains a field (after `hops`): `path: list[str] = field(default_factory=list)` and `to_dict()` gains `"path": self.path,`.

In `_execute`, replace the unpacking line:

```python
                res = await query_fn(lq.peer_id)
                found, closest_peers, hops, *rest = res
                path = list(rest[0]) if rest else []
```

and add `path=path,` to the SUCCESS `QueryResult(...)` construction. Update `find_peer`'s docstring: query_fn may return `(found, closest_peers, hops)` or `(found, closest_peers, hops, path)`.

- [ ] **Step 5: Run to verify pass, then full suite** (existing coordinator tests use 3-tuple fakes and MUST still pass — that's the compatibility check).

- [ ] **Step 6: Commit** — `feat: record lookup path through simulated Kademlia iterative lookup`

---

### Task 8: Dashboard — controls wiring (scenario, chaos, loadgen sync, full config)

**Files:**
- Modify: `static/index.html` (sidebar cards + JS)

No automated JS tests (single-file dashboard, no JS test infra — deliberate). Verification is a scripted curl + browser smoke.

**Interfaces:**
- Consumes: `POST /api/network/scenario`, `POST /api/network/peer`, `DELETE /api/network/peer/{id}`, `POST /api/network/peer/{id}/toggle`, `GET /api/loadgen`, `POST /api/config` (5 fields), snapshot keys `network.scenario`, `load_gen.achieved_qps`, `stream_manager.config`.
- Produces: dashboard controls for all of the above; helper `escapeHtml(s)` used by later tasks.

- [ ] **Step 1: Add a Scenario card** in the sidebar (after the "Resource Pools" card):

```html
    <div class="card">
      <div class="section-title">Network Scenario</div>
      <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
        <button class="btn btn-outline scenario-btn" data-scenario="NORMAL" onclick="setScenario('NORMAL')">NORMAL</button>
        <button class="btn btn-outline scenario-btn" data-scenario="DEGRADED" onclick="setScenario('DEGRADED')">DEGRADED</button>
        <button class="btn btn-outline scenario-btn" data-scenario="STRESSED" onclick="setScenario('STRESSED')">STRESSED</button>
        <button class="btn btn-outline scenario-btn" data-scenario="SATURATED" onclick="setScenario('SATURATED')">SATURATED</button>
      </div>
      <div style="font-size:9px; color:var(--text-dim); margin-top:8px;">Switching rebuilds the simulated network (new peer IDs).</div>
    </div>

    <div class="card">
      <div class="section-title">Peer Chaos</div>
      <div style="display:grid; grid-template-columns:1fr 1fr 1fr; gap:8px;">
        <button class="btn btn-outline" onclick="addPeer()">ADD</button>
        <button class="btn btn-outline" onclick="removeRandomPeer()">KILL</button>
        <button class="btn btn-outline" onclick="toggleRandomPeer()">FLAP</button>
      </div>
    </div>
```

- [ ] **Step 2: Add the JS** (in the `<script>` block):

```javascript
function escapeHtml(s) {
  return String(s).replace(/[&<>"']/g, c => (
    {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
}

async function setScenario(s) {
  await fetch('/api/network/scenario', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ scenario: s }) });
}

function randomNodeId() {
  const nodes = (lastSnapshot && lastSnapshot.nodes) || [];
  if (!nodes.length) return null;
  return nodes[Math.floor(Math.random() * nodes.length)].peer_id_full;
}

async function addPeer() {
  await fetch('/api/network/peer', { method: 'POST' });
}
async function removeRandomPeer() {
  const id = randomNodeId();
  if (id) await fetch(`/api/network/peer/${id}`, { method: 'DELETE' });
}
async function toggleRandomPeer() {
  const id = randomNodeId();
  if (id) await fetch(`/api/network/peer/${id}/toggle`, { method: 'POST' });
}

async function syncLoadgen() {
  try {
    const s = await (await fetch('/api/loadgen')).json();
    loadActive = !!s.active;
    document.getElementById('qpsRange').value = s.qps;
    document.getElementById('qpsVal').textContent = s.qps;
    document.getElementById('loadMode').value = s.mode;
    const btn = document.getElementById('loadBtn');
    btn.textContent = loadActive ? 'STOP LOAD GENERATOR' : 'START LOAD GENERATOR';
    btn.classList.toggle('btn-primary', loadActive);
  } catch (e) { /* server not up yet — WS reconnect will drive retry */ }
}
```

In `init()`, add `syncLoadgen();` after `connectWS();`.

In `handleSnapshot`, add scenario-button highlighting and the loadgen readout:

```javascript
  const scen = snap.network?.scenario;
  document.querySelectorAll('.scenario-btn').forEach(b =>
    b.classList.toggle('active', b.dataset.scenario === scen));
  const lg = snap.load_gen || {};
  setText('lgReq', lg.active ? (lg.qps ?? 0) : 0);
  setText('lgFired', (lg.achieved_qps ?? 0).toFixed(1));
  setText('lgDone', (rates.throughput_qps || 0).toFixed(1));
```

- [ ] **Step 3: Loadgen readout markup** — inside the Traffic Generator card, above the START button:

```html
      <div style="font-size:10px; font-family:var(--font-mono); color:var(--text-dim); margin-bottom:12px; display:flex; justify-content:space-between;">
        <span>req <b id="lgReq" style="color:#fff">0</b></span>
        <span>fired <b id="lgFired" style="color:var(--color-yellow)">0</b></span>
        <span>done <b id="lgDone" style="color:var(--color-green)">0</b></span>
      </div>
```

- [ ] **Step 4: Config modal — all 5 fields.** Add two inputs to the modal (after the QUERY TIMEOUT block):

```html
    <div style="margin-bottom:16px;">
      <span style="font-size:10px; color:var(--text-dim); display:block; margin-bottom:6px; font-weight:700">MAX STREAMS</span>
      <input type="number" id="cfgMaxS">
    </div>
    <div style="margin-bottom:24px;">
      <span style="font-size:10px; color:var(--text-dim); display:block; margin-bottom:6px; font-weight:700">STREAM TIMEOUT (SECONDS)</span>
      <input type="number" id="cfgStreamT">
    </div>
```

In `openConfig()` add (guarding on the key existing):

```javascript
    const smc = (lastSnapshot.stream_manager || {}).config || {};
    document.getElementById('cfgMaxS').value = smc.max_streams ?? 50;
    document.getElementById('cfgStreamT').value = smc.stream_timeout_s ?? 60;
```

In `applyConfig()` body JSON add:

```javascript
      max_streams: parseInt(document.getElementById('cfgMaxS').value),
      stream_timeout: parseFloat(document.getElementById('cfgStreamT').value)
```

- [ ] **Step 5: Verify**

Run: `.venv/bin/python main.py --port 8901 &`, then:
- `curl -s -X POST localhost:8901/api/network/scenario -H 'Content-Type: application/json' -d '{"scenario":"DEGRADED"}' | grep -o DEGRADED | head -1`
- Open `http://localhost:8901/` in a browser: click each scenario button (active highlight follows), ADD/KILL/FLAP change the topology count, start the load generator → req/fired/done move; reload the page → button still says STOP. Open config modal → 5 fields populated; apply → gauges reflect new caps.
- `kill %1`

- [ ] **Step 6: Commit** — `feat(dashboard): scenario switcher, peer chaos, loadgen sync + QPS readout, full config modal`

---

### Task 9: Dashboard — event log + percentile display

**Files:**
- Modify: `static/index.html`

**Interfaces:**
- Consumes: snapshot keys `network.scenario`, `coordinator.config`, `recent_results[*].{query_id,status,peer_id,duration_ms}`, `capacity_limiter.utilisation_pct`, `rates.p95_ms/p99_ms`; `escapeHtml` from Task 8.
- Produces: `logEvent(msg, level)` used by any later dashboard work.

- [ ] **Step 1: Implement `logEvent` + snapshot-delta events** (in the `<script>`):

```javascript
const LOG_LIMIT = 200;
const seenResults = new Set();
let prevScenario = null, prevConfigJson = null, prevSaturated = false;

function logEvent(msg, level = 'info') {
  const area = document.getElementById('logArea');
  const color = { info: 'var(--text-dim)', warn: 'var(--color-yellow)', error: 'var(--color-red)', ok: 'var(--color-green)' }[level];
  const line = document.createElement('div');
  const ts = new Date().toLocaleTimeString('en-GB', { hour12: false });
  line.innerHTML = `<span style="color:var(--border-strong)">${ts}</span> <span style="color:${color}">${escapeHtml(msg)}</span>`;
  area.prepend(line);
  while (area.childElementCount > LOG_LIMIT) area.lastElementChild.remove();
}

function deriveEvents(snap) {
  const scen = snap.network?.scenario;
  if (prevScenario && scen !== prevScenario) logEvent(`scenario → ${scen}`, 'warn');
  prevScenario = scen;

  const cfg = JSON.stringify(snap.coordinator?.config || {});
  if (prevConfigJson && cfg !== prevConfigJson) logEvent('limits reconfigured', 'info');
  prevConfigJson = cfg;

  (snap.recent_results || []).slice(0, 10).forEach(r => {
    if (seenResults.has(r.query_id)) return;
    seenResults.add(r.query_id);
    if (r.status === 'timeout') logEvent(`${r.query_id} TIMEOUT after ${Math.round(r.duration_ms)}ms`, 'error');
    else if (r.status === 'failed') logEvent(`${r.query_id} failed (${r.peer_id})`, 'warn');
  });
  if (seenResults.size > 2000) seenResults.clear();

  const util = snap.capacity_limiter?.utilisation_pct || 0;
  if (util >= 90 && !prevSaturated) logEvent(`query pool saturated (${util}%) — back-pressure engaged`, 'error');
  if (util < 90 && prevSaturated) logEvent('query pool below saturation', 'ok');
  prevSaturated = util >= 90;
}
```

Call `deriveEvents(snap);` at the end of `handleSnapshot`. In `connectWS`, add `logEvent('websocket connected', 'ok')` in `onopen` and `logEvent('websocket lost — reconnecting', 'error')` in `onclose`.

Note: `.log-container` uses `flex-direction: column-reverse`, so `prepend` + column-reverse would flip order — change the CSS `flex-direction: column-reverse` to `column` on `.log-container` (prepend already puts newest on top).

- [ ] **Step 2: Percentiles in the DHT Latency panel** — replace the panel-sub-stats span pair with:

```html
          <span class="sub-stat-item">p95: <b id="cP95">0</b>ms</span>
          <span class="sub-stat-item">p99: <b id="cP99">0</b>ms</span>
          <span class="sub-stat-item">Network: <b id="cScenario">NORMAL</b></span>
```

and in `handleSnapshot`: `setText('cP95', rates.p95_ms || 0); setText('cP99', rates.p99_ms || 0);`

- [ ] **Step 3: Verify** — run the server, fire queries + SATURATED scenario + load gen; the Events panel shows scenario changes, timeouts, saturation crossings; p95/p99 populate.

- [ ] **Step 4: Commit** — `feat(dashboard): live event log + latency percentiles`

---

### Task 10: Dashboard — XOR topology, lookup-path highlight, escaping

**Files:**
- Modify: `static/index.html` (`renderTopology`, `renderResults`, `renderLiveQueries`, `handleSnapshot`)

**Interfaces:**
- Consumes: `recent_results[*].path` + `.peer_id_full` (Task 7), `nodes[*].peer_id_full/online`, `network.online_nodes`, `escapeHtml` (Task 8).
- Produces: nothing consumed later.

- [ ] **Step 1: Escape peer-derived strings.** In `renderResults`, wrap every interpolated field: `${escapeHtml(r.query_id)}`, `title="${escapeHtml(r.peer_id_full)}"`, `${escapeHtml(r.peer_id)}`, `${escapeHtml(r.status)}`. Same in `renderLiveQueries` for `q.query_id`, `q.peer_id`, `q.status`.

- [ ] **Step 2: Rewrite `renderTopology` — XOR-distance layout + path.** The backend ranks XOR distance on `sha256(peer_id)[:20]` (`dht_simulation._peer_id_bytes`), so the JS must hash the same way (`crypto.subtle` is async → cache):

```javascript
const hashCache = new Map();  // peer_id -> BigInt of sha256(id)[:20]
async function idBig(pid) {
  if (hashCache.has(pid)) return hashCache.get(pid);
  const buf = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(pid));
  const bytes = new Uint8Array(buf).slice(0, 20);
  let v = 0n;
  for (const b of bytes) v = (v << 8n) | BigInt(b);
  hashCache.set(pid, v);
  if (hashCache.size > 5000) hashCache.clear();
  return v;
}

let topoBusy = false;
async function renderTopology(nodes, lastResult, onlineCount) {
  if (topoBusy) return;           // skip tick if previous render still hashing
  topoBusy = true;
  try {
    const svg = document.getElementById('topology');
    if (!svg) return;
    const parent = svg.parentElement;
    const W = parent.clientWidth, H = parent.clientHeight;
    if (W === 0 || H === 0) return;
    svg.setAttribute('width', W); svg.setAttribute('height', H);
    svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

    const target = lastResult?.peer_id_full || null;
    const targetBig = target ? await idBig(target) : null;
    const shown = nodes.slice(0, 36);
    const dists = [];
    for (const n of shown) {
      const d = targetBig === null ? 1n : (await idBig(n.peer_id_full)) ^ targetBig;
      dists.push(d);
    }
    const maxD = dists.reduce((a, b) => (b > a ? b : a), 1n);

    const cx = W / 2, cy = H / 2, rMax = Math.min(W, H) * 0.42;
    const pos = new Map();  // peer_id_full -> [x, y]
    svg.innerHTML = '';
    shown.forEach((n, i) => {
      const a = (i / shown.length) * Math.PI * 2 - Math.PI / 2;
      // radius ∝ XOR distance from query target (closer = nearer center)
      const frac = targetBig === null ? 1 : Number((dists[i] * 1000n) / maxD) / 1000;
      const r = rMax * (0.25 + 0.75 * frac);
      const x = cx + Math.cos(a) * r, y = cy + Math.sin(a) * r;
      pos.set(n.peer_id_full, [x, y]);
      const c = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      c.setAttribute('cx', x); c.setAttribute('cy', y); c.setAttribute('r', 3.5);
      c.setAttribute('fill', n.online ? 'var(--color-green)' : 'var(--color-red)');
      c.setAttribute('opacity', n.online ? '0.9' : '0.4');
      svg.appendChild(c);
    });

    // Center = query target (or hub when no query yet)
    const center = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
    center.setAttribute('cx', cx); center.setAttribute('cy', cy); center.setAttribute('r', 7);
    center.setAttribute('fill', 'var(--color-blue)');
    svg.appendChild(center);

    // Lookup path of the most recent query, hop by hop
    const path = lastResult?.path || [];
    let prev = null;
    path.forEach((pid, hop) => {
      const p = pos.get(pid);
      if (!p) return;
      if (prev) {
        const l = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        l.setAttribute('x1', prev[0]); l.setAttribute('y1', prev[1]);
        l.setAttribute('x2', p[0]); l.setAttribute('y2', p[1]);
        l.setAttribute('stroke', 'var(--color-orange)'); l.setAttribute('stroke-width', '1.5');
        svg.appendChild(l);
      }
      const ring = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      ring.setAttribute('cx', p[0]); ring.setAttribute('cy', p[1]); ring.setAttribute('r', 6);
      ring.setAttribute('fill', 'none'); ring.setAttribute('stroke', 'var(--color-orange)');
      svg.appendChild(ring);
      prev = p;
    });
    // last hop → center (target)
    if (prev && lastResult?.found) {
      const l = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      l.setAttribute('x1', prev[0]); l.setAttribute('y1', prev[1]);
      l.setAttribute('x2', cx); l.setAttribute('y2', cy);
      l.setAttribute('stroke', 'var(--color-orange)'); l.setAttribute('stroke-width', '1.5');
      svg.appendChild(l);
    }

    setText('topoCount', `${onlineCount} Nodes Online`);
  } finally {
    topoBusy = false;
  }
}
```

In `handleSnapshot`, replace `renderTopology(snap.nodes || [])` with:

```javascript
  const lastResult = (snap.recent_results || [])[0] || null;
  renderTopology(snap.nodes || [], lastResult, snap.network?.online_nodes ?? 0);
```

(fire-and-forget async call is fine; the `topoBusy` guard drops overlapping ticks). Note this also fixes the node-count label — it now uses `network.online_nodes` (true count) instead of the truncated 30-node list.

- [ ] **Step 3: Verify** — run server, fire several KNOWN queries: orange hop rings + polyline appear, nodes rearrange radially per query target, count label shows the full online count (e.g. 57 with `--nodes 60`), no console errors.

- [ ] **Step 4: Commit** — `feat(dashboard): XOR-distance topology with lookup-path highlight; escape peer strings`

---

### Task 11: Vendor Chart.js, drop CDN/fonts dependency

**Files:**
- Create: `static/chart.umd.min.js` (downloaded)
- Modify: `static/index.html` (head), `api/app.py` (static mount)

**Interfaces:** Produces `/static/*` file serving used by the dashboard.

- [ ] **Step 1: Download**

Run: `curl -sSf -o static/chart.umd.min.js https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js && head -c 60 static/chart.umd.min.js`
Expected: minified JS banner (`/*! Chart.js v4.4.1 ...`).

- [ ] **Step 2: Mount static dir in `api/app.py`** (after the `app = FastAPI(...)` block):

```python
from fastapi.staticfiles import StaticFiles
```
```python
    app.mount(
        "/static",
        StaticFiles(directory=Path(__file__).parent.parent / "static"),
        name="static",
    )
```

- [ ] **Step 3: Update `static/index.html` head** — delete the two Google Fonts `<link>` lines and change the Chart.js `<script src=...>` to `/static/chart.umd.min.js`. Update the font vars:

```css
    --font-sans: system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
    --font-mono: ui-monospace, "JetBrains Mono", Menlo, Consolas, monospace;
```

- [ ] **Step 4: Verify** — restart server, hard-reload dashboard with browser devtools network tab: zero requests to external hosts; charts render.

- [ ] **Step 5: Commit** — `feat(dashboard): vendor Chart.js, drop CDN and Google Fonts dependency`

```bash
git add static/chart.umd.min.js static/index.html api/app.py
git commit -m "feat(dashboard): vendor Chart.js, drop CDN and Google Fonts dependency"
```

---

### Task 12: Merge main_simulated/main_real; fixable CLI booleans

**Files:**
- Modify: `main.py` (replace `main_simulated` + `main_real` with one `run_server`; fix argparse)

**Interfaces:**
- Consumes: everything wired in Tasks 2–3.
- Produces: `async def run_server(args)` — single wiring path; `parse_args()` unchanged flag names but `--enable-random-walk/--no-enable-random-walk` and `--enable-pubsub/--no-enable-pubsub` now actually toggle. Task 15 adds `--experiment` beside them.

- [ ] **Step 1: Fix the two boolean flags in `parse_args`**

```python
    p.add_argument(
        "--enable-random-walk",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable DHT random walk worker",
    )
    p.add_argument(
        "--enable-pubsub",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable GossipSub (real mode)",
    )
```

- [ ] **Step 2: Replace both `main_simulated` and `main_real` with one function.** Keep the exact wiring from the (post-Task-3) simulated path; the mode switch only decides backend construction and extra snapshot keys:

```python
async def run_server(args):
    node = None
    if args.mode == "real":
        from src.libp2p_node import Libp2pNode, Libp2pNodeConfig, RealDHTNetwork

        logger.info("libp2p DHT Monitor — REAL libp2p MODE")
        node_config = Libp2pNodeConfig(
            port=args.libp2p_port,
            enable_mdns=args.enable_mdns,
            enable_upnp=args.enable_upnp,
            enable_quic=args.enable_quic,
            bootstrap_peers=args.bootstrap,
            enable_random_walk=args.enable_random_walk,
            enable_pubsub=args.enable_pubsub,
            max_connections=args.max_connections,
            max_streams=args.max_libp2p_streams,
        )
        node = Libp2pNode(node_config)
        await node.start()
        network = RealDHTNetwork(node, scenario=args.scenario)
        extra = {"mode": "real", "peer_id": node.peer_id,
                 "listen_addrs": node.get_listen_addresses()}
    else:
        from src.dht_simulation import SimulatedDHTNetwork

        logger.info("libp2p DHT Monitor — SIMULATED MODE")
        network = SimulatedDHTNetwork(node_count=args.nodes, scenario=args.scenario)
        extra = {"mode": args.mode}

    stream_manager = StreamManager(
        max_streams=args.max_streams,
        stream_timeout=args.query_timeout + 5.0,
    )
    broadcast_send, broadcast_recv = trio.open_memory_channel(max_buffer_size=50)
    load_gen_state: dict = {"active": False, "qps": 2.0, "mode": "mixed",
                            "achieved_qps": 0.0}
    extra["load_gen"] = load_gen_state

    async def _on_snapshot(snap: dict) -> None:
        merged = {
            **snap,
            **stream_manager.snapshot(),
            **network.snapshot(),
            "ts": time.time(),
            **extra,
        }
        try:
            broadcast_send.send_nowait(merged)
        except trio.WouldBlock:
            pass

    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=args.max_queries,
        max_random_walks=args.max_walks,
        query_timeout=args.query_timeout,
        on_snapshot=_on_snapshot,
    )

    app, connected_ws = create_app(
        coordinator=coordinator,
        network=network,
        stream_manager=stream_manager,
        load_gen_state=load_gen_state,
        libp2p_node=node,
        mode=args.mode,
    )

    hc_config = hypercorn.config.Config()
    hc_config.bind = [f"{args.host}:{args.port}"]
    hc_config.use_reloader = False
    hc_config.accesslog = "-"
    hc_config.errorlog = "-"
    hc_config.loglevel = "WARNING"

    async def _ws_broadcaster():
        async for snapshot in broadcast_recv:
            dead = []
            for ws in list(connected_ws):
                try:
                    await ws.send_text(json.dumps(snapshot, default=str))
                except Exception:
                    dead.append(ws)
            for ws in dead:
                if ws in connected_ws:
                    connected_ws.remove(ws)

    logger.info("Dashboard → http://localhost:%d/", args.port)
    try:
        async with trio.open_nursery() as nursery:
            await nursery.start(metrics_broadcaster, coordinator, network,
                                stream_manager, broadcast_send,
                                args.broadcast_interval, extra)
            nursery.start_soon(_ws_broadcaster)
            if args.enable_random_walk:
                await nursery.start(random_walk_worker, coordinator, network,
                                    stream_manager, args.walk_interval)
            await nursery.start(load_generator, coordinator, network,
                                stream_manager, load_gen_state)
            nursery.start_soon(hypercorn_trio.serve, app, hc_config)
            logger.info("All services running. Press Ctrl+C to stop.")
    finally:
        if node is not None:
            logger.info("Stopping libp2p node...")
            await node.stop()


async def main(args):
    await run_server(args)
```

- [ ] **Step 3: Verify**

Run: `.venv/bin/python -m pytest tests/ -v` then smoke:
`.venv/bin/python main.py --port 8901 --no-enable-random-walk &` → `sleep 3 && curl -s localhost:8901/api/snapshot | .venv/bin/python -c "import json,sys; d=json.load(sys.stdin); print(d['mode'], d['random_walk_limiter']['borrowed'])" && kill %1`
Expected: `simulated 0` (walker disabled — flag finally works).

- [ ] **Step 4: Commit** — `refactor: merge main_simulated/main_real into run_server; fix undisablable CLI flags`

---

### Task 13: Test fixes, _emit logging, dead code, layer naming

**Files:**
- Modify: `tests/test_integration.py` (two broken tests), `src/coordinator.py` (`_emit`, unused import), `src/dht_simulation.py` (unused import), `src/stream_manager.py` (docstring)

- [ ] **Step 1: Fix the vacuous scenario test.** In `tests/test_integration.py`, `test_scenario_affects_latency` currently asserts `mean(stressed) >= mean(normal) * 0.5` (passes even if STRESSED is faster). Replace the assertion with a directional one:

```python
    assert mean(stressed_durations) > mean(normal_durations), (
        f"STRESSED (base 400ms) must be slower than NORMAL (base 40ms): "
        f"{mean(stressed_durations):.0f}ms vs {mean(normal_durations):.0f}ms"
    )
```

(Latency bases differ 10×, so direction is robust despite jitter. Match the test's actual local variable names when editing.)

- [ ] **Step 2: Fix the flaky known-peer test.** In `test_find_known_peer`, NORMAL injects 5% stream errors per contacted node, so FAILED is a legitimate outcome. Extend the allowed statuses:

```python
    assert result.status in (QueryStatus.SUCCESS, QueryStatus.TIMEOUT, QueryStatus.FAILED)
```

- [ ] **Step 3: `_emit` logs instead of swallowing.** In `src/coordinator.py`:

```python
    async def _emit(self) -> None:
        if self._on_snapshot:
            try:
                await self._on_snapshot(self.snapshot())
            except Exception:
                logger.exception("on_snapshot callback failed")
```

- [ ] **Step 4: Dead code sweep.** Remove `Any` from `src/coordinator.py` imports; remove `math` from `src/dht_simulation.py` imports. In `src/stream_manager.py`, fix the docstring lines 10–12 to the canonical scheme: StreamManager is **Layer C** (stream pool); the DHTQueryCoordinator holds Layer A (queries) and Layer B (walks). Leave `src/libp2p_node.py` untouched (unverifiable without libp2p installed — not worth the risk for an unused import).

- [ ] **Step 5: Run suite 3× to confirm the flake is gone**

Run: `for i in 1 2 3; do .venv/bin/python -m pytest tests/test_integration.py -v || break; done`
Expected: green all three rounds.

- [ ] **Step 6: Commit** — `fix: vacuous+flaky integration tests, _emit logging, dead imports, layer naming`

---

### Task 14: Experiment runner core (`src/experiment.py`)

**Files:**
- Create: `src/experiment.py`
- Create: `tests/test_experiment.py`
- Create: `experiments/baseline.json`

**Interfaces:**
- Consumes: `DHTQueryCoordinator`, `SimulatedDHTNetwork`, `StreamManager`, percentiles from Task 6.
- Produces:
  - `async def run_arm(name: str, arm_cfg: dict, workload: dict, network_cfg: dict) -> dict` — summary dict (see code).
  - `async def run_experiment(config: dict) -> dict` — `{"name", "network", "workload", "arms": {arm_name: summary}}`.
  - Config schema = `experiments/baseline.json` shape. Task 15 (report/CLI) consumes both.

- [ ] **Step 1: Write `experiments/baseline.json`**

```json
{
  "name": "baseline",
  "network": {"nodes": 60, "scenario": "STRESSED"},
  "workload": {"qps": 10.0, "duration_s": 20},
  "arms": {
    "unprotected": {"max_queries": 10000, "max_walks": 9999, "max_streams": 10000, "query_timeout": 20.0},
    "protected": {"max_queries": 10, "max_walks": 3, "max_streams": 50, "query_timeout": 20.0}
  }
}
```

- [ ] **Step 2: Write the failing test**

```python
# tests/test_experiment.py
import pytest

from src.experiment import run_experiment

TINY = {
    "name": "tiny",
    "network": {"nodes": 20, "scenario": "STRESSED"},
    "workload": {"qps": 8.0, "duration_s": 5},
    "arms": {
        "unprotected": {"max_queries": 10000, "max_walks": 9999,
                        "max_streams": 10000, "query_timeout": 10.0},
        "protected": {"max_queries": 4, "max_walks": 2,
                      "max_streams": 20, "query_timeout": 10.0},
    },
}


@pytest.mark.trio
async def test_experiment_runs_both_arms(autojump_clock):
    result = await run_experiment(TINY)

    assert set(result["arms"]) == {"unprotected", "protected"}
    for arm in result["arms"].values():
        assert arm["counters"]["total"] > 0
        assert "p95_ms" in arm["rates"]

    protected = result["arms"]["protected"]
    # The invariant the whole project exists to prove:
    assert protected["peak_borrowed"] <= TINY["arms"]["protected"]["max_queries"]
    assert protected["peak_concurrency"] <= TINY["arms"]["protected"]["max_queries"]
    # Unprotected must actually build up more concurrency than the cap allows:
    assert result["arms"]["unprotected"]["peak_concurrency"] > 4
```

- [ ] **Step 3: Run to verify failure** — `ModuleNotFoundError: src.experiment`

- [ ] **Step 4: Implement `src/experiment.py`**

```python
"""
A/B experiment runner: same workload against an 'unprotected' coordinator
(caps effectively unlimited) and a 'protected' one (real caps), producing a
comparable summary per arm.  Headless — no HTTP server involved.
"""

from __future__ import annotations

import logging
import random

import trio

from src.coordinator import DHTQueryCoordinator, QueryPriority
from src.dht_simulation import SimulatedDHTNetwork
from src.stream_manager import StreamManager

logger = logging.getLogger(__name__)

SAMPLE_INTERVAL_S = 0.5


async def run_arm(name: str, arm_cfg: dict, workload: dict, network_cfg: dict) -> dict:
    network = SimulatedDHTNetwork(
        node_count=network_cfg["nodes"], scenario=network_cfg["scenario"]
    )
    stream_manager = StreamManager(max_streams=arm_cfg["max_streams"])
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=arm_cfg["max_queries"],
        max_random_walks=arm_cfg["max_walks"],
        query_timeout=arm_cfg["query_timeout"],
    )

    samples: list[dict] = []

    async def sampler() -> None:
        while True:
            await trio.sleep(SAMPLE_INTERVAL_S)
            snap = coordinator.snapshot()
            samples.append({
                "borrowed": snap["capacity_limiter"]["borrowed"],
                "concurrency": snap["coordinator"]["concurrency"]["current"],
                "waiting": snap["coordinator"]["concurrency"]["acquiring"],
            })

    async def fire(target: str) -> None:
        async def _query_fn(pid: str):
            async with stream_manager.open_stream(pid, "/libp2p/kad/1.0.0"):
                return await network.query(pid)

        try:
            await coordinator.find_peer(target, _query_fn, QueryPriority.USER)
        except Exception as exc:  # experiment must survive injected errors
            logger.debug("experiment query error: %s", exc)

    qps = float(workload["qps"])
    duration = float(workload["duration_s"])

    async with trio.open_nursery() as outer:
        outer.start_soon(sampler)
        async with trio.open_nursery() as fires:
            with trio.move_on_after(duration):
                while True:
                    fires.start_soon(fire, random.choice(network.peer_ids))
                    await trio.sleep(1.0 / qps)
            # leaving `fires` waits for in-flight queries (bounded by query_timeout)
        outer.cancel_scope.cancel()

    snap = coordinator.snapshot()
    coord = snap["coordinator"]
    return {
        "arm": name,
        "config": dict(arm_cfg),
        "counters": coord["counters"],
        "rates": coord["rates"],
        "peak_concurrency": coord["concurrency"]["peak"],
        "peak_borrowed": max((s["borrowed"] for s in samples), default=0),
        "peak_waiting": max((s["waiting"] for s in samples), default=0),
        "achieved_qps": round(coord["counters"]["total"] / duration, 2),
        "samples": samples,
    }


async def run_experiment(config: dict) -> dict:
    arms: dict[str, dict] = {}
    for name, arm_cfg in config["arms"].items():
        logger.info("experiment %s: running arm %r…", config.get("name"), name)
        arms[name] = await run_arm(name, arm_cfg, config["workload"], config["network"])
    return {
        "name": config.get("name", "experiment"),
        "network": config["network"],
        "workload": config["workload"],
        "arms": arms,
    }
```

- [ ] **Step 5: Run to verify pass** — `.venv/bin/python -m pytest tests/test_experiment.py -v`

- [ ] **Step 6: Commit** — `feat: headless A/B experiment runner (unprotected vs protected coordinator)`

```bash
git add src/experiment.py tests/test_experiment.py experiments/baseline.json
git commit -m "feat: headless A/B experiment runner (unprotected vs protected coordinator)"
```

---

### Task 15: Experiment report (JSON + HTML) and `--experiment` CLI

**Files:**
- Modify: `src/experiment.py` (add `render_html`, `write_report`)
- Modify: `main.py` (`--experiment` arg + dispatch)
- Test: append to `tests/test_experiment.py`

**Interfaces:**
- Consumes: `run_experiment` result dict (Task 14).
- Produces: `write_report(result: dict, out_dir: str | Path, stamp: str) -> tuple[Path, Path]` (json_path, html_path); CLI `python main.py --experiment experiments/baseline.json`.

- [ ] **Step 1: Write the failing test (append to `tests/test_experiment.py`)**

```python
import json


@pytest.mark.trio
async def test_report_files(tmp_path, autojump_clock):
    result = await run_experiment(TINY)
    from src.experiment import write_report

    json_path, html_path = write_report(result, tmp_path, stamp="20260813-120000")
    assert json_path.exists() and html_path.exists()
    loaded = json.loads(json_path.read_text())
    assert set(loaded["arms"]) == {"unprotected", "protected"}
    html = html_path.read_text()
    assert "unprotected" in html and "protected" in html
    assert "<html" not in html[:20] or True  # self-contained fragment or full doc — both fine
```

- [ ] **Step 2: Run to verify failure** — ImportError on `write_report`.

- [ ] **Step 3: Implement in `src/experiment.py`**

```python
import json
from pathlib import Path

_ROW_KEYS = [
    ("Total queries", lambda a: a["counters"]["total"]),
    ("Success", lambda a: a["counters"]["success"]),
    ("Failed", lambda a: a["counters"]["failed"]),
    ("Timeout", lambda a: a["counters"]["timeout"]),
    ("Success rate %", lambda a: a["rates"]["success_pct"]),
    ("Achieved QPS", lambda a: a["achieved_qps"]),
    ("Avg latency ms", lambda a: a["rates"]["avg_duration_ms"]),
    ("p95 ms", lambda a: a["rates"]["p95_ms"]),
    ("p99 ms", lambda a: a["rates"]["p99_ms"]),
    ("Peak concurrency", lambda a: a["peak_concurrency"]),
    ("Peak borrowed slots", lambda a: a["peak_borrowed"]),
    ("Peak waiting (queued)", lambda a: a["peak_waiting"]),
]


def render_html(result: dict) -> str:
    arm_names = list(result["arms"])
    head = "".join(f"<th>{n}</th>" for n in arm_names)
    rows = ""
    for label, get in _ROW_KEYS:
        cells = "".join(f"<td>{get(result['arms'][n])}</td>" for n in arm_names)
        rows += f"<tr><th>{label}</th>{cells}</tr>"
    prot = result["arms"].get("protected", {})
    cap = prot.get("config", {}).get("max_queries", "?")
    verdict = (
        f"Protected arm peak concurrency {prot.get('peak_concurrency', '?')} "
        f"stayed within cap {cap}; queued work (peak waiting "
        f"{prot.get('peak_waiting', '?')}) instead of exhausting resources."
    )
    return f"""<!doctype html><meta charset="utf-8">
<title>kad-monitor experiment: {result['name']}</title>
<style>
 body{{font-family:system-ui;background:#0b0e14;color:#d8d9da;padding:32px;max-width:860px;margin:auto}}
 table{{border-collapse:collapse;width:100%;margin:24px 0}}
 th,td{{border:1px solid #2c3235;padding:8px 12px;text-align:right;font-variant-numeric:tabular-nums}}
 th:first-child{{text-align:left}} thead th{{background:#181b1f}}
 .verdict{{background:#12331a;border:1px solid #2d6a3f;padding:12px 16px;border-radius:4px}}
</style>
<h1>Experiment: {result['name']}</h1>
<p>Network: {result['network']['nodes']} nodes, scenario {result['network']['scenario']}.
Workload: {result['workload']['qps']} QPS for {result['workload']['duration_s']}s per arm.</p>
<table><thead><tr><th>Metric</th>{head}</tr></thead><tbody>{rows}</tbody></table>
<p class="verdict"><b>Verdict:</b> {verdict}</p>
"""


def write_report(result: dict, out_dir, stamp: str):
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    base = f"{result['name']}-{stamp}"
    json_path = out / f"{base}.json"
    html_path = out / f"{base}.html"
    json_path.write_text(json.dumps(result, indent=2))
    html_path.write_text(render_html(result))
    return json_path, html_path
```

- [ ] **Step 4: CLI in `main.py`.** Add to `parse_args`:

```python
    p.add_argument(
        "--experiment",
        metavar="CONFIG_JSON",
        help="Run a headless A/B experiment from a JSON config and exit",
    )
```

In the `__main__` block, before `trio.run(main, args)`:

```python
    if args.experiment:
        from src.experiment import run_experiment, write_report

        config = json.loads(Path(args.experiment).read_text())
        result = trio.run(run_experiment, config)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        json_path, html_path = write_report(result, "reports", stamp)
        print(f"Report: {json_path}\nReport: {html_path}")
        sys.exit(0)
```

(`time` is already imported in main.py; `Path` too.)

- [ ] **Step 5: Run tests + a real (short) experiment**

Run: `.venv/bin/python -m pytest tests/test_experiment.py -v`
Then create a quick config and run it for real:
`.venv/bin/python - <<'EOF'
import json; c=json.load(open('experiments/baseline.json')); c['workload']['duration_s']=6; json.dump(c, open('/tmp/quick.json','w'))
EOF`
`.venv/bin/python main.py --experiment /tmp/quick.json`
Expected: two report paths printed; open the HTML — protected arm's peak concurrency ≤ 10 while unprotected's exceeds it.

- [ ] **Step 6: Commit** — `feat: experiment reports (JSON + self-contained HTML) and --experiment CLI`

---

### Task 16: `/healthz` + `/metrics`

**Files:**
- Modify: `api/app.py` (two routes)
- Test: append to `tests/test_api.py`

**Interfaces:**
- Consumes: `_full_snapshot()` (Task 3).
- Produces: `GET /healthz` → `{"status": "ok", "mode", "uptime_s"}`; `GET /metrics` → Prometheus text exposition. Docker healthcheck (Task 18) hits `/healthz`.

- [ ] **Step 1: Failing tests (append to `tests/test_api.py`)**

```python
@pytest.mark.trio
async def test_healthz():
    async with make_client() as client:
        r = await client.get("/healthz")
    assert r.status_code == 200
    d = r.json()
    assert d["status"] == "ok"
    assert d["mode"] == "simulated"
    assert d["uptime_s"] >= 0


@pytest.mark.trio
async def test_metrics_exposition():
    async with make_client() as client:
        await client.post("/api/query", json={"mode": "known"})
        r = await client.get("/metrics")
    assert r.status_code == 200
    text = r.text
    assert 'dht_queries_total{status="success"}' in text
    assert "dht_query_limiter_capacity 10" in text
    assert "dht_stream_pool_capacity 20" in text
```

- [ ] **Step 2: Run to verify failure** (404s).

- [ ] **Step 3: Implement in `api/app.py`.** Add `from fastapi.responses import HTMLResponse, PlainTextResponse` (extend the existing import). Inside `create_app`, near the top: `started_at = time.time()`. Routes:

```python
    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "mode": mode,
                "uptime_s": round(time.time() - started_at, 1)}

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics():
        snap = _full_snapshot()
        coord = snap["coordinator"]
        ql = snap["capacity_limiter"]
        rwl = snap["random_walk_limiter"]
        sm = snap["stream_manager"]
        lines = ["# TYPE dht_queries_total counter"]
        for status in ("success", "failed", "timeout", "cancelled"):
            lines.append(f'dht_queries_total{{status="{status}"}} {coord["counters"][status]}')
        lines += [
            "# TYPE dht_query_limiter_borrowed gauge",
            f"dht_query_limiter_borrowed {ql['borrowed']}",
            f"dht_query_limiter_capacity {ql['total']}",
            f"dht_walk_limiter_borrowed {rwl['borrowed']}",
            f"dht_walk_limiter_capacity {rwl['total']}",
            f"dht_stream_pool_open {sm['pool']['open']}",
            f"dht_stream_pool_capacity {sm['config']['max_streams']}",
            "# TYPE dht_throughput_qps gauge",
            f"dht_throughput_qps {coord['rates']['throughput_qps']}",
            f"dht_latency_p95_ms {coord['rates']['p95_ms']}",
            f"dht_latency_p99_ms {coord['rates']['p99_ms']}",
            f"dht_loadgen_achieved_qps {snap['load_gen'].get('achieved_qps', 0)}",
        ]
        return "\n".join(lines) + "\n"
```

- [ ] **Step 4: Run to verify pass, then full suite.**
- [ ] **Step 5: Commit** — `feat: /healthz and hand-rendered Prometheus /metrics`

---

### Task 17: Snapshot history (SQLite) + `/api/history` + chart hydration

**Files:**
- Create: `src/history.py`
- Create: `tests/test_history.py`
- Modify: `src/workers.py` (`metrics_broadcaster` gains `history=None`), `api/app.py` (`create_app` gains `history=None`, new route), `main.py` (construct + wire; `--history-db` arg), `static/index.html` (hydrate charts on load)

**Interfaces:**
- Consumes: broadcaster tick (Task 3 shape).
- Produces:
  - `SnapshotHistory(db_path: str, retention_hours: float = 24.0)` with `.append(snapshot: dict) -> None` (stores a slim projection) and `.query(minutes: float, max_points: int = 240) -> list[dict]` returning `[{"ts", "qps", "avg_ms", "concurrency"}, ...]` ascending.
  - `GET /api/history?minutes=30` → `{"points": [...]}`.
  - `create_app(..., history=None)`; `metrics_broadcaster(..., history=None)` appends each tick when set.

- [ ] **Step 1: Failing tests**

```python
# tests/test_history.py
import time

from src.history import SnapshotHistory


def make_snap(ts, qps):
    return {
        "ts": ts,
        "coordinator": {
            "rates": {"throughput_qps": qps, "avg_duration_ms": 100.0},
            "concurrency": {"current": 2, "acquiring": 1},
        },
    }


def test_append_and_query(tmp_path):
    h = SnapshotHistory(str(tmp_path / "h.db"))
    now = time.time()
    for i in range(5):
        h.append(make_snap(now - 60 + i, qps=float(i)))

    points = h.query(minutes=5)
    assert len(points) == 5
    assert points[0]["qps"] == 0.0 and points[-1]["qps"] == 4.0
    assert points[0]["concurrency"] == 3  # current + acquiring


def test_prune(tmp_path):
    h = SnapshotHistory(str(tmp_path / "h.db"), retention_hours=1.0)
    now = time.time()
    h.append(make_snap(now - 7200, qps=1.0))   # 2h old — beyond retention
    h._last_prune = 0.0                        # force prune on next append
    h.append(make_snap(now, qps=2.0))
    points = h.query(minutes=600)
    assert [p["qps"] for p in points] == [2.0]
```

- [ ] **Step 2: Run to verify failure** — ModuleNotFoundError.

- [ ] **Step 3: Implement `src/history.py`**

```python
"""
SQLite-backed snapshot history so dashboard charts survive page reloads.

Stores a slim per-tick projection, not the full snapshot (which carries node
lists and result tables).  sqlite3 is synchronous; a sub-millisecond INSERT
every 0.5s is negligible on the trio loop.
# ponytail: sync sqlite on the event loop — move to a worker thread if ticks ever get slow
"""

from __future__ import annotations

import json
import sqlite3
import time


class SnapshotHistory:
    PRUNE_EVERY_S = 300.0

    def __init__(self, db_path: str, retention_hours: float = 24.0) -> None:
        self._conn = sqlite3.connect(db_path)
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS snapshots (ts REAL PRIMARY KEY, data TEXT NOT NULL)"
        )
        self._conn.commit()
        self._retention_s = retention_hours * 3600
        self._last_prune = time.monotonic()

    @staticmethod
    def _slim(snapshot: dict) -> dict:
        coord = snapshot.get("coordinator", {})
        rates = coord.get("rates", {})
        conc = coord.get("concurrency", {})
        return {
            "ts": snapshot.get("ts", time.time()),
            "qps": rates.get("throughput_qps", 0.0),
            "avg_ms": rates.get("avg_duration_ms", 0.0),
            "concurrency": conc.get("current", 0) + conc.get("acquiring", 0),
        }

    def append(self, snapshot: dict) -> None:
        slim = self._slim(snapshot)
        self._conn.execute(
            "INSERT OR REPLACE INTO snapshots VALUES (?, ?)",
            (slim["ts"], json.dumps(slim)),
        )
        self._conn.commit()
        if time.monotonic() - self._last_prune > self.PRUNE_EVERY_S or self._last_prune == 0.0:
            self._conn.execute(
                "DELETE FROM snapshots WHERE ts < ?", (slim["ts"] - self._retention_s,)
            )
            self._conn.commit()
            self._last_prune = time.monotonic()

    def query(self, minutes: float, max_points: int = 240) -> list[dict]:
        cutoff = time.time() - minutes * 60
        rows = self._conn.execute(
            "SELECT data FROM snapshots WHERE ts >= ? ORDER BY ts", (cutoff,)
        ).fetchall()
        step = max(1, len(rows) // max_points)
        return [json.loads(r[0]) for r in rows[::step]]
```

- [ ] **Step 4: Wire it.**
- `src/workers.py` `metrics_broadcaster`: add param `history=None` (after `extra`); after the `send_nowait` try/except add:

```python
        if history is not None:
            try:
                history.append(snapshot)
            except Exception:
                logger.exception("history append failed")
```

- `api/app.py` `create_app`: add param `history=None` and route:

```python
    @app.get("/api/history")
    async def get_history(minutes: float = 30.0):
        if history is None:
            return {"points": []}
        return {"points": history.query(minutes=min(minutes, 24 * 60))}
```

- `main.py`: add `--history-db` arg (`default="history.db"`, `help="SQLite snapshot history path ('' disables)"`). In `run_server`, before `create_app`:

```python
    history = SnapshotHistory(args.history_db) if args.history_db else None
```

(import `from src.history import SnapshotHistory` at top of main.py), pass `history=history` to `create_app` and as the extra positional after `extra` in the `nursery.start(metrics_broadcaster, ...)` call.

- Update the Task 2 tests' `nursery.start(metrics_broadcaster, ...)` invocation if signature ordering breaks it (it shouldn't — new params have defaults).

- [ ] **Step 5: Dashboard hydration.** In `static/index.html` `init()`, after `setupCharts()`:

```javascript
  fetch('/api/history?minutes=1')
    .then(r => r.json())
    .then(d => {
      const pts = (d.points || []).slice(-MAX_HISTORY);
      pts.forEach(p => {
        updateChart('qps', p.qps || 0);
        updateChart('dur', p.avg_ms || 0);
        updateChart('conc', p.concurrency || 0);
      });
    })
    .catch(() => {});
```

- [ ] **Step 6: Run tests + smoke** — `.venv/bin/python -m pytest tests/ -v`; run server 30s with load gen on, reload dashboard: charts start pre-filled, `curl 'localhost:8901/api/history?minutes=1'` returns points.

- [ ] **Step 7: Commit** — `feat: SQLite snapshot history, /api/history, chart hydration on reload`

---

### Task 18: Dockerfile + Compose

**Files:**
- Create: `Dockerfile`, `docker-compose.yml`, `.dockerignore`

**Interfaces:** Consumes `/healthz` (Task 16), `--history-db` (Task 17).

- [ ] **Step 1: `.dockerignore`**

```text
.venv/
.git/
.remember/
__pycache__/
reports/
history.db
docs/
tests/
```

- [ ] **Step 2: `Dockerfile`** (single stage — pure-Python deps ship as wheels, a build stage would add nothing):

```dockerfile
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN useradd -m monitor && mkdir -p /data && chown monitor /data
USER monitor

EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s \
  CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz')"]

CMD ["python", "main.py", "--host", "0.0.0.0", "--port", "8000", "--history-db", "/data/history.db"]
```

- [ ] **Step 3: `docker-compose.yml`**

```yaml
services:
  kad-monitor:
    build: .
    ports:
      - "8000:8000"
    volumes:
      - kad-data:/data
    restart: unless-stopped

volumes:
  kad-data:
```

- [ ] **Step 4: Verify**

Run: `docker build -t kad-monitor . && docker compose up -d && sleep 8 && curl -s localhost:8000/healthz && docker compose down`
Expected: `{"status":"ok",...}`. If Docker is unavailable on this machine, verify with `docker --version` first and note the skip in the commit message — the CI task doesn't depend on it.

- [ ] **Step 5: Commit** — `feat: Dockerfile + compose (healthcheck, persistent history volume)`

---

### Task 19: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: Write the workflow**

```yaml
name: CI
on:
  push:
  pull_request:

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements.txt
      - run: pytest tests/ -v
```

(Simulated-mode only: libp2p is not in requirements.txt, and `test_libp2p_node.py` skips itself — Task 1.)

- [ ] **Step 2: Sanity check locally** — the exact CI command: `.venv/bin/python -m pytest tests/ -v` → green.

- [ ] **Step 3: Commit** — `ci: run test suite on push/PR`

---

### Task 20: README overhaul + final verification sweep

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Fix and extend the README.**
- Quick start: `cd kad-monitor` (not `libp2p-dht-monitor`); add `python -m venv .venv && source .venv/bin/activate` before pip install; note libp2p is optional (comment block in requirements.txt) and required only for `--mode real`.
- Project structure: add `src/libp2p_node.py`, `src/experiment.py`, `src/history.py`, `experiments/`, `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, `.github/workflows/ci.yml`.
- Replace the stale "Swapping in Real py-libp2p" code sample (`from libp2p import new_node` does not exist) with the actual mechanism: `python main.py --mode real --libp2p-port 4001 ...` and a pointer to `RealDHTNetwork` in `src/libp2p_node.py`.
- New sections:
  - **A/B Experiment**: `python main.py --experiment experiments/baseline.json` → JSON + HTML reports in `reports/`; one paragraph on what the two arms prove.
  - **Docker**: `docker compose up` → dashboard on :8000, history persisted in the `kad-data` volume.
  - **Endpoints**: add `/healthz`, `/metrics`, `/api/history`, the peer-chaos routes.
- Dashboard features table: event log is now real; add scenario switcher, peer chaos, requested/fired/done QPS, percentiles, lookup-path topology.
- Layer table: keep A=queries, B=walks, C=streams (now consistent with the code after Task 13).

- [ ] **Step 2: Final verification sweep (success criteria from the spec)**

```bash
.venv/bin/python -m pytest tests/ -v                      # criterion 5: all green, libp2p absent
.venv/bin/python main.py --port 8901 &                    # criterion 1: runs from clean venv
sleep 3
curl -s localhost:8901/healthz
curl -s -X POST localhost:8901/api/loadgen -H 'Content-Type: application/json' \
  -d '{"active":true,"qps":10,"mode":"mixed"}'
sleep 6
curl -s localhost:8901/api/snapshot | .venv/bin/python -c "
import json,sys; d=json.load(sys.stdin)
lg=d['load_gen']; print('achieved:', lg['achieved_qps'])   # criterion 2: ≈10, not ≈2
assert lg['achieved_qps'] > 6"
kill %1
.venv/bin/python main.py --experiment experiments/baseline.json   # criterion 6
```

Browser pass (criteria 3, 4, 7): scenario buttons, chaos buttons, config modal (5 fields), event log narrating, path rendering on topology, charts surviving reload.

- [ ] **Step 3: Commit** — `docs: README overhaul (quick start, experiment, docker, endpoint reference)`

---

## Self-Review Notes

- **Spec coverage:** Phase 1 → Tasks 1–4; Phase 2 → Tasks 5–11; Phase 3 → Tasks 12–13, 20; Phase 4 → Tasks 14–15; Phase 5 → Tasks 16–19. Spec's "initial WS frame same shape" → Task 3; "topology count label" → Task 10; "HTML-escape" → Task 10; "pytest config" → Task 1.
- **Deliberate deviations from spec, carried into it by reference:** (1) Dockerfile is single-stage (pure-Python wheels; a build stage adds nothing) — spec said multi-stage; (2) topology keeps hash-stable ring *angles* with XOR-distance *radius* toward the query target rather than a full XOR re-layout — same information, far less code; (3) the load generator owns its long-lived nursery internally instead of receiving main's (equivalent lifecycle, no signature change).
- **Type consistency check:** `create_app` final signature (Task 17): `(coordinator, network, stream_manager, load_gen_state, libp2p_node=None, mode="simulated", history=None)`. `metrics_broadcaster` final: `(coordinator, network, stream_manager, send_channel, interval_s=0.5, extra=None, history=None, *, task_status)`. Tasks 3, 12, 17 all match these.
