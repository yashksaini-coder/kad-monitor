# kad-monitor: Demo Harness Completion — Design

**Date:** 2026-08-13
**Status:** Approved (direction and design approved in brainstorming session; extended same day with Phases 4–5 — experiment runner, CI, deployment, persistence — after from-scratch goals review)
**Direction:** Complete the simulation-based demo harness end to end, then make it a deployable, self-proving product. Real-libp2p deep work is explicitly out of scope.

## Context

kad-monitor demonstrates that dual-layer `trio.CapacityLimiter` back-pressure prevents DHT resource exhaustion. A codebase audit (5-agent review + live end-to-end verification) found:

- **P0:** `python main.py` crashes at import. `src/__init__.py` unconditionally imports `src/libp2p_node.py`, whose module-level `from libp2p import ...` requires `libp2p==0.6.0` — which does not install on Python 3.14 (coincurve build failure). Simulated mode is advertised as dependency-light but is unrunnable.
- **P0:** The load generator cannot generate load. `src/workers.py:131-138` uses a per-iteration `async with trio.open_nursery()` believing it fires-and-forgets; trio nurseries wait on exit. Verified: 10 QPS requested → ~2.1 QPS achieved, peak concurrency 2. The project's central demo (back-pressure under overlapping load) is untriggerable from the UI.
- The dashboard calls only 3 endpoints + `/ws` (~15% of the API surface). Scenario switching — a README-advertised interactive control — is display-only. The "Events & System Logs" panel is styled but permanently empty.
- Zero test coverage of `api/app.py`, `main.py`, and `workers.py` (which is why both P0s survived). `httpx` is already in requirements, unused.
- Dead code: a startup broadcaster in `api/app.py:117-135` that cancels itself immediately (and would steal snapshots from `main.py`'s working duplicate if it ever ran); `main_simulated`/`main_real` are ~80% copy-paste.

## Goals

1. `python main.py` runs out of the box in simulated mode with only trio/fastapi/hypercorn installed.
2. The load generator actually saturates the coordinator; the dashboard shows requested vs. achieved QPS so back-pressure is visible.
3. Every simulated-mode capability the backend has is reachable from the dashboard.
4. The gaps that let the P0s survive are closed with tests.
5. **The harness proves its own thesis:** a scripted A/B experiment (coordinator caps off vs. on, same workload) produces a reproducible before/after report.
6. **One-command deployment:** `docker compose up` serves the dashboard on any host; CI runs the full suite on every push.

## Non-Goals (YAGNI)

- Real-mode integrity work: real hop counting, pubsub receive loop, binding StreamManager to real libp2p streams. (Future "Direction B".)
- Solving the libp2p-on-Python-3.14 install problem. Real mode stays code-complete but optional and unverified.
- Public always-on hosted instance (auth, uptime, abuse concerns). Compose-on-any-host is the ceiling.
- Kubernetes or multi-container orchestration. One container is the right size for this tool.

## Architecture (unchanged)

Single trio event loop: `DHTQueryCoordinator` + `SimulatedDHTNetwork` + background workers + Hypercorn serving FastAPI, WebSocket snapshot push every 0.5s, single-file dashboard (`static/index.html`). We fix wiring, not architecture. `libp2p_node.py` stays in the tree but is imported lazily, only on `--mode real`.

## Phase 1 — Make it run, make load real

### 1.1 Startup fix
- Remove the `libp2p_node` re-export from `src/__init__.py`.
- `main.py` imports `Libp2pNode`/`RealDHTNetwork` inside `main_real()` only, with a clear error message if libp2p is missing.
- `requirements.txt`: drop `pytest-asyncio` (all-trio repo); move `libp2p==0.6.0` to a commented optional extra with a note about the Python-version constraint.

### 1.2 Load generator fix
- `load_generator(...)` receives the long-lived nursery from `main.py` and uses `nursery.start_soon(_fire)` — true fire-and-forget. Delete the per-iteration nursery and the false "nursery will cancel" comment. Remove the duplicate `import hashlib` and the no-op `current_trio_token()` call.
- Loadgen state gains `achieved_qps`: a rolling count of queries actually fired in the last few seconds, computed in the worker, included in the metrics snapshot alongside the requested `qps`.

### 1.3 Single broadcaster
- Delete `api/app.py`'s dead `_ws_broadcaster`/`_start_broadcaster` (lines 117-135) and the unused `broadcast_send` parameter of `create_app`. `main.py`'s broadcaster is the single fan-out.
- Fix the initial WS frame (`api/app.py:329-335`) to have the same shape as pushed frames (include `mode`, and in real mode `peer_id`/`listen_addrs`) by building it through the same snapshot-merge helper `main.py` uses.

### 1.4 Test harness
- New `tests/test_api.py`: httpx `ASGITransport` against `create_app` with the simulated backend. Covers: `/api/snapshot` shape, `/api/query` success path, `/api/config` hot-reload, `/api/loadgen` set/get roundtrip, `/api/network/scenario`, the new peer-chaos routes, and `/ws` first-frame shape.
- New `tests/test_workers.py`: the regression test the loadgen bug deserved — run the generator at high QPS against a slow fake coordinator and assert peak concurrency > 1 and achieved QPS ≈ requested; plus a metrics_broadcaster tick test.
- `tests/test_libp2p_node.py` gets a module-level `pytest.importorskip("libp2p")` so the suite collects cleanly without libp2p.
- Add pytest config (`pyproject.toml` or `pytest.ini`): trio mode, test paths.

## Phase 2 — Dashboard completion

All items are UI wiring against existing endpoints unless noted.

### 2.1 Scenario switcher
Four buttons (NORMAL / DEGRADED / STRESSED / SATURATED) → existing `POST /api/network/scenario`; active scenario highlighted from the snapshot. Note in the UI that switching rebuilds the simulated network (peer IDs change — existing behavior).

### 2.2 Peer chaos panel
Buttons: add peer (existing `POST /api/network/peer`), remove peer, toggle peer online/offline. **New backend:** two small routes wrapping the already-implemented `SimulatedDHTNetwork.remove_peer` / `toggle_peer_online`; 400 in real mode like the other sim-only routes.

### 2.3 Event log (fills the dead panel)
Client-side only: derive events from WS snapshot deltas — query completed/failed/timed out, scenario changed, config reloaded, WS connect/reconnect, limiter utilisation crossing a saturation threshold (e.g. ≥90%). Timestamped, bounded ring (last ~200), newest first.

### 2.4 Load-gen truthfulness
- On page load, `GET /api/loadgen` resyncs the button/slider state (fixes desync after reload).
- Display requested vs. achieved QPS side by side — the "is back-pressure biting?" readout, fed by 1.2.

### 2.5 Config modal completion
Expose all 5 `ConfigRequest` fields (adds `max_streams`, `stream_timeout`).

### 2.6 Latency percentiles
p50/p95/p99 computed server-side in `coordinator.snapshot()` from the existing duration history, rendered next to the average.

### 2.7 Lookup-path visualization (only feature with real backend change)
- `SimulatedDHTNetwork.query` records the actual iterative-lookup path (ordered list of contacted peer IDs); returned alongside `(found, closest, hops)`; `QueryResult` gains `path`.
- Topology view: position nodes by XOR distance from the most recent query's target (instead of the current decorative ring) and highlight that query's route hop by hop.
- Fix the node-count label to use `network.online_nodes` (true count) instead of the truncated 30-node list.
- Real mode: `path` is empty; topology falls back to current behavior.

### 2.8 Output hygiene in the UI
HTML-escape peer IDs and any server-derived strings in the two `innerHTML` render paths (results table, live queries) — cheap XSS hardening.

## Phase 3 — Hygiene

- Merge `main_simulated`/`main_real` into one `main(...)` with a backend-construction switch; single copy of the broadcaster/nursery wiring.
- CLI: `--enable-random-walk`/`--enable-pubsub` become `argparse.BooleanOptionalAction` so they can actually be disabled.
- Tests: fix `test_scenario_affects_latency` (assert STRESSED mean > NORMAL mean with margin, seeded/repeated to be stable); fix flaky `test_find_known_peer` (allow FAILED — NORMAL injects 5% stream errors by design).
- Layer naming settled on the README's scheme everywhere: **A = query limiter, B = walk sub-limiter, C = stream pool**. Fix contradicting docstrings in `coordinator.py` and `stream_manager.py`.
- `coordinator._emit()` logs exceptions (module logger) instead of bare `except: pass`.
- Vendor Chart.js into `static/` (kill the CDN + Google Fonts hard dependency so the dashboard works offline).
- Dead code sweep: `_dht_cancel_scope`, unused imports (`Any`, `math`, `time`), deprecated `@app.on_event` → lifespan.
- README corrections: directory name in quick-start, project-structure listing (add `main.py` sections that exist, list `libp2p_node.py`), replace the stale `from libp2p import new_node` sample with the actual `RealDHTNetwork` usage, document the optional-libp2p install.

## Phase 4 — A/B experiment runner (the "production-grade" differentiator)

Turns "watch the dashboard and squint" into evidence. Runs headless and in-process (no HTTP server) against the simulated network.

- **Config:** a small JSON file (`experiments/*.json`) defining: node count, scenario, workload (target QPS, duration seconds), and the two coordinator configs — `unprotected` (caps set effectively unlimited, e.g. 10_000) and `protected` (real caps, e.g. max_queries=10, max_walks=3).
- **Runner:** `src/experiment.py` builds network + StreamManager + coordinator per arm, drives the (fixed) load generator at the configured QPS for the configured duration, samples coordinator/stream snapshots every 0.5s, and computes per-arm summaries: achieved QPS, success/failure/timeout counts, p50/p95/p99 latency, peak concurrency, peak limiter utilisation.
- **Report:** JSON written to `reports/<name>-<timestamp>.json` plus a self-contained HTML report (inline CSS/JS, side-by-side arm comparison tables + verdict line). Timestamp comes from the caller.
- **CLI:** `python main.py --experiment experiments/baseline.json` runs both arms sequentially and prints the report paths. No dashboard integration (YAGNI — the report is the artifact).
- A default `experiments/baseline.json` ships in the repo and is exercised by tests (short duration).

## Phase 5 — CI, packaging, deployment, persistence

- **Packaging:** `pyproject.toml` with project metadata, dependencies, and pytest/tool config (replaces the Phase 1 standalone pytest config; requirements.txt kept as a thin mirror for the README quick-start).
- **CI:** GitHub Actions workflow — checkout, install, `pytest tests/ -v` on push/PR. Simulated-mode only (no libp2p in CI; the importorskip from Phase 1 makes this clean).
- **Health endpoint:** `GET /healthz` returning `{"status": "ok"}` plus basic liveness facts (uptime, mode) — used by the Docker healthcheck.
- **Prometheus metrics:** `GET /metrics` in text exposition format, hand-rendered from the existing snapshot counters/gauges (~15 series: query counters by status, limiter tokens/capacity, achieved QPS). No new dependency.
- **Snapshot history:** `src/history.py` — SQLite (stdlib `sqlite3`) writer fed from the metrics_broadcaster tick, storing one row per tick (timestamp + JSON snapshot), bounded by age (default: keep 24h, pruned periodically). `GET /api/history?minutes=N` returns downsampled series; the dashboard hydrates its charts from it on page load so charts survive reloads.
- **Docker:** multi-stage Dockerfile (slim Python base, non-root user), `docker-compose.yml` with port mapping, a volume for the SQLite file, and a healthcheck hitting `/healthz`.

## Error handling

Coordinator's SUCCESS/FAILED/TIMEOUT/CANCELLED classification is unchanged. New routes follow existing FastAPI error conventions (400 for wrong mode, 404 for unknown peer). `_emit` failures become visible via logging.

## Testing summary

| Suite | Status |
|---|---|
| `test_coordinator.py` (18 tests) | keep green, untouched |
| `test_integration.py` | two fixes (vacuous + flaky), otherwise keep |
| `test_libp2p_node.py` | `importorskip`, otherwise untouched |
| `test_api.py` (new) | endpoint coverage incl. new peer-chaos routes, /healthz, /metrics, /api/history |
| `test_workers.py` (new) | loadgen concurrency regression + broadcaster tick |
| `test_experiment.py` (new) | short A/B run produces both arms + report files; protected arm respects caps |
| `test_history.py` (new) | write/prune/query roundtrip on the SQLite store |

## Success criteria

1. Fresh venv, `pip install -r requirements.txt`, `python main.py` → dashboard live at `:8000` (no libp2p installed).
2. Load generator at 10 QPS achieves ≈10 QPS with visible limiter saturation under SATURATED; requested-vs-achieved readout shows back-pressure when caps bite.
3. Every scenario, peer-chaos action, and config field is operable from the dashboard; event log narrates it.
4. Most recent query's lookup path renders on the topology view.
5. `pytest tests/ -v` fully green without libp2p installed.
6. `python main.py --experiment experiments/baseline.json` produces a JSON + HTML report where the unprotected arm shows exhaustion symptoms and the protected arm shows bounded concurrency.
7. `docker compose up` → dashboard live with a passing healthcheck; charts survive a page reload (history hydration).
8. GitHub Actions runs the suite green on push.
