# libp2p DHT Monitor

**Production-grade test harness and real-time dashboard for validating the
`DHTQueryCoordinator` fix described in the Technical Design Doc:
"Resolving DHT Resource Exhaustion".**

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────┐
│                  Single Trio Event Loop                   │
│                                                          │
│  ┌──────────────────┐    ┌───────────────────────────┐  │
│  │ DHTQueryCoordinator│   │      SimulatedDHTNetwork  │  │
│  │                  │    │   (Kademlia-style DHT)    │  │
│  │  Layer A:        │    │                           │  │
│  │  trio.CapacityLimiter│ │  50-100 virtual nodes     │  │
│  │  (max_queries)   │    │  XOR-distance routing     │  │
│  │                  │    │  Configurable scenarios   │  │
│  │  Layer B:        │    └───────────┬───────────────┘  │
│  │  trio.CapacityLimiter│            │ query_fn           │
│  │  (max_walks)     │◄───────────────┘                  │
│  └────────┬─────────┘                                    │
│           │ on_snapshot                                  │
│           ▼                                              │
│  ┌──────────────────┐    ┌───────────────────────────┐  │
│  │  StreamManager   │    │    Background Workers     │  │
│  │                  │    │                           │  │
│  │  trio.CapacityLimiter│ │  random_walk_worker       │  │
│  │  (max_streams)   │    │  load_generator           │  │
│  │  try…finally     │    │  metrics_broadcaster      │  │
│  │  stream lifecycle│    └───────────────────────────┘  │
│  └──────────────────┘                                    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐   │
│  │       FastAPI (Hypercorn / Trio ASGI backend)    │   │
│  │                                                  │   │
│  │  REST: /api/query  /api/config  /api/loadgen     │   │
│  │        /api/network/scenario   /api/snapshot     │   │
│  │  WS:   /ws  (500ms push of full system snapshot) │   │
│  │  UI:   /    (real-time dashboard)                │   │
│  └──────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

---

## The Fix: Dual-Layer `trio.CapacityLimiter`

| Layer | Component | Role |
|-------|-----------|------|
| **A** | `DHTQueryCoordinator._query_limiter` | Hard cap on ALL DHT ops |
| **B** | `DHTQueryCoordinator._rw_limiter` | Sub-cap on background walks |
| **C** | `StreamManager._limiter` | Hard cap on physical streams |

**Before the fix** (original py-libp2p behaviour):
- Random walks + user queries competed for unlimited streams
- Timed-out queries leaked stream slots → `Stream limit exceeded`
- Subsequent calls hung forever at `nursery.start_soon(...)`

**After the fix** (this implementation):
- All acquisitions go through `async with limiter:` — guaranteed back-pressure
- `try … finally` in `StreamManager.open_stream` ensures slots always return
- `trio.move_on_after` enforces per-query deadlines with clean cancellation

---

## Quick Start

### 1. Install dependencies

```bash
cd libp2p-dht-monitor
pip install -r requirements.txt
```

### 2. Run the monitor

```bash
python main.py
# Dashboard → http://localhost:8000/
# API docs  → http://localhost:8000/docs
```

### 3. CLI options

```
--host           Bind host          (default: 0.0.0.0)
--port           Bind port          (default: 8000)
--nodes          Simulated nodes    (default: 60)
--scenario       Initial scenario   (NORMAL | DEGRADED | STRESSED | SATURATED)
--max-queries    Layer A cap        (default: 10)
--max-walks      Layer B cap        (default: 3)
--max-streams    Stream pool cap    (default: 50)
--query-timeout  Per-query timeout  (default: 20s)
--walk-interval  Random walk period (default: 4s)
```

**Reproduce the original bug** (no coordinator, minimal caps):
```bash
python main.py --max-queries 50 --max-walks 49 --nodes 30 --scenario SATURATED
# Then hammer the load generator at 10+ QPS
# Observe streams exhausting and queries hanging
```

**Demonstrate the fix**:
```bash
python main.py --max-queries 10 --max-walks 3 --nodes 60 --scenario STRESSED
# Layer B cap keeps walks at ≤3 concurrent even at 10 QPS
# User queries always have ≥7 slots reserved
```

---

## Running Tests

```bash
# All tests
pytest tests/ -v

# Coordinator unit tests only
pytest tests/test_coordinator.py -v

# Integration tests
pytest tests/test_integration.py -v

# Specific test
pytest tests/test_coordinator.py::test_background_walks_cannot_starve_user_queries -v
```

### Key test cases

| Test | What it verifies |
|------|-----------------|
| `test_capacity_limiter_blocks_excess_queries` | >max concurrent queries queue, not crash |
| `test_background_walks_cannot_starve_user_queries` | Layer B isolation works |
| `test_random_walk_cap_enforced` | Peak concurrent walks ≤ `max_random_walks` |
| `test_timeout_enforced` | `trio.move_on_after` fires correctly |
| `test_stream_slots_always_released` | `finally` block works on error |
| `test_no_resource_exhaustion_under_load` | 20 concurrent queries with cap=10: no deadlock |

---

## Dashboard Features

| Panel | Description |
|-------|-------------|
| **Capacity Limiters** | Live gauge of Layer A / Layer B / Stream pool utilisation |
| **Query Counters** | Total / success / failed / timeout with success % |
| **Throughput chart** | Rolling QPS over last 60 ticks |
| **Duration chart** | Rolling avg duration (ms) |
| **Concurrency chart** | Rolling concurrent query count |
| **Live Queries** | Real-time view of in-flight queries with status |
| **Network Topology** | SVG map of simulated nodes (green=online, red=offline) |
| **Recent Results** | Scrollable result table with full metadata |
| **Event Log** | Timestamped event stream |

### Interactive Controls

- **Fire Query**: Single manual `find_peer` (known or unknown peer)
- **Load Generator**: Continuous query storm at configurable QPS
- **Network Scenario**: Switch between NORMAL / DEGRADED / STRESSED / SATURATED
- **Configure Limits**: Hot-reload `max_queries`, `max_walks`, `query_timeout`, `max_streams`

---

## Swapping in Real py-libp2p

The coordinator accepts any `async (peer_id: str) → (found, closest_peers, hops)` callable.
Replace the simulation with real libp2p:

```python
from libp2p import new_node
from libp2p.kademlia.kad_peerinfo import create_kad_peerinfo

async def real_query_fn(peer_id: str):
    async with stream_manager.open_stream(peer_id, "/libp2p/kad/1.0.0"):
        # Real libp2p DHT call here
        result = await libp2p_node.find_peer(peer_id)
        return bool(result), result.closest_peers, result.num_hops

result = await coordinator.find_peer(target_id, real_query_fn)
```

The coordinator, StreamManager, and all resource lifecycle guarantees remain identical.

---

## Project Structure

```
libp2p-dht-monitor/
├── src/
│   ├── coordinator.py      # DHTQueryCoordinator (trio.CapacityLimiter dual-layer)
│   ├── stream_manager.py   # StreamManager (physical stream lifecycle)
│   ├── dht_simulation.py   # Kademlia-style DHT simulation
│   └── workers.py          # Background trio tasks
├── api/
│   └── app.py              # FastAPI app (REST + WebSocket)
├── static/
│   └── index.html          # Real-time monitoring dashboard
├── tests/
│   ├── test_coordinator.py # Unit tests (pytest-trio)
│   └── test_integration.py # Integration tests
├── main.py                 # Entry point (Hypercorn + Trio)
├── requirements.txt
└── README.md
```
