# Setup

## Prerequisites

- Python **3.11+** (3.12 is what CI runs; 3.14 works for simulated mode)
- Git
- Docker + Compose (only for containerized runs — see [DEPLOYMENT.md](DEPLOYMENT.md))

## Development setup

```bash
git clone https://github.com/yashksaini-coder/kad-monitor
cd kad-monitor
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

That installs the full simulated-mode stack plus dev tooling (pytest,
pytest-trio, httpx). **libp2p is intentionally not included** — simulated
mode never imports it (guarded by `tests/test_imports.py`).

## Run it

```bash
python main.py
# Dashboard → http://localhost:8000/
# API docs  → http://localhost:8000/docs
```

See [CLI.md](CLI.md) for every flag, and the README for the guided tour.

## Run the tests

```bash
pytest tests/ -v
```

Expected: everything green, with `tests/test_libp2p_node.py` skipped
(it needs the optional libp2p dependency). The suite is deterministic —
a red test is a real failure, not a flake.

## Optional: real py-libp2p mode

Real mode (`--mode real`) wraps an actual py-libp2p 0.6.0 node
(KadDHT + GossipSub + ResourceManager):

```bash
pip install libp2p==0.6.0 multiaddr base58
python main.py --mode real --libp2p-port 4001
```

**Known constraint:** `libp2p==0.6.0` does not install on Python 3.14
(its `coincurve` dependency fails to build). Use Python ≤3.12 for real
mode. Real mode is functional but less battle-tested than simulated mode;
its hop counts are approximate and pubsub receive is not yet surfaced.

## Repo layout

```
src/         coordinator, simulation, stream manager, workers, experiment, history
api/         FastAPI app (REST + WebSocket)
static/      single-file dashboard + vendored Chart.js
tests/       pytest-trio suite (unit, integration, API, workers, experiment, history)
experiments/ A/B experiment configs
docs/        this documentation
```
