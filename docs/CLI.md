# CLI Reference

```
python main.py [flags]
```

Running with no flags serves the dashboard at `http://localhost:8000/` on a
60-node simulated network under the NORMAL scenario.

## Server

| Flag | Default | Description |
|---|---|---|
| `--host` | `0.0.0.0` | HTTP bind host |
| `--port` | `8000` | HTTP bind port |
| `--broadcast-interval` | `0.5` | Metrics WebSocket push interval (seconds) |
| `--history-db` | `history.db` | SQLite snapshot-history path; `''` disables history |

## Backend selection

| Flag | Default | Description |
|---|---|---|
| `--mode` | `simulated` | `simulated` (in-process Kademlia fabric) or `real` (py-libp2p node) |

## Simulated mode

| Flag | Default | Description |
|---|---|---|
| `--nodes` | `60` | Virtual node count |
| `--scenario` | `NORMAL` | Initial scenario: `NORMAL` / `DEGRADED` / `STRESSED` / `SATURATED` (switchable live from the dashboard) |

Scenarios control latency, jitter, offline ratio, injected stream-error
rate, and unknown-peer rate. Switching rebuilds the network (new peer IDs).

## Coordinator limits (the demo's subject)

| Flag | Default | Description |
|---|---|---|
| `--max-queries` | `10` | Layer A: hard cap on ALL concurrent DHT queries |
| `--max-walks` | `3` | Layer B: sub-cap on background random walks (must be < max-queries) |
| `--max-streams` | `50` | Layer C: stream-slot pool |
| `--query-timeout` | `20.0` | Per-query deadline (seconds, `trio.move_on_after`) |
| `--walk-interval` | `4.0` | Seconds between background random walks |
| `--enable-random-walk` / `--no-enable-random-walk` | enabled | Background walk worker |

All four limits are hot-reloadable at runtime via `POST /api/config` or the
dashboard's SYSTEM CONFIG modal (validated: values ≥1, timeouts >0,
walks < queries enforced).

## Real mode only

| Flag | Default | Description |
|---|---|---|
| `--libp2p-port` | `4001` | libp2p listen port |
| `--bootstrap` | none | Bootstrap peer multiaddr (repeatable) |
| `--enable-mdns` | off | mDNS discovery |
| `--enable-upnp` | off | UPnP port mapping |
| `--enable-quic` | off | QUIC transport |
| `--enable-pubsub` / `--no-enable-pubsub` | enabled | GossipSub |
| `--max-connections` | `200` | libp2p ResourceManager connection cap |
| `--max-libp2p-streams` | `1000` | libp2p ResourceManager stream cap |

## Experiment mode (headless, exits when done)

| Flag | Description |
|---|---|
| `--experiment CONFIG_JSON` | Run an A/B experiment from a JSON config; writes JSON + HTML reports to `reports/` and exits |

```bash
python main.py --experiment experiments/baseline.json
```

Config shape (see `experiments/baseline.json`): a `network` block
(nodes, scenario), a `workload` block (qps, duration_s), and named `arms`
each with its own coordinator caps. Every arm runs the identical workload;
the report compares counters, latency percentiles, peak concurrency, and
peak queue depth side by side.

## Recipes

```bash
# Demonstrate the fix: caps on, watch queries queue instead of exhausting
python main.py --max-queries 10 --max-walks 3 --scenario STRESSED
# then start the load generator from the dashboard at 10+ QPS

# Approximate the original unbounded behavior
python main.py --max-queries 5000 --max-walks 4999 --scenario SATURATED

# Quiet mode for API-only use: no walks, no history
python main.py --no-enable-random-walk --history-db ''

# The reproducible before/after evidence
python main.py --experiment experiments/baseline.json
```
