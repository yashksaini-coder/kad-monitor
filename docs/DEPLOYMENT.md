# Deployment

## The plan, in one paragraph

This is a demo/validation harness, and its deployment ceiling is chosen to
match: **one container on any single host, via Docker Compose.** No
Kubernetes, no multi-node orchestration, no public unauthenticated
instance — those are explicit non-goals (the API exposes control endpoints
with no auth, so a world-reachable deployment would let anyone drive your
load generator). Everything needed for that ceiling ships in the repo:
a single-stage Dockerfile, a compose file with a persistent volume and
healthcheck, `/healthz` for liveness, and Prometheus-format `/metrics`
if you want it on a dashboard of dashboards.

## Quick deploy (any Docker host)

```bash
git clone https://github.com/yashksaini-coder/kad-monitor
cd kad-monitor
docker compose up -d
# Dashboard → http://<host>:8000/
```

What that gives you:

- **Image:** `python:3.12-slim`, non-root `monitor` user, simulated mode,
  history at `/data/history.db`
- **Volume:** `kad-data` — snapshot history survives container restarts
  (24h retention, pruned automatically)
- **Healthcheck:** polls `/healthz` every 15s; `docker ps` shows
  `healthy`/`unhealthy` accordingly, and orchestrators can restart on it
- **Restart policy:** `unless-stopped`

## Exposure and safety

The compose file maps `8000:8000` on all interfaces, which is right for a
lab machine or LAN demo. If the host has a public interface, bind to
loopback and front it yourself:

```yaml
    ports:
      - "127.0.0.1:8000:8000"
```

then reach it over an SSH tunnel (`ssh -L 8000:localhost:8000 host`) or
put a reverse proxy with auth in front. **Do not expose the API publicly
as-is** — `/api/loadgen`, `/api/config`, and `/api/network/scenario` are
intentionally unauthenticated control surfaces.

## Observability hooks

- `GET /healthz` → `{"status": "ok", "mode": "...", "uptime_s": ...}`
- `GET /metrics` → Prometheus text exposition (query counters by status,
  limiter borrowed/capacity gauges, throughput, p95/p99, achieved QPS).
  Scrape config:

  ```yaml
  scrape_configs:
    - job_name: kad-monitor
      static_configs:
        - targets: ["<host>:8000"]
  ```

- `GET /api/history?minutes=N` → the same series the dashboard charts,
  as JSON.

## Configuration at deploy time

Override the container command to change flags (see
[CLI.md](CLI.md) for all of them):

```yaml
    command: ["python", "main.py", "--host", "0.0.0.0", "--port", "8000",
              "--history-db", "/data/history.db",
              "--nodes", "100", "--scenario", "STRESSED",
              "--max-queries", "10", "--max-walks", "3"]
```

Coordinator limits can also be changed at runtime from the dashboard's
SYSTEM CONFIG modal or `POST /api/config` — no restart needed.

## Upgrades

```bash
git pull
docker compose up -d --build
```

History persists in the volume; everything else is stateless.

## CI ↔ deployment relationship

`.github/workflows/ci.yml` runs the full suite on every push/PR. The
Docker image is built from the same tree, so a green CI run is the
pre-deployment gate. There is deliberately no CD pipeline — deploys are
one manual command against a tagged/known-green commit, which is the
honest amount of automation for a single-container demo tool.

## If this ever needs to grow

Documented ceilings and their upgrade paths, in the order they'd matter:

1. **Public shared instance** → add an auth layer (reverse proxy with
   basic auth is enough) and rate-limit the control endpoints.
2. **Long-horizon metrics** → point Prometheus at `/metrics` and let it
   own retention; the SQLite history stays as the dashboard's 24h buffer.
3. **Real-network monitoring at scale** → that's "Direction B" (real
   py-libp2p integrity work: true hop counts, pubsub receive loop,
   StreamManager bound to real streams) — a design decision, not a
   deployment one.
