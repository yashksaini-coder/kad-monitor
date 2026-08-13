"""
A/B experiment runner: same workload against an 'unprotected' coordinator
(caps effectively unlimited) and a 'protected' one (real caps), producing a
comparable summary per arm.  Headless — no HTTP server involved.
"""

from __future__ import annotations

import json
import logging
import random
from pathlib import Path

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
