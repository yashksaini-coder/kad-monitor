"""
main.py
=======
Entry point for the libp2p DHT Monitor.

Supports two modes:
  - simulated: Uses SimulatedDHTNetwork (default, no external dependencies)
  - real: Uses real py-libp2p 0.6.0 (KadDHT + GossipSub + ResourceManager)

Usage
-----
    # Simulated mode (default)
    python main.py --nodes 60 --scenario STRESSED

    # Real libp2p mode
    python main.py --mode real --libp2p-port 4001 --bootstrap /ip4/1.2.3.4/tcp/4001/p2p/PeerID...

    # Full options
    python main.py --mode simulated --nodes 80 --scenario DEGRADED --max-queries 12
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

import trio
import hypercorn.config
import hypercorn.trio as hypercorn_trio

from api.app import create_app
from src.coordinator import DHTQueryCoordinator
from src.stream_manager import StreamManager
from src.workers import load_generator, metrics_broadcaster, random_walk_worker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("main")


def parse_args():
    p = argparse.ArgumentParser(description="libp2p DHT Monitor (py-libp2p 0.6.0)")

    p.add_argument(
        "--host", default="0.0.0.0", help="HTTP bind host (default: 0.0.0.0)"
    )
    p.add_argument(
        "--port", type=int, default=8000, help="HTTP bind port (default: 8000)"
    )

    p.add_argument(
        "--mode",
        default="simulated",
        choices=["simulated", "real"],
        help="DHT backend: 'simulated' or 'real' libp2p (default: simulated)",
    )

    # Simulated mode options
    p.add_argument(
        "--nodes",
        type=int,
        default=60,
        help="Simulated DHT node count (simulated mode)",
    )
    p.add_argument(
        "--scenario",
        default="NORMAL",
        choices=["NORMAL", "DEGRADED", "STRESSED", "SATURATED"],
        help="Initial network scenario",
    )

    # Real libp2p options
    p.add_argument(
        "--libp2p-port", type=int, default=4001, help="libp2p listen port (real mode)"
    )
    p.add_argument(
        "--enable-mdns", action="store_true", help="Enable mDNS discovery (real mode)"
    )
    p.add_argument("--enable-upnp", action="store_true", help="Enable UPnP (real mode)")
    p.add_argument(
        "--enable-quic", action="store_true", help="Enable QUIC transport (real mode)"
    )
    p.add_argument(
        "--bootstrap",
        action="append",
        default=[],
        help="Bootstrap peer multiaddr (can specify multiple)",
    )
    p.add_argument(
        "--enable-random-walk",
        action="store_true",
        default=True,
        help="Enable DHT random walk (real mode)",
    )
    p.add_argument(
        "--enable-pubsub",
        action="store_true",
        default=True,
        help="Enable GossipSub (real mode)",
    )

    # Coordinator limits
    p.add_argument(
        "--max-queries", type=int, default=10, help="Max concurrent DHT queries"
    )
    p.add_argument(
        "--max-walks", type=int, default=3, help="Max concurrent random walks"
    )
    p.add_argument("--max-streams", type=int, default=50, help="Max concurrent streams")
    p.add_argument(
        "--query-timeout", type=float, default=20.0, help="Query timeout in seconds"
    )
    p.add_argument(
        "--walk-interval",
        type=float,
        default=4.0,
        help="Random walk interval in seconds",
    )
    p.add_argument(
        "--broadcast-interval",
        type=float,
        default=0.5,
        help="Metrics push interval in seconds",
    )

    # ResourceManager limits (real mode)
    p.add_argument(
        "--max-connections", type=int, default=200, help="ResourceManager max connections"
    )
    p.add_argument(
        "--max-libp2p-streams", type=int, default=1000, help="ResourceManager max streams"
    )

    return p.parse_args()


async def main_simulated(args):
    from src.dht_simulation import SimulatedDHTNetwork

    logger.info("=" * 60)
    logger.info("  libp2p DHT Monitor — SIMULATED MODE")
    logger.info("=" * 60)

    logger.info(
        "Initialising simulated DHT network (%d nodes, scenario=%s)…",
        args.nodes,
        args.scenario,
    )
    network = SimulatedDHTNetwork(
        node_count=args.nodes,
        scenario=args.scenario,
    )

    stream_manager = StreamManager(
        max_streams=args.max_streams,
        stream_timeout=args.query_timeout + 5.0,
    )

    broadcast_send, broadcast_recv = trio.open_memory_channel(max_buffer_size=50)

    async def _on_snapshot(snap: dict) -> None:
        merged = {
            **snap,
            **stream_manager.snapshot(),
            **network.snapshot(),
            "ts": time.time(),
            "mode": "simulated",
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

    load_gen_state: dict = {
        "active": False,
        "qps": 2.0,
        "mode": "mixed",
    }

    app, connected_ws = create_app(
        coordinator=coordinator,
        network=network,
        stream_manager=stream_manager,
        load_gen_state=load_gen_state,
        broadcast_send=broadcast_send,
        broadcast_recv=broadcast_recv,
        libp2p_node=None,
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

    logger.info("Starting all services on http://%s:%d …", args.host, args.port)
    logger.info("Dashboard → http://localhost:%d/", args.port)
    logger.info("API docs  → http://localhost:%d/docs", args.port)
    logger.info("")

    async with trio.open_nursery() as nursery:
        await nursery.start(
            metrics_broadcaster,
            coordinator,
            network,
            stream_manager,
            broadcast_send,
            args.broadcast_interval,
        )

        nursery.start_soon(_ws_broadcaster)

        await nursery.start(
            random_walk_worker,
            coordinator,
            network,
            stream_manager,
            args.walk_interval,
        )

        await nursery.start(
            load_generator,
            coordinator,
            network,
            stream_manager,
            load_gen_state,
        )

        nursery.start_soon(hypercorn_trio.serve, app, hc_config)

        logger.info("All services running. Press Ctrl+C to stop.")


async def main_real(args):
    from src.libp2p_node import Libp2pNode, Libp2pNodeConfig, RealDHTNetwork

    logger.info("=" * 60)
    logger.info("  libp2p DHT Monitor — REAL libp2p 0.6.0 MODE")
    logger.info("  KadDHT + GossipSub + ResourceManager + trio")
    logger.info("=" * 60)

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

    logger.info("Starting real libp2p node on port %d…", args.libp2p_port)
    if args.bootstrap:
        logger.info("Bootstrap peers: %s", args.bootstrap)

    node = Libp2pNode(node_config)
    await node.start()

    network = RealDHTNetwork(node, scenario=args.scenario)

    stream_manager = StreamManager(
        max_streams=args.max_streams,
        stream_timeout=args.query_timeout + 5.0,
    )

    broadcast_send, broadcast_recv = trio.open_memory_channel(max_buffer_size=50)

    async def _on_snapshot(snap: dict) -> None:
        merged = {
            **snap,
            **stream_manager.snapshot(),
            **network.snapshot(),
            "ts": time.time(),
            "mode": "real",
            "peer_id": node.peer_id,
            "listen_addrs": node.get_listen_addresses(),
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

    load_gen_state: dict = {
        "active": False,
        "qps": 2.0,
        "mode": "mixed",
    }

    app, connected_ws = create_app(
        coordinator=coordinator,
        network=network,
        stream_manager=stream_manager,
        load_gen_state=load_gen_state,
        broadcast_send=broadcast_send,
        broadcast_recv=broadcast_recv,
        libp2p_node=node,
    )

    http_config = hypercorn.config.Config()
    http_config.bind = [f"{args.host}:{args.port}"]
    http_config.use_reloader = False
    http_config.accesslog = "-"
    http_config.errorlog = "-"
    http_config.loglevel = "WARNING"

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

    logger.info("Starting all services on http://%s:%d …", args.host, args.port)
    logger.info("Dashboard → http://localhost:%d/", args.port)
    logger.info("API docs  → http://localhost:%d/docs", args.port)
    logger.info("libp2p Peer ID: %s", node.peer_id)
    logger.info("libp2p Addresses: %s", node.get_listen_addresses())
    logger.info("ResourceManager: enabled (max_conn=%d, max_streams=%d)",
                args.max_connections, args.max_libp2p_streams)
    logger.info("PubSub: %s", "GossipSub enabled" if args.enable_pubsub else "disabled")
    logger.info("")

    try:
        async with trio.open_nursery() as nursery:
            await nursery.start(
                metrics_broadcaster,
                coordinator,
                network,
                stream_manager,
                broadcast_send,
                args.broadcast_interval,
            )

            nursery.start_soon(_ws_broadcaster)

            if args.enable_random_walk:
                await nursery.start(
                    random_walk_worker,
                    coordinator,
                    network,
                    stream_manager,
                    args.walk_interval,
                )

            await nursery.start(
                load_generator,
                coordinator,
                network,
                stream_manager,
                load_gen_state,
            )

            nursery.start_soon(hypercorn_trio.serve, app, http_config)

            logger.info("All services running. Press Ctrl+C to stop.")
    finally:
        logger.info("Stopping libp2p node...")
        await node.stop()


async def main(args):
    if args.mode == "real":
        await main_real(args)
    else:
        await main_simulated(args)


if __name__ == "__main__":
    args = parse_args()
    try:
        trio.run(main, args)
    except KeyboardInterrupt:
        logger.info("Shutting down — goodbye.")
