"""
FastAPI Application
===================
REST + WebSocket API for the libp2p DHT Monitor.

Runs under Hypercorn with the Trio ASGI backend so all ``async def``
handlers share the same Trio event loop as the coordinator and workers.

Endpoints
---------
GET  /api/snapshot           -> full system state (one-shot)
GET  /api/nodes              -> list of peers
POST /api/query              -> trigger a user find_peer
POST /api/config             -> hot-reload coordinator / stream limits
POST /api/network/scenario   -> switch load scenario
DELETE /api/network/peer/{id} -> remove a peer (simulated mode)
POST /api/network/peer/{id}/toggle -> toggle peer online/offline (simulated mode)
POST /api/loadgen            -> start / stop the load generator
POST /api/dht/put            -> DHT put_value (real mode)
POST /api/dht/get            -> DHT get_value (real mode)
POST /api/dht/provide        -> DHT provide (real mode)
POST /api/dht/providers      -> DHT find_providers (real mode)
POST /api/pubsub/publish     -> publish to GossipSub topic (real mode)
POST /api/pubsub/subscribe   -> subscribe to topic (real mode)
POST /api/peer/connect       -> connect to a peer by multiaddr (real mode)
WS   /ws                     -> live push of metrics snapshots (500ms tick)
GET  /                       -> serve the dashboard
"""

from __future__ import annotations

import hashlib
import json
import logging
import random
import time
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from src.coordinator import QueryPriority

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    peer_id: str = Field(default="", description="Peer ID to look up (empty = random known)")
    mode: str = Field(default="known", description="known | unknown | custom")


class ConfigRequest(BaseModel):
    max_concurrent_queries: int | None = None
    max_random_walks: int | None = None
    query_timeout: float | None = None
    max_streams: int | None = None
    stream_timeout: float | None = None


class ScenarioRequest(BaseModel):
    scenario: str = Field(description="NORMAL | DEGRADED | STRESSED | SATURATED")


class LoadGenRequest(BaseModel):
    active: bool = False
    qps: float = Field(default=2.0, ge=0.1, le=20.0)
    mode: str = Field(default="mixed", description="known | unknown | mixed")


class DHTValueRequest(BaseModel):
    key: str = Field(description="DHT key (e.g. /app/mykey)")
    value: str = Field(default="", description="Value to store (for put)")


class ProvideRequest(BaseModel):
    key: str = Field(description="Content key to provide or find providers for")
    count: int = Field(default=20, ge=1, le=100, description="Max providers to return")


class PubSubRequest(BaseModel):
    topic: str = Field(description="Topic name")
    data: str = Field(default="", description="Message data (for publish)")


class PeerConnectRequest(BaseModel):
    multiaddr: str = Field(description="Full multiaddr with /p2p/ suffix")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def create_app(
    coordinator,
    network,
    stream_manager,
    load_gen_state: dict,
    libp2p_node=None,
    mode: str = "simulated",
) -> tuple:
    app = FastAPI(
        title="libp2p DHT Monitor",
        description=(
            "Real-time monitoring and interaction with py-libp2p 0.6.0. "
            "DHTQueryCoordinator with trio.CapacityLimiter, KadDHT, GossipSub, "
            "ResourceManager."
        ),
        version="2.0.0",
    )

    connected_ws: list[WebSocket] = []

    def _full_snapshot() -> dict:
        return {
            **coordinator.snapshot(),
            **stream_manager.snapshot(),
            **network.snapshot(),
            "load_gen": dict(load_gen_state),
            "mode": mode,
            "ts": time.time(),
        }

    # -----------------------------------------------------------------------
    # Core REST routes
    # -----------------------------------------------------------------------

    @app.get("/api/snapshot")
    async def get_snapshot():
        return _full_snapshot()

    @app.get("/api/nodes")
    async def get_nodes():
        return {"nodes": network.snapshot()["nodes"]}

    @app.post("/api/query")
    async def trigger_query(req: QueryRequest):
        if req.mode == "unknown" or (req.mode == "custom" and not req.peer_id):
            target = hashlib.sha256(str(time.time()).encode()).hexdigest()[:40]
        elif req.mode == "known" or not req.peer_id:
            pids = network.peer_ids
            if not pids:
                raise HTTPException(400, "Network has no peers")
            target = random.choice(pids)
        else:
            target = req.peer_id

        async def _query_fn(pid: str):
            async with stream_manager.open_stream(pid, "/libp2p/kad/1.0.0"):
                return await network.query(pid)

        result = await coordinator.find_peer(target, _query_fn, QueryPriority.USER)
        return result.to_dict()

    @app.post("/api/config")
    async def update_config(req: ConfigRequest):
        coordinator.reconfigure(
            max_concurrent_queries=req.max_concurrent_queries,
            max_random_walks=req.max_random_walks,
            query_timeout=req.query_timeout,
        )
        stream_manager.reconfigure(
            max_streams=req.max_streams,
            stream_timeout=req.stream_timeout,
        )
        return {"status": "ok", "snapshot": coordinator.snapshot()}

    @app.post("/api/network/scenario")
    async def set_scenario(req: ScenarioRequest):
        try:
            network.set_scenario(req.scenario)
        except ValueError as exc:
            raise HTTPException(400, str(exc))
        return {"status": "ok", "scenario": req.scenario, **network.snapshot()}

    @app.post("/api/loadgen")
    async def control_loadgen(req: LoadGenRequest):
        load_gen_state["active"] = req.active
        load_gen_state["qps"] = req.qps
        load_gen_state["mode"] = req.mode
        return {"status": "ok", **load_gen_state}

    @app.get("/api/loadgen")
    async def get_loadgen():
        return load_gen_state

    @app.post("/api/network/peer")
    async def add_peer():
        try:
            node = network.add_peer()
            if node and hasattr(node, "to_dict"):
                return {"status": "added", "peer": node.to_dict()}
            return {"status": "ok", "message": "Peer management handled by real network"}
        except Exception as e:
            raise HTTPException(400, str(e))

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

    # -----------------------------------------------------------------------
    # DHT Value Store (real mode only)
    # -----------------------------------------------------------------------

    @app.post("/api/dht/put")
    async def dht_put_value(req: DHTValueRequest):
        if libp2p_node is None:
            raise HTTPException(400, "DHT value store requires real libp2p mode")
        if not req.key or not req.value:
            raise HTTPException(400, "Both key and value are required")
        ok = await libp2p_node.put_value(req.key, req.value.encode("utf-8"))
        return {"status": "ok" if ok else "failed", "key": req.key}

    @app.post("/api/dht/get")
    async def dht_get_value(req: DHTValueRequest):
        if libp2p_node is None:
            raise HTTPException(400, "DHT value store requires real libp2p mode")
        if not req.key:
            raise HTTPException(400, "Key is required")
        value = await libp2p_node.get_value(req.key)
        return {
            "key": req.key,
            "found": value is not None,
            "value": value.decode("utf-8", errors="replace") if value else None,
        }

    @app.post("/api/dht/provide")
    async def dht_provide(req: ProvideRequest):
        if libp2p_node is None:
            raise HTTPException(400, "DHT providers require real libp2p mode")
        ok = await libp2p_node.provide(req.key)
        return {"status": "ok" if ok else "failed", "key": req.key}

    @app.post("/api/dht/providers")
    async def dht_find_providers(req: ProvideRequest):
        if libp2p_node is None:
            raise HTTPException(400, "DHT providers require real libp2p mode")
        providers = await libp2p_node.find_providers(req.key, req.count)
        return {"key": req.key, "count": len(providers), "providers": providers}

    # -----------------------------------------------------------------------
    # PubSub (real mode only)
    # -----------------------------------------------------------------------

    @app.post("/api/pubsub/subscribe")
    async def pubsub_subscribe(req: PubSubRequest):
        if libp2p_node is None:
            raise HTTPException(400, "PubSub requires real libp2p mode")
        ok = await libp2p_node.subscribe(req.topic)
        return {
            "status": "subscribed" if ok else "failed",
            "topic": req.topic,
            "topics": libp2p_node.get_subscribed_topics(),
        }

    @app.post("/api/pubsub/unsubscribe")
    async def pubsub_unsubscribe(req: PubSubRequest):
        if libp2p_node is None:
            raise HTTPException(400, "PubSub requires real libp2p mode")
        ok = await libp2p_node.unsubscribe(req.topic)
        return {
            "status": "unsubscribed" if ok else "failed",
            "topic": req.topic,
            "topics": libp2p_node.get_subscribed_topics(),
        }

    @app.post("/api/pubsub/publish")
    async def pubsub_publish(req: PubSubRequest):
        if libp2p_node is None:
            raise HTTPException(400, "PubSub requires real libp2p mode")
        if not req.topic or not req.data:
            raise HTTPException(400, "Both topic and data are required")
        ok = await libp2p_node.publish(req.topic, req.data.encode("utf-8"))
        return {"status": "published" if ok else "failed", "topic": req.topic}

    @app.get("/api/pubsub/topics")
    async def pubsub_topics():
        if libp2p_node is None:
            return {"topics": [], "mode": "simulated"}
        return {"topics": libp2p_node.get_subscribed_topics(), "mode": "real"}

    # -----------------------------------------------------------------------
    # Peer Management (real mode)
    # -----------------------------------------------------------------------

    @app.post("/api/peer/connect")
    async def peer_connect(req: PeerConnectRequest):
        if libp2p_node is None:
            raise HTTPException(400, "Peer connect requires real libp2p mode")
        ok = await libp2p_node.connect_peer(req.multiaddr)
        return {"status": "connected" if ok else "failed", "multiaddr": req.multiaddr}

    @app.get("/api/peer/connected")
    async def peer_connected():
        if libp2p_node is None:
            return {"peers": [], "mode": "simulated"}
        return {"peers": libp2p_node.get_connected_peers(), "mode": "real"}

    @app.get("/api/node/info")
    async def node_info():
        if libp2p_node is None:
            return {"mode": "simulated", "node": None}
        return {"mode": "real", **libp2p_node.snapshot()}

    # -----------------------------------------------------------------------
    # WebSocket
    # -----------------------------------------------------------------------

    @app.websocket("/ws")
    async def websocket_endpoint(ws: WebSocket):
        await ws.accept()
        connected_ws.append(ws)
        logger.info("WebSocket client connected (%d total)", len(connected_ws))
        try:
            snapshot = _full_snapshot()
            await ws.send_text(json.dumps(snapshot, default=str))

            while True:
                try:
                    data = await ws.receive_text()
                    msg = json.loads(data)
                    if msg.get("type") == "ping":
                        await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
                except WebSocketDisconnect:
                    break
                except Exception:
                    break
        finally:
            if ws in connected_ws:
                connected_ws.remove(ws)
            logger.info("WebSocket client disconnected (%d remaining)", len(connected_ws))

    # -----------------------------------------------------------------------
    # Dashboard
    # -----------------------------------------------------------------------

    @app.get("/", response_class=HTMLResponse)
    async def dashboard():
        html_path = Path(__file__).parent.parent / "static" / "index.html"
        if html_path.exists():
            return HTMLResponse(html_path.read_text())
        return HTMLResponse("<h1>Dashboard not found</h1>")

    # -----------------------------------------------------------------------
    # Expose state for external wiring
    # -----------------------------------------------------------------------

    app.state.connected_ws = connected_ws
    app.state.coordinator = coordinator
    app.state.network = network
    app.state.stream_manager = stream_manager
    app.state.load_gen_state = load_gen_state
    app.state.libp2p_node = libp2p_node

    return app, connected_ws
