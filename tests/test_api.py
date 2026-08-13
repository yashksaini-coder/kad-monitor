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
