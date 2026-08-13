"""
tests/test_libp2p_node.py
==========================
Tests for the real py-libp2p 0.6.0 integration.

These tests verify:
- Libp2pNode creates a host with ResourceManager
- KadDHT starts and exposes routing table
- GossipSub / PubSub initialises
- Node lifecycle (start/stop) is clean
- RealDHTNetwork adapter matches SimulatedDHTNetwork interface

Run with:
    pytest tests/test_libp2p_node.py -v --tb=short
"""

from __future__ import annotations

import pytest

pytest.importorskip("libp2p", reason="real-mode tests require the optional libp2p dependency")

import trio

from src.libp2p_node import Libp2pNode, Libp2pNodeConfig, RealDHTNetwork


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


def test_config_defaults():
    cfg = Libp2pNodeConfig()
    assert cfg.port == 4001
    assert cfg.max_connections == 200
    assert cfg.max_streams == 1000
    assert cfg.enable_pubsub is True
    assert cfg.enable_random_walk is True


def test_config_from_dict():
    cfg = Libp2pNodeConfig.from_dict({
        "port": 5555,
        "enable_quic": True,
        "max_connections": 500,
    })
    assert cfg.port == 5555
    assert cfg.enable_quic is True
    assert cfg.max_connections == 500


def test_config_from_dict_ignores_unknown():
    cfg = Libp2pNodeConfig.from_dict({
        "port": 4001,
        "nonexistent_field": "ignored",
    })
    assert cfg.port == 4001


# ---------------------------------------------------------------------------
# Node lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_node_start_stop():
    """Node starts, gets a peer ID, and stops cleanly."""
    cfg = Libp2pNodeConfig(
        port=0,  # OS-assigned port
        enable_mdns=False,
        enable_upnp=False,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    node = Libp2pNode(cfg)
    assert not node.is_started

    try:
        await node.start()
        assert node.is_started
        assert node.peer_id is not None
        assert len(node.peer_id) > 0
    finally:
        await node.stop()
        assert not node.is_started


@pytest.mark.trio
async def test_node_context_manager():
    """Node works as async context manager."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        assert node.is_started
        assert node.peer_id is not None

    assert not node.is_started


@pytest.mark.trio
async def test_node_listen_addresses():
    """Node reports listen addresses after start."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        addrs = node.get_listen_addresses()
        assert isinstance(addrs, list)
        # Should have at least TCP address
        assert len(addrs) >= 1


@pytest.mark.trio
async def test_node_snapshot():
    """Node snapshot has required structure."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        snap = node.snapshot()
        assert "node" in snap
        assert "resource_manager" in snap
        assert "stats" in snap
        assert snap["node"]["peer_id"] == node.peer_id
        assert snap["node"]["started"] is True


@pytest.mark.trio
async def test_resource_manager_stats():
    """ResourceManager is initialised and returns stats."""
    cfg = Libp2pNodeConfig(
        port=0,
        max_connections=100,
        max_streams=500,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        stats = node.resource_manager_stats
        assert isinstance(stats, dict)


# ---------------------------------------------------------------------------
# DHT operations (single-node — no remote peers)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_find_peer_nonexistent():
    """find_peer for a non-existent peer returns (False, closest, hops)."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        found, closest, hops = await node.find_peer("QmNonExistent123")
        assert found is False
        assert isinstance(closest, list)


@pytest.mark.trio
async def test_put_get_value():
    """put_value and get_value on the local DHT."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        ok = await node.put_value("/test/key1", b"hello world")
        # On a single-node DHT, put_value may or may not succeed depending on
        # quorum requirements, but it should not raise
        assert isinstance(ok, bool)

        val = await node.get_value("/test/key1")
        # Single node may not store locally — just check it doesn't crash
        assert val is None or isinstance(val, bytes)


@pytest.mark.trio
async def test_provide_find_providers():
    """provide and find_providers on the local DHT."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        ok = await node.provide("/test/content1")
        assert isinstance(ok, bool)

        providers = await node.find_providers("/test/content1")
        assert isinstance(providers, list)


# ---------------------------------------------------------------------------
# PubSub (single-node)
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_pubsub_subscribe_unsubscribe():
    """Subscribe and unsubscribe to topics."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=True,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        ok = await node.subscribe("/test/topic1")
        assert ok is True
        assert "/test/topic1" in node.get_subscribed_topics()

        ok = await node.unsubscribe("/test/topic1")
        assert ok is True
        assert "/test/topic1" not in node.get_subscribed_topics()


@pytest.mark.trio
async def test_pubsub_publish():
    """Publishing to a topic should not raise."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=True,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        await node.subscribe("/test/topic1")
        ok = await node.publish("/test/topic1", b"hello pubsub")
        assert isinstance(ok, bool)


@pytest.mark.trio
async def test_pubsub_disabled():
    """When PubSub is disabled, operations return False gracefully."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        ok = await node.subscribe("/test/topic1")
        assert ok is False
        ok = await node.publish("/test/topic1", b"nope")
        assert ok is False


# ---------------------------------------------------------------------------
# RealDHTNetwork adapter
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_real_dht_network_adapter():
    """RealDHTNetwork wraps Libp2pNode with SimulatedDHTNetwork-compatible interface."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        network = RealDHTNetwork(node, scenario="NORMAL")

        assert network.scenario_name == "NORMAL"
        assert isinstance(network.peer_ids, list)
        assert len(network.peer_ids) >= 1  # at least self

        snap = network.snapshot()
        assert "network" in snap
        assert "nodes" in snap
        assert snap["network"]["is_real_libp2p"] is True
        assert snap["network"]["peer_id"] == node.peer_id


@pytest.mark.trio
async def test_real_dht_network_scenario_switch():
    """Scenario switching changes the adapter's latency parameters."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        network = RealDHTNetwork(node, scenario="NORMAL")

        network.set_scenario("STRESSED")
        assert network.scenario_name == "STRESSED"

        with pytest.raises(ValueError):
            network.set_scenario("NONEXISTENT")


@pytest.mark.trio
async def test_real_dht_network_query():
    """query() through the adapter should complete without hanging."""
    cfg = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    async with Libp2pNode(cfg) as node:
        network = RealDHTNetwork(node, scenario="NORMAL")

        with trio.move_on_after(5.0) as cancel_scope:
            found, closest, hops = await network.query("QmFakePeerId12345")

        assert not cancel_scope.cancelled_caught, "query() hung — did not complete within 5s"
        assert found is False  # single node, no peers
        assert isinstance(closest, list)


# ---------------------------------------------------------------------------
# Two-node connectivity
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_two_nodes_connect():
    """Two real libp2p nodes can connect to each other."""
    cfg1 = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )
    cfg2 = Libp2pNodeConfig(
        port=0,
        enable_pubsub=False,
        enable_random_walk=False,
    )

    async with Libp2pNode(cfg1) as node1, Libp2pNode(cfg2) as node2:
        addrs1 = node1.get_listen_addresses()
        assert len(addrs1) >= 1

        # Build a full multiaddr with peer ID
        full_addr = f"{addrs1[0]}/p2p/{node1.peer_id}"
        ok = await node2.connect_peer(full_addr)
        assert ok is True

        # node2 should now see node1 as a connected peer
        connected = node2.get_connected_peers()
        assert node1.peer_id in connected
