import time

from src.history import SnapshotHistory


def make_snap(ts, qps):
    return {
        "ts": ts,
        "coordinator": {
            "rates": {"throughput_qps": qps, "avg_duration_ms": 100.0},
            "concurrency": {"current": 2, "acquiring": 1},
        },
    }


def test_append_and_query(tmp_path):
    h = SnapshotHistory(str(tmp_path / "h.db"))
    now = time.time()
    for i in range(5):
        h.append(make_snap(now - 60 + i, qps=float(i)))

    points = h.query(minutes=5)
    assert len(points) == 5
    assert points[0]["qps"] == 0.0 and points[-1]["qps"] == 4.0
    assert points[0]["concurrency"] == 3  # current + acquiring


def test_prune(tmp_path):
    h = SnapshotHistory(str(tmp_path / "h.db"), retention_hours=1.0)
    now = time.time()
    h.append(make_snap(now - 7200, qps=1.0))   # 2h old — beyond retention
    h._last_prune = 0.0                        # force prune on next append
    h.append(make_snap(now, qps=2.0))
    points = h.query(minutes=600)
    assert [p["qps"] for p in points] == [2.0]
