import json

import pytest

from src.experiment import run_experiment

TINY = {
    "name": "tiny",
    "network": {"nodes": 20, "scenario": "STRESSED"},
    "workload": {"qps": 8.0, "duration_s": 5},
    "arms": {
        "unprotected": {"max_queries": 10000, "max_walks": 9999,
                        "max_streams": 10000, "query_timeout": 10.0},
        "protected": {"max_queries": 4, "max_walks": 2,
                      "max_streams": 20, "query_timeout": 10.0},
    },
}


@pytest.mark.trio
async def test_experiment_runs_both_arms(autojump_clock):
    result = await run_experiment(TINY)

    assert set(result["arms"]) == {"unprotected", "protected"}
    for arm in result["arms"].values():
        assert arm["counters"]["total"] > 0
        assert "p95_ms" in arm["rates"]

    protected = result["arms"]["protected"]
    # The invariant the whole project exists to prove:
    assert protected["peak_borrowed"] <= TINY["arms"]["protected"]["max_queries"]
    assert protected["peak_concurrency"] <= TINY["arms"]["protected"]["max_queries"]
    # Unprotected must actually build up more concurrency than the cap allows:
    assert result["arms"]["unprotected"]["peak_concurrency"] > 4


@pytest.mark.trio
async def test_report_files(tmp_path, autojump_clock):
    result = await run_experiment(TINY)
    from src.experiment import write_report

    json_path, html_path = write_report(result, tmp_path, stamp="20260813-120000")
    assert json_path.exists() and html_path.exists()
    loaded = json.loads(json_path.read_text())
    assert set(loaded["arms"]) == {"unprotected", "protected"}
    html = html_path.read_text()
    assert "unprotected" in html and "protected" in html
    assert html.startswith("<!doctype html>")
