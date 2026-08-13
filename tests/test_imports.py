"""Simulated mode must not require libp2p (README: 'no external dependencies')."""
import sys


def test_src_imports_without_libp2p():
    for mod in [m for m in list(sys.modules) if m == "src" or m.startswith("src.")]:
        del sys.modules[mod]
    import src  # noqa: F401

    assert "libp2p" not in sys.modules, (
        "importing src must not pull in libp2p — simulated mode is dependency-light"
    )
