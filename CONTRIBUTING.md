# Contributing

## Getting started

Follow [docs/SETUP.md](docs/SETUP.md). Everything below assumes an
activated `.venv` in the repo root.

## Ground rules (the project's binding constraints)

These are invariants, not preferences — CI and the test suite enforce most
of them:

1. **Simulated mode never imports libp2p.** `import src` must work with
   only trio/fastapi/hypercorn/anyio/pydantic installed
   (`tests/test_imports.py` guards this). Real-mode imports live inside
   the `--mode real` branch of `main.py` only.
2. **All async code is trio.** No asyncio APIs, no pytest-asyncio. Tests
   use `@pytest.mark.trio`; time-dependent tests use the pytest-trio
   `autojump_clock` fixture so they run instantly and deterministically.
3. **No new runtime dependencies without a strong reason.** The stdlib
   (sqlite3, statistics, json) and the existing stack cover a lot.
   Frontend libraries are vendored into `static/` (CSP: the dashboard
   makes zero external requests).
4. **Layer naming is fixed:** Layer A = query limiter, Layer B = walk
   sub-limiter, Layer C = stream pool. Keep docstrings and UI labels
   consistent with it.
5. **Tests are deterministic.** If your test depends on the simulation's
   randomness, seed the global RNG and restore state afterwards
   (see `test_scenario_affects_latency` for the pattern). A test that
   flakes will be treated as a bug.

## Making changes

- Branch from `master`; keep commits focused.
- **Every behavior change carries a test** — the two worst bugs in this
  project's history survived precisely because their modules had zero
  coverage.
- Run the full suite before pushing: `pytest tests/ -v`
  (expect all green, 1 skip without libp2p).
- If you touch the dashboard, exercise it in a browser — the suite covers
  the API, not the DOM.

## Commit messages

- Conventional-commit style: `fix:`, `feat:`, `test:`, `docs:`, `ci:`,
  `refactor:`.
- **No AI co-author trailers** (`Co-Authored-By: Claude …` or similar) —
  repository policy; the history was rewritten once to remove them and
  they will not be merged again.

## Pull requests

- CI (`.github/workflows/ci.yml`) must be green — it runs the suite on
  Python 3.12 without libp2p.
- Describe what changed and how you verified it; measured numbers beat
  adjectives (see PR #1 for the house style).
