"""
DHTQueryCoordinator
===================
Application-layer DHT query management using trio.CapacityLimiter.

Implements the dual-layer concurrency limiting strategy described in the
Technical Design Doc: Resolving DHT Resource Exhaustion.

Layer A  →  query_limiter      : hard cap on ALL concurrent DHT queries
Layer B  →  random_walk_limiter: sub-cap on BACKGROUND maintenance walks
           (always ≤ Layer A capacity)

This guarantees user-initiated find_peer calls are never starved by
background random walks, and physical stream slots are never blindly
exhausted.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Optional

import trio

logger = logging.getLogger(__name__)


def _percentile(sorted_vals: list[float], pct: float) -> float:
    """Linear-interpolated percentile; sorted_vals must be pre-sorted."""
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


# ---------------------------------------------------------------------------
# Domain types
# ---------------------------------------------------------------------------


class QueryStatus(str, Enum):
    PENDING = "pending"
    ACQUIRING = "acquiring"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


class QueryPriority(str, Enum):
    USER = "user"
    BACKGROUND = "background"


@dataclass
class QueryResult:
    query_id: str
    peer_id: str
    status: QueryStatus
    priority: QueryPriority
    duration_ms: float
    found: bool = False
    closest_peers: list[str] = field(default_factory=list)
    hops: int = 0
    path: list[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "query_id": self.query_id,
            "peer_id": self.peer_id[:20] + "…" if len(self.peer_id) > 20 else self.peer_id,
            "peer_id_full": self.peer_id,
            "status": self.status,
            "priority": self.priority,
            "duration_ms": round(self.duration_ms, 2),
            "found": self.found,
            "closest_peers": self.closest_peers[:5],
            "hops": self.hops,
            "path": self.path,
            "error": self.error,
        }


# ---------------------------------------------------------------------------
# Live query tracking
# ---------------------------------------------------------------------------


@dataclass
class LiveQuery:
    query_id: str
    peer_id: str
    priority: QueryPriority
    status: QueryStatus
    started_at: float
    limiter_wait_ms: float = 0.0

    def to_dict(self) -> dict:
        elapsed_ms = (trio.current_time() - self.started_at) * 1000
        return {
            "query_id": self.query_id,
            "peer_id": self.peer_id[:20] + "…" if len(self.peer_id) > 20 else self.peer_id,
            "priority": self.priority,
            "status": self.status,
            "elapsed_ms": round(elapsed_ms, 1),
            "limiter_wait_ms": round(self.limiter_wait_ms, 1),
        }


# ---------------------------------------------------------------------------
# Main coordinator
# ---------------------------------------------------------------------------


class DHTQueryCoordinator:
    """
    Coordinates DHT lookups with strict resource budgets.

    Parameters
    ----------
    max_concurrent_queries:
        Hard cap on ALL simultaneous DHT operations (user + background).
        Maps directly to ``trio.CapacityLimiter(max_concurrent_queries)``.
    max_random_walks:
        Sub-cap on background maintenance walks.  Must be < max_concurrent_queries
        so that user queries always have reserved capacity.
    query_timeout:
        Per-query deadline in seconds enforced via ``trio.move_on_after``.
    on_snapshot:
        Optional async callback invoked with coordinator state after every
        status transition (useful for WebSocket broadcasting).
    """

    def __init__(
        self,
        max_concurrent_queries: int = 10,
        max_random_walks: int = 3,
        query_timeout: float = 30.0,
        on_snapshot: Optional[Callable[[dict], Awaitable[None]]] = None,
    ) -> None:
        if max_random_walks >= max_concurrent_queries:
            raise ValueError(
                "max_random_walks must be < max_concurrent_queries "
                "to preserve capacity for user queries."
            )

        self._max_queries = max_concurrent_queries
        self._max_walks = max_random_walks
        self._query_timeout = query_timeout
        self._on_snapshot = on_snapshot

        # Lazily initialised inside trio event loop
        self._query_limiter: Optional[trio.CapacityLimiter] = None
        self._rw_limiter: Optional[trio.CapacityLimiter] = None

        # Counters
        self._total = 0
        self._successes = 0
        self._failures = 0
        self._timeouts = 0
        self._cancelled = 0
        self._total_duration_ms = 0.0
        self._peak_concurrent = 0

        # Live tracking
        self._live: dict[str, LiveQuery] = {}

        # Rolling result history (last N)
        self._history: list[QueryResult] = []
        self._max_history = 200

        # Throughput window (timestamps of completed queries)
        self._completion_times: list[float] = []
        self._throughput_window = 10.0  # seconds

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _bootstrap(self) -> None:
        """Must be called from inside a running trio event loop."""
        if self._query_limiter is None:
            self._query_limiter = trio.CapacityLimiter(self._max_queries)
            self._rw_limiter = trio.CapacityLimiter(self._max_walks)
            logger.info(
                "DHTQueryCoordinator bootstrapped — "
                "query_cap=%d  walk_cap=%d  timeout=%.1fs",
                self._max_queries,
                self._max_walks,
                self._query_timeout,
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def find_peer(
        self,
        peer_id: str,
        query_fn: Callable[[str], Awaitable[tuple[bool, list[str], int]]],
        priority: QueryPriority = QueryPriority.USER,
    ) -> QueryResult:
        """
        Locate *peer_id* via the supplied async *query_fn*.

        ``query_fn(peer_id)`` must return ``(found: bool, closest_peers: list[str], hops: int)``
        or ``(found: bool, closest_peers: list[str], hops: int, path: list[str])``.

        Resource lifecycle
        ------------------
        1. Acquire capacity slot (blocks if saturated → graceful back-pressure)
        2. Execute with hard timeout (``trio.move_on_after``)
        3. Release slot in ``finally`` — guaranteed even on cancellation
        """
        self._bootstrap()

        query_id = f"q-{str(uuid.uuid4())[:8]}"
        started_at = trio.current_time()

        lq = LiveQuery(
            query_id=query_id,
            peer_id=peer_id,
            priority=priority,
            status=QueryStatus.ACQUIRING,
            started_at=started_at,
        )
        self._live[query_id] = lq
        self._total += 1
        await self._emit()

        try:
            result = await self._run_with_capacity(lq, query_fn, started_at)
        except trio.Cancelled:
            duration_ms = (trio.current_time() - started_at) * 1000
            result = QueryResult(
                query_id=query_id,
                peer_id=peer_id,
                status=QueryStatus.CANCELLED,
                priority=priority,
                duration_ms=duration_ms,
            )
            self._cancelled += 1
        finally:
            self._live.pop(query_id, None)
            await self._emit()

        self._record(result)
        return result

    # ------------------------------------------------------------------
    # Internal execution
    # ------------------------------------------------------------------

    async def _run_with_capacity(
        self,
        lq: LiveQuery,
        query_fn: Callable,
        started_at: float,
    ) -> QueryResult:
        """Acquire limiter(s) then execute with timeout."""
        assert self._query_limiter is not None
        assert self._rw_limiter is not None

        limiter_wait_start = trio.current_time()

        # Background walks must acquire BOTH limiters (walk ⊆ query budget).
        # User queries acquire only the main limiter.
        if lq.priority == QueryPriority.BACKGROUND:
            async with self._rw_limiter:
                async with self._query_limiter:
                    lq.limiter_wait_ms = (trio.current_time() - limiter_wait_start) * 1000
                    return await self._execute(lq, query_fn, started_at)
        else:
            async with self._query_limiter:
                lq.limiter_wait_ms = (trio.current_time() - limiter_wait_start) * 1000
                return await self._execute(lq, query_fn, started_at)

    async def _execute(
        self,
        lq: LiveQuery,
        query_fn: Callable,
        started_at: float,
    ) -> QueryResult:
        """Run query_fn with mandatory timeout and structured error handling."""
        lq.status = QueryStatus.RUNNING

        # Track peak concurrency
        running_count = sum(
            1 for q in self._live.values() if q.status == QueryStatus.RUNNING
        )
        if running_count > self._peak_concurrent:
            self._peak_concurrent = running_count

        await self._emit()

        with trio.move_on_after(self._query_timeout) as cancel_scope:
            try:
                res = await query_fn(lq.peer_id)
                found, closest_peers, hops, *rest = res
                path = list(rest[0]) if rest else []
                duration_ms = (trio.current_time() - started_at) * 1000
                self._successes += 1
                self._total_duration_ms += duration_ms
                return QueryResult(
                    query_id=lq.query_id,
                    peer_id=lq.peer_id,
                    status=QueryStatus.SUCCESS,
                    priority=lq.priority,
                    duration_ms=duration_ms,
                    found=found,
                    closest_peers=closest_peers,
                    hops=hops,
                    path=path,
                )
            except Exception as exc:
                duration_ms = (trio.current_time() - started_at) * 1000
                self._failures += 1
                logger.warning("Query %s failed: %s", lq.query_id, exc)
                return QueryResult(
                    query_id=lq.query_id,
                    peer_id=lq.peer_id,
                    status=QueryStatus.FAILED,
                    priority=lq.priority,
                    duration_ms=duration_ms,
                    error=str(exc),
                )

        # cancel_scope fell through → timeout
        duration_ms = (trio.current_time() - started_at) * 1000
        self._timeouts += 1
        logger.warning(
            "Query %s timed out after %.1fs", lq.query_id, self._query_timeout
        )
        return QueryResult(
            query_id=lq.query_id,
            peer_id=lq.peer_id,
            status=QueryStatus.TIMEOUT,
            priority=lq.priority,
            duration_ms=duration_ms,
            error=f"Timed out after {self._query_timeout:.1f}s",
        )

    # ------------------------------------------------------------------
    # History & metrics
    # ------------------------------------------------------------------

    def _record(self, result: QueryResult) -> None:
        now = trio.current_time()
        self._completion_times.append(now)
        # Trim to window
        cutoff = now - self._throughput_window
        self._completion_times = [t for t in self._completion_times if t >= cutoff]

        self._history.append(result)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

    async def _emit(self) -> None:
        if self._on_snapshot:
            try:
                await self._on_snapshot(self.snapshot())
            except Exception:
                logger.exception("on_snapshot callback failed")

    # ------------------------------------------------------------------
    # Snapshot (serialisable)
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        ql = self._query_limiter
        rwl = self._rw_limiter

        ql_borrowed = int(ql.borrowed_tokens) if ql else 0
        rwl_borrowed = int(rwl.borrowed_tokens) if rwl else 0

        total_done = self._successes + self._failures + self._timeouts + self._cancelled
        success_rate = (self._successes / max(total_done, 1)) * 100

        throughput = len(self._completion_times) / self._throughput_window

        durations = sorted(r.duration_ms for r in self._history)

        return {
            "coordinator": {
                "config": {
                    "max_concurrent_queries": self._max_queries,
                    "max_random_walks": self._max_walks,
                    "query_timeout_s": self._query_timeout,
                },
                "counters": {
                    "total": self._total,
                    "success": self._successes,
                    "failed": self._failures,
                    "timeout": self._timeouts,
                    "cancelled": self._cancelled,
                },
                "rates": {
                    "success_pct": round(success_rate, 1),
                    "throughput_qps": round(throughput, 2),
                    "avg_duration_ms": round(
                        self._total_duration_ms / max(self._successes, 1), 1
                    ),
                    "p50_ms": round(_percentile(durations, 0.50), 1),
                    "p95_ms": round(_percentile(durations, 0.95), 1),
                    "p99_ms": round(_percentile(durations, 0.99), 1),
                },
                "concurrency": {
                    "current": len(
                        [q for q in self._live.values() if q.status == QueryStatus.RUNNING]
                    ),
                    "peak": self._peak_concurrent,
                    "acquiring": len(
                        [q for q in self._live.values() if q.status == QueryStatus.ACQUIRING]
                    ),
                },
            },
            "capacity_limiter": {
                "label": "Query Pool (Layer A)",
                "total": self._max_queries,
                "borrowed": ql_borrowed,
                "available": self._max_queries - ql_borrowed,
                "utilisation_pct": round((ql_borrowed / max(self._max_queries, 1)) * 100, 1),
            },
            "random_walk_limiter": {
                "label": "Walk Pool (Layer B)",
                "total": self._max_walks,
                "borrowed": rwl_borrowed,
                "available": self._max_walks - rwl_borrowed,
                "utilisation_pct": round((rwl_borrowed / max(self._max_walks, 1)) * 100, 1),
            },
            "live_queries": [q.to_dict() for q in self._live.values()],
            "recent_results": [r.to_dict() for r in reversed(self._history[-30:])],
        }

    # ------------------------------------------------------------------
    # Control plane
    # ------------------------------------------------------------------

    def reconfigure(
        self,
        max_concurrent_queries: Optional[int] = None,
        max_random_walks: Optional[int] = None,
        query_timeout: Optional[float] = None,
    ) -> None:
        """Hot-reload configuration (takes effect on next query acquisition)."""
        if query_timeout is not None:
            self._query_timeout = query_timeout

        if max_concurrent_queries is not None:
            self._max_queries = max_concurrent_queries
            if self._query_limiter:
                self._query_limiter.total_tokens = max_concurrent_queries

        if max_random_walks is not None:
            self._max_walks = max_random_walks
            if self._rw_limiter:
                self._rw_limiter.total_tokens = max_random_walks
