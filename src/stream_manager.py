"""
StreamManager
=============
Network-layer guard for TCP / QUIC stream slots.

Acts as the *final* authority on how many physical streams the local node
may hold open simultaneously.  All DHT traffic (both user queries and random
walks) flows through this gate before any bytes are sent on the wire.

This is "Layer C" of the dual-gate strategy; the DHTQueryCoordinator holds
"Layer A" (queries) and "Layer B" (random walks), which govern *logical*
operations before they even reach the stream level.

Design goals
------------
* Never exceed ``max_streams`` open streams.
* Guarantee release via ``try … finally`` even on timeout or cancellation.
* Expose live telemetry for the monitoring dashboard.
* Support hot-reload of the stream cap via ``reconfigure()``.
"""

from __future__ import annotations

import logging
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Optional

import trio

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stream lifecycle record
# ---------------------------------------------------------------------------


@dataclass
class StreamRecord:
    stream_id: str
    remote_peer: str
    protocol: str
    opened_at: float
    bytes_sent: int = 0
    bytes_recv: int = 0
    closed: bool = False
    close_reason: Optional[str] = None

    @property
    def age_ms(self) -> float:
        return (time.monotonic() - self.opened_at) * 1000

    def to_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "remote_peer": self.remote_peer[:16] + "…" if len(self.remote_peer) > 16 else self.remote_peer,
            "protocol": self.protocol,
            "age_ms": round(self.age_ms, 1),
            "bytes_sent": self.bytes_sent,
            "bytes_recv": self.bytes_recv,
            "closed": self.closed,
            "close_reason": self.close_reason,
        }


# ---------------------------------------------------------------------------
# StreamManager
# ---------------------------------------------------------------------------


class StreamManager:
    """
    Capacity-limited stream pool backed by ``trio.CapacityLimiter``.

    Usage
    -----
    ::

        async with stream_manager.open_stream(remote_peer, "/libp2p/kad/1.0.0") as stream_id:
            # stream slot is held here
            await do_rpc(stream_id)
        # slot released automatically

    Parameters
    ----------
    max_streams:
        Maximum simultaneous open streams.  Mirrors the libp2p hard limit so
        we never hit it unexpectedly.
    stream_timeout:
        Per-stream deadline.  Prevents zombie streams from occupying slots
        indefinitely.
    """

    def __init__(
        self,
        max_streams: int = 50,
        stream_timeout: float = 60.0,
    ) -> None:
        self._max_streams = max_streams
        self._stream_timeout = stream_timeout

        self._limiter: Optional[trio.CapacityLimiter] = None

        # Telemetry
        self._open_streams: dict[str, StreamRecord] = {}
        self._closed_streams: list[StreamRecord] = []
        self._max_closed_history = 200
        self._total_opened = 0
        self._total_closed = 0
        self._total_errors = 0
        self._total_timeouts = 0
        self._peak_open = 0

        self._stream_counter = 0

    # ------------------------------------------------------------------
    # Bootstrap
    # ------------------------------------------------------------------

    def _bootstrap(self) -> None:
        if self._limiter is None:
            self._limiter = trio.CapacityLimiter(self._max_streams)
            logger.info("StreamManager bootstrapped — max_streams=%d", self._max_streams)

    # ------------------------------------------------------------------
    # Context manager API
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def open_stream(self, remote_peer: str, protocol: str = "/libp2p/kad/1.0.0"):
        """
        Async context manager that acquires a stream slot and yields a stream_id.

        The slot is **always** released in the ``finally`` block — even if the
        caller is cancelled or raises.
        """
        self._bootstrap()
        assert self._limiter is not None

        self._stream_counter += 1
        stream_id = f"s-{self._stream_counter:05d}"

        record = StreamRecord(
            stream_id=stream_id,
            remote_peer=remote_peer,
            protocol=protocol,
            opened_at=time.monotonic(),
        )

        # Acquire stream slot — will block (not hang) if pool is saturated
        async with self._limiter:
            self._open_streams[stream_id] = record
            self._total_opened += 1
            open_count = len(self._open_streams)
            if open_count > self._peak_open:
                self._peak_open = open_count

            try:
                with trio.move_on_after(self._stream_timeout) as cancel_scope:
                    yield stream_id, record

                if cancel_scope.cancelled_caught:
                    self._total_timeouts += 1
                    record.close_reason = "timeout"
                    logger.warning("Stream %s timed out", stream_id)

            except Exception as exc:
                self._total_errors += 1
                record.close_reason = f"error: {exc}"
                logger.debug("Stream %s closed with error: %s", stream_id, exc)
                raise
            finally:
                record.closed = True
                self._open_streams.pop(stream_id, None)
                self._total_closed += 1
                self._closed_streams.append(record)
                if len(self._closed_streams) > self._max_closed_history:
                    self._closed_streams = self._closed_streams[-self._max_closed_history:]
                logger.debug("Stream %s released — pool now %d/%d",
                             stream_id, len(self._open_streams), self._max_streams)

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        limiter = self._limiter
        borrowed = int(limiter.borrowed_tokens) if limiter else 0

        avg_age = 0.0
        if self._open_streams:
            avg_age = sum(r.age_ms for r in self._open_streams.values()) / len(self._open_streams)

        return {
            "stream_manager": {
                "config": {
                    "max_streams": self._max_streams,
                    "stream_timeout_s": self._stream_timeout,
                },
                "pool": {
                    "open": borrowed,
                    "available": self._max_streams - borrowed,
                    "utilisation_pct": round((borrowed / max(self._max_streams, 1)) * 100, 1),
                    "peak_open": self._peak_open,
                    "avg_age_ms": round(avg_age, 1),
                },
                "counters": {
                    "total_opened": self._total_opened,
                    "total_closed": self._total_closed,
                    "errors": self._total_errors,
                    "timeouts": self._total_timeouts,
                },
                "open_streams": [r.to_dict() for r in self._open_streams.values()],
                "recent_closed": [
                    r.to_dict() for r in reversed(self._closed_streams[-10:])
                ],
            }
        }

    def reconfigure(self, max_streams: Optional[int] = None, stream_timeout: Optional[float] = None) -> None:
        if max_streams is not None:
            self._max_streams = max_streams
            if self._limiter:
                self._limiter.total_tokens = max_streams
        if stream_timeout is not None:
            self._stream_timeout = stream_timeout
