"""Tests for pipeline.watchdog — FeedWatchdog health monitoring."""
import asyncio
import time
from unittest.mock import MagicMock, patch

import pytest

from pipeline.models import FeedHealthEvent, RawSnapshot
from pipeline.watchdog import FeedWatchdog


def make_transport(game_ids: list[str], cache: dict[str, RawSnapshot] | None = None) -> MagicMock:
    """Build a mock NBATransport with the given game_ids and cache."""
    transport = MagicMock()
    transport.game_ids = game_ids
    transport.cache = cache or {}
    return transport


def make_snapshot(game_id: str, fetched_at: float) -> RawSnapshot:
    return RawSnapshot(game_id=game_id, payload={}, fetched_at=fetched_at)


class TestFeedHealthEvent:
    def test_frozen(self):
        event = FeedHealthEvent(
            status="HEALTHY",
            game_id="game1",
            last_success=1000.0,
            observed_at=1000.0,
            message="ok",
        )
        with pytest.raises((AttributeError, TypeError)):
            event.status = "DEAD"  # type: ignore


class TestFeedWatchdogInit:
    def test_stores_transport_and_queue(self):
        transport = make_transport(["game1"])
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)
        assert w.transport is transport
        assert w.health_queue is q


class TestClassifyGame:
    def test_fresh_entry_is_healthy(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 1.0)})
        w = FeedWatchdog(transport, asyncio.Queue())
        assert w._classify_game("game1", now) == "HEALTHY"

    def test_stale_entry_is_degraded(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 10.0)})
        w = FeedWatchdog(transport, asyncio.Queue())
        assert w._classify_game("game1", now) == "DEGRADED"

    def test_very_stale_entry_is_dead(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 60.0)})
        w = FeedWatchdog(transport, asyncio.Queue())
        assert w._classify_game("game1", now) == "DEAD"

    def test_missing_cache_entry_is_dead(self):
        now = time.time()
        transport = make_transport(["game1"], {})
        w = FeedWatchdog(transport, asyncio.Queue())
        assert w._classify_game("game1", now) == "DEAD"


class TestClassifyOverall:
    def test_any_healthy_wins(self):
        w = FeedWatchdog(make_transport([]), asyncio.Queue())
        assert w._classify_overall(["HEALTHY", "DEGRADED", "DEAD"]) == "HEALTHY"

    def test_degraded_beats_dead(self):
        w = FeedWatchdog(make_transport([]), asyncio.Queue())
        assert w._classify_overall(["DEGRADED", "DEAD"]) == "DEGRADED"

    def test_all_dead_is_dead(self):
        w = FeedWatchdog(make_transport([]), asyncio.Queue())
        assert w._classify_overall(["DEAD", "DEAD"]) == "DEAD"

    def test_empty_list_is_dead(self):
        w = FeedWatchdog(make_transport([]), asyncio.Queue())
        assert w._classify_overall([]) == "DEAD"


class TestCheckHealth:
    @pytest.mark.asyncio
    async def test_first_check_emits_event(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 1.0)})
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)
        await w.run_once()
        assert not q.empty()

    @pytest.mark.asyncio
    async def test_no_duplicate_emit_on_same_status(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 1.0)})
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)
        await w.run_once()
        await w.run_once()
        # Drain queue — should only have initial per-game + overall events, not duplicates
        events = []
        while not q.empty():
            events.append(q.get_nowait())
        statuses = [e.status for e in events]
        assert statuses.count("HEALTHY") == 2  # one per-game, one overall

    @pytest.mark.asyncio
    async def test_transition_healthy_to_degraded_emits_new_event(self):
        now = time.time()
        snapshot = make_snapshot("game1", now - 1.0)
        transport = make_transport(["game1"], {"game1": snapshot})
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)

        await w.run_once()
        # Drain initial events
        while not q.empty():
            q.get_nowait()

        # Now make the snapshot stale
        transport.cache["game1"] = make_snapshot("game1", now - 10.0)
        await w.run_once()

        events = []
        while not q.empty():
            events.append(q.get_nowait())
        assert any(e.status == "DEGRADED" for e in events)

    @pytest.mark.asyncio
    async def test_empty_cache_emits_overall_dead(self):
        transport = make_transport(["game1"], {})
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)
        await w.run_once()

        events = []
        while not q.empty():
            events.append(q.get_nowait())

        overall = [e for e in events if e.game_id is None]
        assert any(e.status == "DEAD" for e in overall)

    @pytest.mark.asyncio
    async def test_event_has_correct_game_id(self):
        now = time.time()
        transport = make_transport(["game1"], {"game1": make_snapshot("game1", now - 1.0)})
        q = asyncio.Queue()
        w = FeedWatchdog(transport, q)
        await w.run_once()

        events = []
        while not q.empty():
            events.append(q.get_nowait())

        per_game = [e for e in events if e.game_id == "game1"]
        assert len(per_game) == 1
