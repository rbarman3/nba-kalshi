"""Tests for pipeline.processor.NBAProcessor — queue-based lineup event emission."""
import asyncio
import pytest

from pipeline.models import RawSnapshot, LineupChangeEvent
from pipeline.processor import NBAProcessor


def make_snapshot(game_id: str, home_oncourt: list[str], away_oncourt: list[str], fetched_at: float = 1000.0) -> RawSnapshot:
    """Build a minimal RawSnapshot with the given on-court player IDs."""
    return RawSnapshot(
        game_id=game_id,
        payload={
            "game": {
                "period": 1,
                "gameClock": "PT10M00.00S",
                "homeTeam": {
                    "players": [{"personId": pid, "oncourt": "1"} for pid in home_oncourt]
                },
                "awayTeam": {
                    "players": [{"personId": pid, "oncourt": "1"} for pid in away_oncourt]
                },
            }
        },
        fetched_at=fetched_at,
    )


@pytest.fixture
def queues():
    return asyncio.Queue(), asyncio.Queue()


class TestNBAProcessorFirstSnapshot:
    """First snapshot per game initializes state but emits no event."""

    @pytest.mark.asyncio
    async def test_first_snapshot_emits_nothing(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)
        snapshot = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"])

        await in_q.put(snapshot)
        await processor.run_once()

        assert out_q.empty()

    @pytest.mark.asyncio
    async def test_first_snapshot_stores_state(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)
        snapshot = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"])

        await in_q.put(snapshot)
        await processor.run_once()

        assert "game1" in processor._lineup_state


class TestNBAProcessorSubstitution:
    """Consecutive snapshots with lineup change emit LineupChangeEvent."""

    @pytest.mark.asyncio
    async def test_substitution_emits_event(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)

        snap1 = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0)
        snap2 = make_snapshot("game1", ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0)

        await in_q.put(snap1)
        await processor.run_once()

        await in_q.put(snap2)
        await processor.run_once()

        assert not out_q.empty()
        event = out_q.get_nowait()
        assert isinstance(event, LineupChangeEvent)

    @pytest.mark.asyncio
    async def test_substitution_event_players_correct(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)

        snap1 = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"])
        snap2 = make_snapshot("game1", ["p1", "p5"], ["p3", "p4"])

        await in_q.put(snap1)
        await processor.run_once()
        await in_q.put(snap2)
        await processor.run_once()

        event = out_q.get_nowait()
        assert "p5" in event.players_in
        assert "p2" in event.players_out

    @pytest.mark.asyncio
    async def test_no_change_emits_nothing(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)

        snap1 = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"])
        snap2 = make_snapshot("game1", ["p1", "p2"], ["p3", "p4"])

        await in_q.put(snap1)
        await processor.run_once()
        await in_q.put(snap2)
        await processor.run_once()

        assert out_q.empty()


class TestNBAProcessorMultiGame:
    """State is tracked independently per game."""

    @pytest.mark.asyncio
    async def test_two_games_tracked_independently(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)

        await in_q.put(make_snapshot("game1", ["p1", "p2"], ["p3", "p4"]))
        await processor.run_once()
        await in_q.put(make_snapshot("game2", ["p5", "p6"], ["p7", "p8"]))
        await processor.run_once()

        assert "game1" in processor._lineup_state
        assert "game2" in processor._lineup_state
        assert out_q.empty()

    @pytest.mark.asyncio
    async def test_change_in_one_game_does_not_affect_other(self, queues):
        in_q, out_q = queues
        processor = NBAProcessor(in_q, out_q)

        # Initialize both games
        await in_q.put(make_snapshot("game1", ["p1", "p2"], ["p3", "p4"]))
        await processor.run_once()
        await in_q.put(make_snapshot("game2", ["p5", "p6"], ["p7", "p8"]))
        await processor.run_once()

        # Sub in game1 only
        await in_q.put(make_snapshot("game1", ["p1", "p9"], ["p3", "p4"]))
        await processor.run_once()

        # Processor emits LineupChangeEvent + per-player SubstitutionEvents
        # for a single lineup change. All should belong to game1.
        events = []
        while not out_q.empty():
            events.append(out_q.get_nowait())
        assert len(events) >= 1
        assert all(e.game_id == "game1" for e in events)
