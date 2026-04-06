"""Tests for pipeline.replayer — snapshot replay engine for backtesting."""
import asyncio
import tempfile
import time
from pathlib import Path

import pytest

from pipeline.models import RawSnapshot, LineupChangeEvent
from pipeline.replayer import SnapshotReplayer, ReplayResult
from pipeline.store import SnapshotStore

GAME_ID = "0022500001"
DATE = "2026-03-29"


def make_snapshot(
    game_id: str,
    home_oncourt: list[str],
    away_oncourt: list[str],
    fetched_at: float = 1000.0,
) -> RawSnapshot:
    """Build a minimal RawSnapshot with the given on-court player IDs."""
    return RawSnapshot(
        game_id=game_id,
        payload={
            "game": {
                "period": 1,
                "gameClock": "PT10M00.00S",
                "homeTeam": {
                    "score": 0,
                    "players": [
                        {"personId": pid, "oncourt": "1"} for pid in home_oncourt
                    ],
                },
                "awayTeam": {
                    "score": 0,
                    "players": [
                        {"personId": pid, "oncourt": "1"} for pid in away_oncourt
                    ],
                },
            }
        },
        fetched_at=fetched_at,
    )


@pytest.fixture
def temp_store():
    """SnapshotStore backed by a temporary directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield SnapshotStore(base_dir=tmpdir)


# ------------------------------------------------------------------
# replay_snapshots — core loop
# ------------------------------------------------------------------


class TestReplaySnapshots:
    """Test the core replay loop with in-memory snapshots."""

    @pytest.mark.asyncio
    async def test_single_snapshot_emits_nothing(self):
        """First snapshot initializes processor state, no events."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"])]
        result = await replayer.replay_snapshots(snapshots)

        assert result.events == []
        assert result.snapshot_count == 1

    @pytest.mark.asyncio
    async def test_two_snapshots_same_lineup_no_events(self):
        """Identical consecutive snapshots produce no events."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1001.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert result.events == []
        assert result.snapshot_count == 2

    @pytest.mark.asyncio
    async def test_lineup_change_emits_event(self):
        """Substitution between snapshots produces a LineupChangeEvent."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert len(result.events) == 1
        assert isinstance(result.events[0], LineupChangeEvent)
        assert "p5" in result.events[0].players_in
        assert "p2" in result.events[0].players_out

    @pytest.mark.asyncio
    async def test_result_game_id(self):
        """ReplayResult.game_id matches the snapshots."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [make_snapshot(GAME_ID, ["p1"], ["p3"])]
        result = await replayer.replay_snapshots(snapshots)

        assert result.game_id == GAME_ID

    @pytest.mark.asyncio
    async def test_result_snapshot_count(self):
        """ReplayResult.snapshot_count matches number of snapshots fed."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1"], ["p3"], fetched_at=1000.0 + i)
            for i in range(5)
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert result.snapshot_count == 5

    @pytest.mark.asyncio
    async def test_empty_snapshots_returns_empty_result(self):
        """Empty snapshot list returns zero-count result."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        result = await replayer.replay_snapshots([])

        assert result.events == []
        assert result.snapshot_count == 0

    @pytest.mark.asyncio
    async def test_multiple_changes_emits_multiple_events(self):
        """Multiple substitutions across snapshots produce multiple events."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p6"], fetched_at=1002.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert len(result.events) == 2

    @pytest.mark.asyncio
    async def test_fresh_processor_per_call(self):
        """Each replay_snapshots call uses a fresh processor — no state leakage."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]

        result1 = await replayer.replay_snapshots(snapshots)
        result2 = await replayer.replay_snapshots(snapshots)

        assert len(result1.events) == len(result2.events)


# ------------------------------------------------------------------
# replay_game — loads from store
# ------------------------------------------------------------------


class TestReplayGame:
    """Test replay_game which loads from SnapshotStore."""

    @pytest.mark.asyncio
    async def test_replay_game_loads_from_store(self, temp_store):
        """replay_game loads snapshots via store.load and replays them."""
        snap1 = make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0)
        snap2 = make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0)
        await temp_store.persist(snap1, date=DATE)
        await temp_store.persist(snap2, date=DATE)

        replayer = SnapshotReplayer(store=temp_store, mode="fast")
        result = await replayer.replay_game(GAME_ID, DATE)

        assert result.snapshot_count == 2
        assert len(result.events) == 1
        assert isinstance(result.events[0], LineupChangeEvent)

    @pytest.mark.asyncio
    async def test_replay_game_no_snapshots(self, temp_store):
        """replay_game returns empty result when store has no data."""
        replayer = SnapshotReplayer(store=temp_store, mode="fast")
        result = await replayer.replay_game(GAME_ID, DATE)

        assert result.snapshot_count == 0
        assert result.events == []


# ------------------------------------------------------------------
# replay_date — replays all games for a date
# ------------------------------------------------------------------


class TestReplayDate:
    """Test replay_date which replays all games for a date."""

    @pytest.mark.asyncio
    async def test_replay_date_multiple_games(self, temp_store):
        """replay_date returns one ReplayResult per game."""
        game_a = "0022500001"
        game_b = "0022500002"

        await temp_store.persist(
            make_snapshot(game_a, ["p1"], ["p3"], fetched_at=1000.0), date=DATE
        )
        await temp_store.persist(
            make_snapshot(game_b, ["p5"], ["p7"], fetched_at=1000.0), date=DATE
        )

        replayer = SnapshotReplayer(store=temp_store, mode="fast")
        results = await replayer.replay_date(DATE)

        assert len(results) == 2
        game_ids = {r.game_id for r in results}
        assert game_ids == {game_a, game_b}

    @pytest.mark.asyncio
    async def test_replay_date_no_games(self, temp_store):
        """replay_date returns empty list when no games exist."""
        replayer = SnapshotReplayer(store=temp_store, mode="fast")
        results = await replayer.replay_date("2099-01-01")

        assert results == []


# ------------------------------------------------------------------
# Timing modes
# ------------------------------------------------------------------


class TestReplayTiming:
    """Test fast vs realtime modes."""

    @pytest.mark.asyncio
    async def test_fast_mode_is_instant(self):
        """Fast mode doesn't sleep between snapshots."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1"], ["p3"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1"], ["p3"], fetched_at=1010.0),
        ]

        start = time.monotonic()
        await replayer.replay_snapshots(snapshots)
        elapsed = time.monotonic() - start

        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_realtime_mode_respects_timing(self):
        """Realtime mode sleeps based on fetched_at differences."""
        replayer = SnapshotReplayer(store=None, mode="realtime")
        snapshots = [
            make_snapshot(GAME_ID, ["p1"], ["p3"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1"], ["p3"], fetched_at=1000.1),
        ]

        start = time.monotonic()
        await replayer.replay_snapshots(snapshots)
        elapsed = time.monotonic() - start

        assert elapsed >= 0.08

    @pytest.mark.asyncio
    async def test_result_duration_seconds(self):
        """ReplayResult.duration_seconds reflects actual wall time."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [make_snapshot(GAME_ID, ["p1"], ["p3"])]
        result = await replayer.replay_snapshots(snapshots)

        assert result.duration_seconds >= 0
        assert result.duration_seconds < 1.0


# ------------------------------------------------------------------
# stream_game — async generator (anti-look-ahead)
# ------------------------------------------------------------------


class TestStreamGame:
    """Test async generator that yields events one at a time."""

    @pytest.mark.asyncio
    async def test_stream_yields_events_individually(self):
        """stream_snapshots yields one event at a time, not a batch."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p6"], fetched_at=1002.0),
        ]
        events = []
        async for event in replayer.stream_snapshots(snapshots):
            events.append(event)

        assert len(events) == 2
        assert isinstance(events[0], LineupChangeEvent)
        assert isinstance(events[1], LineupChangeEvent)

    @pytest.mark.asyncio
    async def test_stream_no_events_yields_nothing(self):
        """stream_snapshots yields nothing when no changes occur."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1001.0),
        ]
        events = [e async for e in replayer.stream_snapshots(snapshots)]

        assert events == []

    @pytest.mark.asyncio
    async def test_stream_empty_snapshots(self):
        """stream_snapshots handles empty input."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        events = [e async for e in replayer.stream_snapshots([])]

        assert events == []

    @pytest.mark.asyncio
    async def test_stream_prevents_look_ahead(self):
        """Events are yielded as they occur — consumer can't see future events."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p6"], fetched_at=1002.0),
        ]
        seen_at_yield = []
        total_so_far = 0
        async for event in replayer.stream_snapshots(snapshots):
            total_so_far += 1
            seen_at_yield.append(total_so_far)

        # Each event was yielded individually — first yield saw 1, second saw 2
        assert seen_at_yield == [1, 2]

    @pytest.mark.asyncio
    async def test_stream_game_loads_from_store(self, temp_store):
        """stream_game loads from store and yields events."""
        snap1 = make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0)
        snap2 = make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0)
        await temp_store.persist(snap1, date=DATE)
        await temp_store.persist(snap2, date=DATE)

        replayer = SnapshotReplayer(store=temp_store, mode="fast")
        events = [e async for e in replayer.stream_game(GAME_ID, DATE)]

        assert len(events) == 1
        assert isinstance(events[0], LineupChangeEvent)

    @pytest.mark.asyncio
    async def test_stream_realtime_respects_timing(self):
        """stream_snapshots in realtime mode paces by fetched_at."""
        replayer = SnapshotReplayer(store=None, mode="realtime")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1000.1),
        ]

        start = time.monotonic()
        events = [e async for e in replayer.stream_snapshots(snapshots)]
        elapsed = time.monotonic() - start

        assert len(events) == 1
        assert elapsed >= 0.08


# ------------------------------------------------------------------
# signal_delay — latency bias correction
# ------------------------------------------------------------------


class TestSignalDelay:
    """Test signal_delay offsets observed_at on emitted events."""

    @pytest.mark.asyncio
    async def test_signal_delay_offsets_observed_at(self):
        """Events have observed_at shifted forward by signal_delay."""
        replayer = SnapshotReplayer(store=None, mode="fast", signal_delay=2.5)
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert len(result.events) == 1
        # observed_at should be fetched_at + signal_delay
        assert result.events[0].observed_at == 1001.0 + 2.5

    @pytest.mark.asyncio
    async def test_zero_delay_preserves_original(self):
        """signal_delay=0 leaves observed_at unchanged."""
        replayer = SnapshotReplayer(store=None, mode="fast", signal_delay=0.0)
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert result.events[0].observed_at == 1001.0

    @pytest.mark.asyncio
    async def test_default_delay_is_zero(self):
        """Default signal_delay is 0 — no offset."""
        replayer = SnapshotReplayer(store=None, mode="fast")
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert result.events[0].observed_at == 1001.0

    @pytest.mark.asyncio
    async def test_signal_delay_applies_to_stream(self):
        """signal_delay also applies in streaming mode."""
        replayer = SnapshotReplayer(store=None, mode="fast", signal_delay=3.0)
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
        ]
        events = [e async for e in replayer.stream_snapshots(snapshots)]

        assert len(events) == 1
        assert events[0].observed_at == 1001.0 + 3.0

    @pytest.mark.asyncio
    async def test_signal_delay_multiple_events(self):
        """Each event gets its own offset based on its own fetched_at."""
        replayer = SnapshotReplayer(store=None, mode="fast", signal_delay=1.0)
        snapshots = [
            make_snapshot(GAME_ID, ["p1", "p2"], ["p3", "p4"], fetched_at=1000.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p4"], fetched_at=1001.0),
            make_snapshot(GAME_ID, ["p1", "p5"], ["p3", "p6"], fetched_at=1002.0),
        ]
        result = await replayer.replay_snapshots(snapshots)

        assert result.events[0].observed_at == 1002.0
        assert result.events[1].observed_at == 1003.0
