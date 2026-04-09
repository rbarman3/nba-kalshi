"""Tests for ESPNReplayer."""
import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipeline.espn_replayer import ESPNReplayer, _parse_wallclock
from pipeline.models import (
    ReplayResult,
    ScoringPlayEvent,
    FoulEvent,
    TimeoutEvent,
)


# Test helper: create minimal event instances
def make_scoring_event(
    game_id: str = "401584793",
    sequence: int = 1,
    wallclock: str = "2026-03-29T00:00:00Z",
    observed_at: float = 1000.0,
) -> ScoringPlayEvent:
    return ScoringPlayEvent(
        game_id=game_id,
        espn_play_id="1",
        sequence=sequence,
        period=1,
        clock="12:00",
        home_score=0,
        away_score=2,
        score_value=2,
        team_id="2",
        player_id="123",
        play_type="Jump Shot",
        text="Test scoring play",
        wallclock=wallclock,
        observed_at=observed_at,
    )


def make_foul_event(
    game_id: str = "401584793",
    sequence: int = 2,
    wallclock: str = "2026-03-29T00:01:00Z",
    observed_at: float = 1060.0,
) -> FoulEvent:
    return FoulEvent(
        game_id=game_id,
        espn_play_id="2",
        sequence=sequence,
        period=1,
        clock="11:00",
        home_score=0,
        away_score=2,
        team_id="13",
        player_id="456",
        foul_type="Shooting Foul",
        text="Test foul",
        wallclock=wallclock,
        observed_at=observed_at,
    )


@pytest.fixture
def mock_store():
    """Create a mock ESPNStore."""
    store = MagicMock()
    return store


@pytest.fixture
def replayer_fast(mock_store):
    """Create a replayer in fast mode."""
    return ESPNReplayer(store=mock_store, mode="fast", signal_delay=0.0)


@pytest.fixture
def replayer_realtime(mock_store):
    """Create a replayer in realtime mode."""
    return ESPNReplayer(store=mock_store, mode="realtime", signal_delay=0.0)


class TestParseWallclock:
    """Tests for wallclock parsing utility."""

    def test_parse_iso8601_with_z_suffix(self):
        """Parse ISO 8601 string ending with Z."""
        timestamp = _parse_wallclock("2026-03-29T00:00:00Z")
        assert isinstance(timestamp, float)
        assert timestamp > 0

    def test_parse_iso8601_with_timezone_offset(self):
        """Parse ISO 8601 string with timezone offset."""
        timestamp = _parse_wallclock("2026-03-29T00:00:00+00:00")
        assert isinstance(timestamp, float)
        assert timestamp > 0

    def test_parse_returns_same_time_for_z_and_offset(self):
        """Z suffix and +00:00 offset represent same instant."""
        ts_z = _parse_wallclock("2026-03-29T00:00:00Z")
        ts_offset = _parse_wallclock("2026-03-29T00:00:00+00:00")
        assert ts_z == ts_offset


class TestESPNReplayerInit:
    """Tests for ESPNReplayer initialization."""

    def test_init_fast_mode(self, mock_store):
        """Initialize replayer in fast mode."""
        replayer = ESPNReplayer(store=mock_store, mode="fast", signal_delay=0.0)
        assert replayer.mode == "fast"
        assert replayer.signal_delay == 0.0
        assert replayer.store is mock_store

    def test_init_realtime_mode(self, mock_store):
        """Initialize replayer in realtime mode."""
        replayer = ESPNReplayer(store=mock_store, mode="realtime", signal_delay=2.5)
        assert replayer.mode == "realtime"
        assert replayer.signal_delay == 2.5

    def test_init_invalid_mode_raises(self, mock_store):
        """Invalid mode raises ValueError."""
        with pytest.raises(ValueError, match="mode must be 'fast' or 'realtime'"):
            ESPNReplayer(store=mock_store, mode="invalid")

    def test_init_negative_signal_delay_raises(self, mock_store):
        """Negative signal_delay raises ValueError."""
        with pytest.raises(ValueError, match="signal_delay must be >= 0"):
            ESPNReplayer(store=mock_store, mode="fast", signal_delay=-1.0)


class TestApplyDelay:
    """Tests for signal_delay application."""

    def test_apply_delay_zero_returns_original(self, replayer_fast):
        """Zero signal_delay returns original event."""
        event = make_scoring_event(observed_at=1000.0)
        result = replayer_fast._apply_delay(event)
        assert result is event

    def test_apply_delay_positive_shifts_observed_at(self, mock_store):
        """Positive signal_delay shifts observed_at."""
        replayer = ESPNReplayer(store=mock_store, mode="fast", signal_delay=2.5)
        event = make_scoring_event(observed_at=1000.0)
        result = replayer._apply_delay(event)
        assert result.observed_at == 1002.5

    def test_apply_delay_returns_new_instance(self, mock_store):
        """apply_delay returns new event instance (immutability)."""
        replayer = ESPNReplayer(store=mock_store, mode="fast", signal_delay=1.0)
        event = make_scoring_event(observed_at=1000.0)
        result = replayer._apply_delay(event)
        # Original unchanged
        assert event.observed_at == 1000.0
        # New instance
        assert result is not event


class TestStreamEvents:
    """Tests for stream_events async generator."""

    @pytest.mark.asyncio(loop_scope="function")
    async def test_stream_events_yields_in_sequence_order(self, replayer_fast):
        """stream_events yields events in sequence order."""
        events = [
            make_scoring_event(sequence=1, wallclock="2026-03-29T00:00:00Z"),
            make_foul_event(sequence=2, wallclock="2026-03-29T00:01:00Z"),
        ]
        replayer_fast.store.load_events.return_value = events

        collected = []
        async for event in replayer_fast.stream_events("401584793", "2026-03-29"):
            collected.append(event)

        assert len(collected) == 2
        assert collected[0].sequence == 1
        assert collected[1].sequence == 2

    @pytest.mark.asyncio(loop_scope="function")
    async def test_stream_events_fast_mode_no_sleep(self, replayer_fast):
        """Fast mode yields events immediately without sleep."""
        events = [
            make_scoring_event(sequence=1, wallclock="2026-03-29T00:00:00Z"),
            make_foul_event(sequence=2, wallclock="2026-03-29T00:10:00Z"),
        ]
        replayer_fast.store.load_events.return_value = events

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            collected = []
            async for event in replayer_fast.stream_events("401584793", "2026-03-29"):
                collected.append(event)

            # Should never sleep in fast mode
            mock_sleep.assert_not_called()
            assert len(collected) == 2

    @pytest.mark.asyncio(loop_scope="function")
    async def test_stream_events_realtime_mode_sleeps(self, replayer_realtime):
        """Realtime mode sleeps between events based on wallclock delta."""
        events = [
            make_scoring_event(
                sequence=1, wallclock="2026-03-29T00:00:00Z", observed_at=1000.0
            ),
            make_foul_event(
                sequence=2, wallclock="2026-03-29T00:01:00Z", observed_at=1060.0
            ),
        ]
        replayer_realtime.store.load_events.return_value = events

        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            collected = []
            async for event in replayer_realtime.stream_events(
                "401584793", "2026-03-29"
            ):
                collected.append(event)

            # Should sleep 60 seconds between the two events
            mock_sleep.assert_called_once_with(60.0)
            assert len(collected) == 2

    @pytest.mark.asyncio(loop_scope="function")
    async def test_stream_events_applies_signal_delay(self, mock_store):
        """stream_events applies signal_delay to all events."""
        replayer = ESPNReplayer(store=mock_store, mode="fast", signal_delay=2.5)
        events = [
            make_scoring_event(sequence=1, observed_at=1000.0),
            make_foul_event(sequence=2, observed_at=1060.0),
        ]
        replayer.store.load_events.return_value = events

        collected = []
        async for event in replayer.stream_events("401584793", "2026-03-29"):
            collected.append(event)

        assert collected[0].observed_at == 1002.5
        assert collected[1].observed_at == 1062.5

    @pytest.mark.asyncio(loop_scope="function")
    async def test_stream_events_empty_game(self, replayer_fast):
        """stream_events handles empty event list."""
        replayer_fast.store.load_events.return_value = []

        collected = []
        async for event in replayer_fast.stream_events("401584793", "2026-03-29"):
            collected.append(event)

        assert collected == []


class TestReplayGame:
    """Tests for replay_game method."""

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_game_returns_replay_result(self, replayer_fast):
        """replay_game returns ReplayResult with correct structure."""
        events = [
            make_scoring_event(sequence=1),
            make_foul_event(sequence=2),
        ]
        replayer_fast.store.load_events.return_value = events

        result = await replayer_fast.replay_game("401584793", "2026-03-29")

        assert isinstance(result, ReplayResult)
        assert result.game_id == "401584793"
        assert len(result.events) == 2
        assert result.snapshot_count == 2
        assert result.duration_seconds >= 0

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_game_collects_all_events(self, replayer_fast):
        """replay_game collects all events in order."""
        events = [
            make_scoring_event(sequence=1),
            make_foul_event(sequence=2),
        ]
        replayer_fast.store.load_events.return_value = events

        result = await replayer_fast.replay_game("401584793", "2026-03-29")

        assert result.events[0].sequence == 1
        assert result.events[1].sequence == 2

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_game_empty_game(self, replayer_fast):
        """replay_game handles empty event list."""
        replayer_fast.store.load_events.return_value = []

        result = await replayer_fast.replay_game("401584793", "2026-03-29")

        assert result.game_id == "401584793"
        assert result.events == []
        assert result.snapshot_count == 0

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_game_independent_calls(self, replayer_fast):
        """Multiple replay_game calls are independent (no state leakage)."""
        replayer_fast.store.load_events.side_effect = [
            [make_scoring_event(sequence=1)],
            [make_foul_event(sequence=2)],
        ]

        result1 = await replayer_fast.replay_game("401584793", "2026-03-29")
        result2 = await replayer_fast.replay_game("401584794", "2026-03-29")

        assert result1.events[0].sequence == 1
        assert result2.events[0].sequence == 2


class TestReplayDate:
    """Tests for replay_date method."""

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_date_returns_list_of_results(self, replayer_fast):
        """replay_date returns list of ReplayResult, one per game."""
        replayer_fast.store.list_games.return_value = ["401584793", "401584794"]
        replayer_fast.store.load_events.side_effect = [
            [make_scoring_event(sequence=1)],
            [make_foul_event(sequence=2)],
        ]

        results = await replayer_fast.replay_date("2026-03-29")

        assert len(results) == 2
        assert results[0].game_id == "401584793"
        assert results[1].game_id == "401584794"

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_date_empty_date(self, replayer_fast):
        """replay_date handles dates with no games."""
        replayer_fast.store.list_games.return_value = []

        results = await replayer_fast.replay_date("2026-03-29")

        assert results == []

    @pytest.mark.asyncio(loop_scope="function")
    async def test_replay_date_calls_list_games_with_correct_date(self, replayer_fast):
        """replay_date calls store.list_games with correct date."""
        replayer_fast.store.list_games.return_value = []

        await replayer_fast.replay_date("2026-03-29")

        replayer_fast.store.list_games.assert_called_once_with("2026-03-29")
