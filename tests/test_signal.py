"""Tests for FeedQualitySignal — split transport health + data freshness."""
import time
from dataclasses import FrozenInstanceError

import pytest

from pipeline.models import PollResult, WindowStats
from pipeline.signal import (
    CONSEC_FAILURE_CEILING,
    DEFAULT_WINDOW_SECONDS,
    HEALTH_THRESHOLD,
    LATENCY_CEILING_MS,
    STALENESS_CEILING_S,
    TRADE_FRESHNESS_THRESHOLD,
    TRADE_HEALTH_THRESHOLD,
    FeedQualitySignal,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _poll(
    game_id: str = "0022500001",
    status_code: int = 200,
    error_type: str | None = None,
    response_time_ms: float = 30.0,
    timestamp: float | None = None,
) -> PollResult:
    return PollResult(
        game_id=game_id,
        status_code=status_code,
        error_type=error_type,
        response_time_ms=response_time_ms,
        timestamp=timestamp or time.time(),
    )


# ---------------------------------------------------------------------------
# TestWindowStats
# ---------------------------------------------------------------------------

class TestWindowStats:
    def test_frozen(self):
        ws = WindowStats(success_rate=1.0, latency_p50_ms=30.0,
                         latency_p95_ms=100.0, poll_count=10, window_seconds=60.0)
        with pytest.raises(FrozenInstanceError):
            ws.success_rate = 0.5  # type: ignore[misc]


# ---------------------------------------------------------------------------
# TestTransportHealth
# ---------------------------------------------------------------------------

class TestTransportHealth:
    def test_unknown_game_returns_zero(self):
        signal = FeedQualitySignal()
        assert signal.transport_health("unknown") == 0.0

    def test_perfect_transport_near_one(self):
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))

        health = signal.transport_health("0022500001")
        assert health >= 0.9

    def test_consecutive_failures_decay_health(self):
        signal = FeedQualitySignal()
        now = time.time()
        # 4 consecutive failures
        for i in range(4):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))

        health = signal.transport_health("0022500001")
        assert health < 0.5

    def test_high_latency_reduces_health(self):
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=1500.0))

        health = signal.transport_health("0022500001")
        assert health < 0.85

    def test_staleness_does_not_affect_health(self):
        """Key test: stale data should NOT reduce transport health."""
        signal = FeedQualitySignal()
        now = time.time()
        # Perfect polls
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))
        # Snapshot is 30s old — very stale
        signal.on_snapshot("0022500001", now - 30.0)

        health = signal.transport_health("0022500001")
        assert health >= 0.9

    def test_recovery_resets_consecutive_failures(self):
        """After failures, single 200 resets consec counter → health recovers."""
        signal = FeedQualitySignal()
        now = time.time()
        # 3 failures
        for i in range(3):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))
        # Recovery with new success
        signal.on_poll(_poll(status_code=200, timestamp=now + 3))

        state = signal._states["0022500001"]
        assert state.consecutive_failures == 0


# ---------------------------------------------------------------------------
# TestFreshness
# ---------------------------------------------------------------------------

class TestFreshness:
    def test_unknown_game_returns_zero(self):
        signal = FeedQualitySignal()
        assert signal.freshness("unknown") == 0.0

    def test_fresh_snapshot_near_one(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now)

        freshness = signal.freshness("0022500001")
        assert freshness >= 0.9

    def test_stale_snapshot_decays(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now - 8.0)

        freshness = signal.freshness("0022500001")
        # 1 - 8/10 = 0.2
        assert freshness < 0.3

    def test_beyond_ceiling_returns_zero(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now - 20.0)

        freshness = signal.freshness("0022500001")
        assert freshness == 0.0

    def test_transport_failures_do_not_affect_freshness(self):
        """Key test: bad transport should NOT reduce freshness if snapshot is recent."""
        signal = FeedQualitySignal()
        now = time.time()
        # Fresh snapshot
        signal.on_snapshot("0022500001", now)
        # All failures after snapshot
        for i in range(5):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))

        freshness = signal.freshness("0022500001")
        assert freshness >= 0.9

    def test_no_snapshot_returns_zero(self):
        """Game with polls but no snapshot → freshness 0."""
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_poll(_poll(timestamp=now))

        freshness = signal.freshness("0022500001")
        assert freshness == 0.0


# ---------------------------------------------------------------------------
# TestOnPoll
# ---------------------------------------------------------------------------

class TestOnPoll:
    def test_appends_to_window(self):
        signal = FeedQualitySignal()
        signal.on_poll(_poll())
        signal.on_poll(_poll())
        state = signal._states["0022500001"]
        assert len(state.polls) == 2

    def test_prunes_old_entries(self):
        signal = FeedQualitySignal(window_seconds=10.0)
        now = time.time()
        # Old poll outside window
        signal.on_poll(_poll(timestamp=now - 20.0))
        # Recent poll inside window
        signal.on_poll(_poll(timestamp=now))

        state = signal._states["0022500001"]
        assert len(state.polls) == 1
        assert state.polls[0][0] == pytest.approx(now, abs=0.01)

    def test_tracks_consecutive_failures(self):
        signal = FeedQualitySignal()
        signal.on_poll(_poll(status_code=500))
        signal.on_poll(_poll(status_code=403))
        assert signal._states["0022500001"].consecutive_failures == 2

        signal.on_poll(_poll(status_code=200))
        assert signal._states["0022500001"].consecutive_failures == 0


# ---------------------------------------------------------------------------
# TestOnSnapshot
# ---------------------------------------------------------------------------

class TestOnSnapshot:
    def test_sets_last_snapshot_at(self):
        signal = FeedQualitySignal()
        signal.on_snapshot("0022500001", 1000.0)
        assert signal._states["0022500001"].last_snapshot_at == 1000.0

    def test_clears_gap(self):
        signal = FeedQualitySignal()
        signal.on_snapshot("0022500001", 1000.0)
        # Start a gap
        signal.on_poll(_poll(status_code=500, timestamp=1001.0))
        assert signal._states["0022500001"].gap_start > 0

        # Snapshot arrives, clears gap
        signal.on_snapshot("0022500001", 1005.0)
        assert signal._states["0022500001"].gap_start == 0.0

    def test_updates_longest_gap(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now)

        # Simulate gap: failure sets gap_start
        signal.on_poll(_poll(status_code=500, timestamp=now + 1))
        # Gap of ~10 seconds
        signal._states["0022500001"].gap_start = now + 1

        signal.on_snapshot("0022500001", now + 11)
        assert signal._states["0022500001"].longest_gap_s >= 9.0


# ---------------------------------------------------------------------------
# TestGapDetection
# ---------------------------------------------------------------------------

class TestGapDetection:
    def test_gap_seconds_reflects_staleness(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now - 5.0)

        gap = signal.gap_seconds("0022500001")
        assert gap >= 4.9

    def test_gap_seconds_zero_for_unknown(self):
        signal = FeedQualitySignal()
        assert signal.gap_seconds("unknown") == 0.0

    def test_gap_start_set_on_failure_after_snapshot(self):
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now)

        # First failure starts gap
        signal.on_poll(_poll(status_code=500, timestamp=now + 1))
        assert signal._states["0022500001"].gap_start > 0

    def test_gap_start_not_set_before_first_snapshot(self):
        signal = FeedQualitySignal()
        # Failure before any snapshot — no gap (we never had data)
        signal.on_poll(_poll(status_code=500))
        assert signal._states["0022500001"].gap_start == 0.0


# ---------------------------------------------------------------------------
# TestWindowStatsQuery
# ---------------------------------------------------------------------------

class TestWindowStatsQuery:
    def test_empty_window_returns_zeros(self):
        signal = FeedQualitySignal()
        ws = signal.window_stats("unknown")
        assert ws.success_rate == 0.0
        assert ws.poll_count == 0
        assert ws.latency_p50_ms == 0.0

    def test_correct_success_rate(self):
        signal = FeedQualitySignal()
        now = time.time()
        for _ in range(8):
            signal.on_poll(_poll(status_code=200, timestamp=now))
        for _ in range(2):
            signal.on_poll(_poll(status_code=500, timestamp=now))

        ws = signal.window_stats("0022500001")
        assert ws.success_rate == pytest.approx(0.8)
        assert ws.poll_count == 10

    def test_correct_percentiles(self):
        signal = FeedQualitySignal()
        now = time.time()
        # 100 polls with latencies 1.0 to 100.0
        for i in range(1, 101):
            signal.on_poll(_poll(response_time_ms=float(i), timestamp=now))

        ws = signal.window_stats("0022500001")
        assert ws.latency_p50_ms == pytest.approx(50.0, abs=2.0)
        assert ws.latency_p95_ms == pytest.approx(95.0, abs=2.0)


# ---------------------------------------------------------------------------
# TestIsReliable
# ---------------------------------------------------------------------------

class TestIsReliable:
    def test_reliable_when_transport_healthy(self):
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))

        assert signal.is_reliable("0022500001") is True

    def test_reliable_even_when_stale(self):
        """Stale data + healthy transport → still reliable (transport-only check)."""
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))
        signal.on_snapshot("0022500001", now - 25.0)  # very stale

        assert signal.is_reliable("0022500001") is True

    def test_not_reliable_when_transport_degraded(self):
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(5):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))

        assert signal.is_reliable("0022500001") is False

    def test_unknown_game_not_reliable(self):
        signal = FeedQualitySignal()
        assert signal.is_reliable("unknown") is False


# ---------------------------------------------------------------------------
# TestIsTradeable
# ---------------------------------------------------------------------------

class TestIsTradeable:
    def test_unknown_game_not_tradeable(self):
        signal = FeedQualitySignal()
        assert signal.is_tradeable("unknown") is False

    def test_tradeable_when_healthy_and_fresh(self):
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))
        signal.on_snapshot("0022500001", now)
        assert signal.is_tradeable("0022500001") is True

    def test_not_tradeable_when_stale(self):
        """Healthy transport + stale data → not tradeable (default behavior)."""
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))
        signal.on_snapshot("0022500001", now - 8.0)
        assert signal.is_tradeable("0022500001") is False

    def test_not_tradeable_when_transport_degraded(self):
        """Fresh data + bad transport → not tradeable."""
        signal = FeedQualitySignal()
        now = time.time()
        signal.on_snapshot("0022500001", now)
        for i in range(5):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))
        assert signal.is_tradeable("0022500001") is False

    def test_tradeable_with_freshness_override(self):
        """Q4→OT case: healthy transport + stale data + freshness_floor=0.0 → tradeable."""
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(20):
            signal.on_poll(_poll(timestamp=now - 20 + i, response_time_ms=20.0))
        signal.on_snapshot("0022500001", now - 30.0)  # very stale

        # Without override: not tradeable
        assert signal.is_tradeable("0022500001") is False
        # With override: tradeable (strategy says "I know data is stale, feed is healthy")
        assert signal.is_tradeable("0022500001", freshness_floor=0.0) is True

    def test_freshness_override_still_requires_healthy_transport(self):
        """Override can't bypass bad transport."""
        signal = FeedQualitySignal()
        now = time.time()
        for i in range(5):
            signal.on_poll(_poll(status_code=500, timestamp=now + i))
        signal.on_snapshot("0022500001", now - 30.0)

        assert signal.is_tradeable("0022500001", freshness_floor=0.0) is False

    def test_trade_threshold_stricter_than_reliability(self):
        assert TRADE_HEALTH_THRESHOLD > HEALTH_THRESHOLD
