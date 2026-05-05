"""Real-time feed quality signal for the strategy layer.

Purely reactive, in-memory service — no async loop, no queue, no disk I/O.
Transport calls ``on_poll()`` and ``on_snapshot()`` synchronously;
strategy queries ``transport_health()``, ``freshness()``, ``is_reliable()``,
``is_tradeable()``, ``gap_seconds()``, and ``window_stats()`` synchronously.

Two independent dimensions:

    Transport health (0.0–1.0):
        health = W_SUCCESS * success_rate + W_LATENCY * latency_score + W_CONSEC * consec_score

    Data freshness (0.0–1.0):
        freshness = max(0, 1 - staleness_seconds / STALENESS_CEILING_S)

Trading gate:
    is_tradeable() = transport_health >= 0.80 AND freshness >= 0.40
    Strategy can override freshness_floor for known quiet periods (e.g., Q4→OT).

Thresholds:
    is_reliable()  — transport_health >= 0.60 (monitoring / alerting)
    is_tradeable() — transport_health >= 0.80 AND freshness >= 0.40 (trading decisions)
"""
import time
from dataclasses import dataclass, field

from .models import PollResult, WindowStats

# ---------------------------------------------------------------------------
# Thresholds — aligned with api_stats.py SLO constants
# ---------------------------------------------------------------------------
DEFAULT_WINDOW_SECONDS = 60.0
HEALTH_THRESHOLD = 0.60            # is_reliable() gate — transport health only
TRADE_HEALTH_THRESHOLD = 0.80      # transport must be this healthy to trade
TRADE_FRESHNESS_THRESHOLD = 0.40   # freshness must be this high (~6s stale w/ 10s ceiling)

# Transport health weights (sum to 1.0, normalized from original 0.30/0.20/0.15)
W_TRANSPORT_SUCCESS = 0.46
W_TRANSPORT_LATENCY = 0.31
W_TRANSPORT_CONSEC = 0.23

# Ceilings — component reaches 0.0 at these values
STALENESS_CEILING_S = 10.0       # freshness → 0 at 10s stale
LATENCY_CEILING_MS = 2000.0      # SLO_LATENCY_P99_MS
CONSEC_FAILURE_CEILING = 5       # SLO_MAX_CONSECUTIVE_FAILURES

# Rolling-window caps for latency histograms (per game)
MAX_LATENCY_SAMPLES = 500

# SLO targets for end-to-end latency (informational; not enforced as gates yet)
SLO_API_FRESHNESS_P95_MS = 1500.0
SLO_GENERATOR_LAG_P95_MS = 50.0
SLO_MARKET_REACTION_P95_MS = 5000.0


def _push_capped(buf: list[float], value: float) -> None:
    """Append to a rolling list, drop oldest if over MAX_LATENCY_SAMPLES."""
    buf.append(value)
    if len(buf) > MAX_LATENCY_SAMPLES:
        del buf[: len(buf) - MAX_LATENCY_SAMPLES]


def _compute_percentile(sorted_values: list[float], pct: float) -> float:
    """Index-based percentile on a pre-sorted list. Returns 0.0 if empty."""
    if not sorted_values:
        return 0.0
    idx = int(len(sorted_values) * pct / 100)
    idx = min(idx, len(sorted_values) - 1)
    return sorted_values[idx]


@dataclass
class _GameState:
    """Mutable per-game state. Internal to FeedQualitySignal."""
    last_snapshot_at: float = 0.0
    gap_start: float = 0.0
    longest_gap_s: float = 0.0
    consecutive_failures: int = 0
    polls: list[tuple[float, int, float]] = field(default_factory=list)
    # Each entry: (timestamp, status_code, response_time_ms)
    api_freshness_ms: list[float] = field(default_factory=list)
    generator_lag_ms: list[float] = field(default_factory=list)
    market_reaction_ms: list[float] = field(default_factory=list)


class FeedQualitySignal:
    """Per-game transport health + data freshness scoring for the strategy layer."""

    def __init__(self, window_seconds: float = DEFAULT_WINDOW_SECONDS) -> None:
        self._window_seconds = window_seconds
        self._states: dict[str, _GameState] = {}

    # ------------------------------------------------------------------
    # Recording methods (called by transport)
    # ------------------------------------------------------------------

    def on_poll(self, result: PollResult) -> None:
        """Record a poll result. Synchronous — called inline by transport."""
        state = self._ensure_state(result.game_id)
        now = result.timestamp

        # Prune old entries
        cutoff = now - self._window_seconds
        polls = state.polls
        while polls and polls[0][0] < cutoff:
            polls.pop(0)

        # Append new entry
        polls.append((now, result.status_code, result.response_time_ms))

        # Track consecutive failures
        if result.status_code == 200:
            state.consecutive_failures = 0
        else:
            state.consecutive_failures += 1

        # Gap detection: start gap on non-200 if we previously had data
        if result.status_code != 200 and state.gap_start == 0.0 and state.last_snapshot_at > 0:
            state.gap_start = now

    def on_snapshot(self, game_id: str, fetched_at: float) -> None:
        """Record a new (non-duplicate) snapshot arrival."""
        state = self._ensure_state(game_id)

        # Close gap if one was open
        if state.gap_start > 0.0:
            gap_duration = fetched_at - state.gap_start
            state.longest_gap_s = max(state.longest_gap_s, gap_duration)
            state.gap_start = 0.0

        state.last_snapshot_at = fetched_at

    def on_event(self, event) -> None:
        """Record latency samples derived from an emitted pipeline event.

        Pulls cdn_observed_at, observed_at (= snapshot.fetched_at), and emitted_at
        off the event. Silently skips events without these fields.
        """
        game_id = getattr(event, "game_id", None)
        if not game_id:
            return
        observed_at = getattr(event, "observed_at", 0.0)
        emitted_at = getattr(event, "emitted_at", 0.0)
        cdn_observed_at = getattr(event, "cdn_observed_at", None)

        state = self._ensure_state(game_id)

        if emitted_at > 0 and observed_at > 0:
            lag_ms = max(0.0, (emitted_at - observed_at) * 1000.0)
            _push_capped(state.generator_lag_ms, lag_ms)

        if cdn_observed_at and observed_at > 0:
            freshness_ms = max(0.0, (observed_at - cdn_observed_at) * 1000.0)
            _push_capped(state.api_freshness_ms, freshness_ms)

    def on_market_move(self, game_id: str, delta_ms: float) -> None:
        """Record observed delta between an NBA event emission and Kalshi market reaction."""
        if delta_ms < 0:
            return
        state = self._ensure_state(game_id)
        _push_capped(state.market_reaction_ms, float(delta_ms))

    # ------------------------------------------------------------------
    # Score components (private)
    # ------------------------------------------------------------------

    def _success_rate_score(self, state: _GameState) -> float:
        """Success rate component, 0.0–1.0."""
        polls = state.polls
        if not polls:
            return 0.0
        successes = sum(1 for _, sc, _ in polls if sc == 200)
        return successes / len(polls)

    def _latency_score(self, state: _GameState) -> float:
        """Latency component (p95-based), 0.0–1.0."""
        latencies = sorted(rt for _, sc, rt in state.polls if sc == 200)
        if not latencies:
            return 0.0
        p95 = _compute_percentile(latencies, 95)
        return max(0.0, 1.0 - p95 / LATENCY_CEILING_MS)

    def _consec_failure_score(self, state: _GameState) -> float:
        """Consecutive failure component, 0.0–1.0."""
        return max(0.0, 1.0 - state.consecutive_failures / CONSEC_FAILURE_CEILING)

    def _staleness_score(self, state: _GameState, now: float) -> float:
        """Staleness component, 0.0–1.0. Returns 0.0 if no snapshot received."""
        if state.last_snapshot_at <= 0:
            return 0.0
        age_s = now - state.last_snapshot_at
        return max(0.0, 1.0 - age_s / STALENESS_CEILING_S)

    # ------------------------------------------------------------------
    # Query methods (called by strategy)
    # ------------------------------------------------------------------

    def transport_health(self, game_id: str) -> float:
        """Per-game transport health score, 0.0–1.0. No staleness penalty.

        Returns 0.0 for unknown games.
        """
        if game_id not in self._states:
            return 0.0

        state = self._states[game_id]
        return (
            W_TRANSPORT_SUCCESS * self._success_rate_score(state)
            + W_TRANSPORT_LATENCY * self._latency_score(state)
            + W_TRANSPORT_CONSEC * self._consec_failure_score(state)
        )

    def freshness(self, game_id: str) -> float:
        """Per-game data freshness score, 0.0–1.0. Independent of transport health.

        Returns 0.0 for unknown games or games with no snapshot.
        """
        if game_id not in self._states:
            return 0.0

        return self._staleness_score(self._states[game_id], time.time())

    def is_reliable(self, game_id: str) -> bool:
        """True if transport_health >= HEALTH_THRESHOLD. For monitoring/alerting.

        Does NOT consider data staleness — a healthy feed with stale data
        (e.g., halftime) is still reliable.
        """
        return self.transport_health(game_id) >= HEALTH_THRESHOLD

    def is_tradeable(
        self,
        game_id: str,
        *,
        freshness_floor: float | None = None,
    ) -> bool:
        """True if transport is healthy AND data is fresh enough.

        Args:
            game_id: Game to check.
            freshness_floor: Override freshness threshold. Pass 0.0 to skip
                freshness check entirely (e.g., during Q4→OT transition when
                strategy layer knows data staleness is expected).
                None uses TRADE_FRESHNESS_THRESHOLD (default).
        """
        if self.transport_health(game_id) < TRADE_HEALTH_THRESHOLD:
            return False

        threshold = freshness_floor if freshness_floor is not None else TRADE_FRESHNESS_THRESHOLD
        return self.freshness(game_id) >= threshold

    def gap_seconds(self, game_id: str) -> float:
        """Seconds since last new snapshot, or 0.0 if no snapshot received yet."""
        if game_id not in self._states:
            return 0.0
        last = self._states[game_id].last_snapshot_at
        if last <= 0:
            return 0.0
        return time.time() - last

    def window_stats(self, game_id: str) -> WindowStats:
        """Rolling window stats. Returns zeroed WindowStats for unknown games."""
        if game_id not in self._states:
            return WindowStats(
                success_rate=0.0,
                latency_p50_ms=0.0,
                latency_p95_ms=0.0,
                poll_count=0,
                window_seconds=self._window_seconds,
            )

        state = self._states[game_id]
        polls = state.polls
        total = len(polls)
        successes = sum(1 for _, sc, _ in polls if sc == 200)
        latencies = sorted(rt for _, sc, rt in polls if sc == 200)

        api_fresh_sorted = sorted(state.api_freshness_ms)
        gen_lag_sorted = sorted(state.generator_lag_ms)
        market_sorted = sorted(state.market_reaction_ms)

        event_count = max(len(state.api_freshness_ms), len(state.generator_lag_ms))

        return WindowStats(
            success_rate=successes / total if total > 0 else 0.0,
            latency_p50_ms=round(_compute_percentile(latencies, 50), 2),
            latency_p95_ms=round(_compute_percentile(latencies, 95), 2),
            poll_count=total,
            window_seconds=self._window_seconds,
            api_freshness_p50_ms=round(_compute_percentile(api_fresh_sorted, 50), 2),
            api_freshness_p95_ms=round(_compute_percentile(api_fresh_sorted, 95), 2),
            generator_lag_p50_ms=round(_compute_percentile(gen_lag_sorted, 50), 2),
            generator_lag_p95_ms=round(_compute_percentile(gen_lag_sorted, 95), 2),
            market_reaction_p50_ms=round(_compute_percentile(market_sorted, 50), 2),
            market_reaction_p95_ms=round(_compute_percentile(market_sorted, 95), 2),
            event_sample_count=event_count,
            market_sample_count=len(state.market_reaction_ms),
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _ensure_state(self, game_id: str) -> _GameState:
        if game_id not in self._states:
            self._states[game_id] = _GameState()
        return self._states[game_id]
