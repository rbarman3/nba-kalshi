"""ESPN event replayer for backtesting.

Replays ESPN play-by-play events with optional realtime pacing and signal delay.
Used for backtesting strategies on historical ESPN data.
"""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from typing import AsyncGenerator, Any

from .espn_store import ESPNStore
from .models import ReplayResult


def _parse_wallclock(wallclock: str) -> float:
    """Parse ISO 8601 wallclock string to Unix timestamp.

    Args:
        wallclock: ISO 8601 string, e.g. "2026-03-29T00:01:00Z"

    Returns:
        Unix timestamp as float
    """
    return datetime.fromisoformat(wallclock.replace("Z", "+00:00")).timestamp()


class ESPNReplayer:
    """Replays ESPN play-by-play events with optional realtime pacing.

    Attributes:
        store: ESPNStore instance for loading events
        mode: "fast" (no delays) or "realtime" (paced by wallclock)
        signal_delay: Seconds to add to observed_at on all events (for latency simulation)
    """

    def __init__(
        self,
        store: ESPNStore,
        mode: str = "fast",
        signal_delay: float = 0.0,
    ):
        """Initialize replayer.

        Args:
            store: ESPNStore instance
            mode: "fast" or "realtime"
            signal_delay: Seconds to add to observed_at on all events
        """
        if mode not in ("fast", "realtime"):
            raise ValueError(f"mode must be 'fast' or 'realtime', got '{mode}'")
        if signal_delay < 0:
            raise ValueError(f"signal_delay must be >= 0, got {signal_delay}")

        self.store = store
        self.mode = mode
        self.signal_delay = signal_delay

    def _apply_delay(self, event: Any) -> Any:
        """Apply signal_delay to event's observed_at timestamp.

        Args:
            event: Event instance with observed_at attribute

        Returns:
            New event instance with shifted observed_at (or original if delay is 0)
        """
        if self.signal_delay == 0:
            return event
        return replace(event, observed_at=event.observed_at + self.signal_delay)

    async def stream_events(
        self, game_id: str, date: str
    ) -> AsyncGenerator[Any, None]:
        """Stream ESPN events for a game in sequence order.

        Events are yielded one at a time. In realtime mode, sleeps between
        consecutive events based on wallclock delta.

        Args:
            game_id: ESPN game ID
            date: Date string in YYYY-MM-DD format

        Yields:
            Event instances in sequence order (with signal_delay applied)
        """
        events = self.store.load_events(game_id, date)

        for i, event in enumerate(events):
            # In realtime mode, sleep until this event's wallclock time
            if self.mode == "realtime" and i > 0:
                prev_wallclock = _parse_wallclock(events[i - 1].wallclock)
                curr_wallclock = _parse_wallclock(event.wallclock)
                delay_seconds = max(0.0, curr_wallclock - prev_wallclock)
                if delay_seconds > 0:
                    await asyncio.sleep(delay_seconds)

            # Apply signal delay and yield
            yield self._apply_delay(event)

    async def replay_game(self, game_id: str, date: str) -> ReplayResult:
        """Replay all events for a single game, collecting them into a result.

        Args:
            game_id: ESPN game ID
            date: Date string in YYYY-MM-DD format

        Returns:
            ReplayResult with all events collected and timing info
        """
        events_list = []
        start_time = datetime.now(timezone.utc).timestamp()

        async for event in self.stream_events(game_id, date):
            events_list.append(event)

        end_time = datetime.now(timezone.utc).timestamp()
        duration_seconds = end_time - start_time

        return ReplayResult(
            game_id=game_id,
            events=events_list,
            snapshot_count=len(events_list),
            duration_seconds=duration_seconds,
        )

    async def replay_date(self, date: str) -> list[ReplayResult]:
        """Replay all games stored for a given date.

        Args:
            date: Date string in YYYY-MM-DD format

        Returns:
            List of ReplayResult, one per game stored for that date
        """
        game_ids = self.store.list_games(date)
        results = []

        for game_id in game_ids:
            result = await self.replay_game(game_id, date)
            results.append(result)

        return results
