"""Snapshot replay engine for backtesting.

Loads stored RawSnapshots from SnapshotStore and feeds them through
NBAProcessor to collect emitted events. Supports fast mode (no delays)
and realtime mode (paced by original fetched_at timestamps).

Anti-bias features:
  - stream_snapshots() / stream_game(): async generators that yield events
    one at a time, preventing look-ahead bias in strategy code.
  - signal_delay: offsets observed_at on emitted events to simulate realistic
    detection-to-execution latency.
"""
import asyncio
import copy
import time
from dataclasses import dataclass, field, replace
from typing import AsyncIterator, Optional

from .models import RawSnapshot
from .processor import NBAProcessor
from .store import SnapshotStore


@dataclass(frozen=True)
class ReplayResult:
    """Result of replaying snapshots for one game."""
    game_id: str
    events: list = field(default_factory=list)
    snapshot_count: int = 0
    duration_seconds: float = 0.0


class SnapshotReplayer:
    """Replay stored snapshots through NBAProcessor, collect events.

    Args:
        store: SnapshotStore for loading persisted snapshots.
        mode: "fast" (no delays) or "realtime" (paced by fetched_at).
        signal_delay: Seconds to add to each event's observed_at, simulating
            the latency between a real-world event and when the pipeline
            would detect and act on it. Default 0 (no offset).
    """

    def __init__(
        self,
        store: Optional[SnapshotStore] = None,
        mode: str = "fast",
        signal_delay: float = 0.0,
    ) -> None:
        self.store = store
        self.mode = mode
        self.signal_delay = signal_delay

    def _apply_delay(self, event: object) -> object:
        """Return a new event with observed_at offset by signal_delay."""
        if self.signal_delay == 0.0:
            return event
        return replace(event, observed_at=event.observed_at + self.signal_delay)

    async def stream_snapshots(
        self, snapshots: list[RawSnapshot]
    ) -> AsyncIterator:
        """Async generator that yields events one at a time.

        Prevents look-ahead bias — the consumer only sees the current event,
        never future events. Strategy code should consume this instead of
        replay_snapshots() when making trading decisions.

        Args:
            snapshots: Ordered list of RawSnapshots to replay.

        Yields:
            Individual events (LineupChangeEvent, ScoreChangeEvent, etc.)
            as they are produced by the processor.
        """
        if not snapshots:
            return

        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        processor = NBAProcessor(in_q, out_q)

        for i, snapshot in enumerate(snapshots):
            if self.mode == "realtime" and i > 0:
                delay = snapshot.fetched_at - snapshots[i - 1].fetched_at
                if delay > 0:
                    await asyncio.sleep(delay)

            await in_q.put(snapshot)
            await processor.run_once()

            while not out_q.empty():
                yield self._apply_delay(out_q.get_nowait())

    async def stream_game(
        self, game_id: str, date: str
    ) -> AsyncIterator:
        """Load snapshots for a game and stream events one at a time.

        Args:
            game_id: NBA game ID.
            date: Date string (YYYY-MM-DD).

        Yields:
            Individual events as they are produced.
        """
        snapshots = self.store.load(game_id, date)
        async for event in self.stream_snapshots(snapshots):
            yield event

    async def replay_snapshots(self, snapshots: list[RawSnapshot]) -> ReplayResult:
        """Feed snapshots through a fresh NBAProcessor and collect events.

        Args:
            snapshots: Ordered list of RawSnapshots to replay.

        Returns:
            ReplayResult with all emitted events and metadata.
        """
        if not snapshots:
            return ReplayResult(game_id="", events=[], snapshot_count=0, duration_seconds=0.0)

        start = time.monotonic()
        events: list = []

        async for event in self.stream_snapshots(snapshots):
            events.append(event)

        elapsed = time.monotonic() - start

        return ReplayResult(
            game_id=snapshots[0].game_id,
            events=events,
            snapshot_count=len(snapshots),
            duration_seconds=elapsed,
        )

    async def replay_game(self, game_id: str, date: str) -> ReplayResult:
        """Load snapshots for a game from the store and replay them.

        Args:
            game_id: NBA game ID.
            date: Date string (YYYY-MM-DD).

        Returns:
            ReplayResult for this game.
        """
        snapshots = self.store.load(game_id, date)
        if not snapshots:
            return ReplayResult(game_id=game_id, events=[], snapshot_count=0, duration_seconds=0.0)
        return await self.replay_snapshots(snapshots)

    async def replay_date(self, date: str) -> list[ReplayResult]:
        """Replay all games for a given date.

        Args:
            date: Date string (YYYY-MM-DD).

        Returns:
            List of ReplayResult, one per game.
        """
        game_ids = self.store.list_games(date)
        results = []
        for game_id in game_ids:
            result = await self.replay_game(game_id, date)
            results.append(result)
        return results
