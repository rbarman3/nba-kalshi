"""Snapshot replay engine for backtesting.

Loads stored RawSnapshots from SnapshotStore and feeds them through
NBAProcessor to collect emitted events. Supports fast mode (no delays)
and realtime mode (paced by original fetched_at timestamps).
"""
import asyncio
import time
from dataclasses import dataclass, field
from typing import Optional

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
    """Replay stored snapshots through NBAProcessor, collect events."""

    def __init__(
        self,
        store: Optional[SnapshotStore] = None,
        mode: str = "fast",
    ) -> None:
        self.store = store
        self.mode = mode

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
        in_q: asyncio.Queue = asyncio.Queue()
        out_q: asyncio.Queue = asyncio.Queue()
        processor = NBAProcessor(in_q, out_q)
        events: list = []

        for i, snapshot in enumerate(snapshots):
            if self.mode == "realtime" and i > 0:
                delay = snapshot.fetched_at - snapshots[i - 1].fetched_at
                if delay > 0:
                    await asyncio.sleep(delay)

            await in_q.put(snapshot)
            await processor.run_once()

            while not out_q.empty():
                events.append(out_q.get_nowait())

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
