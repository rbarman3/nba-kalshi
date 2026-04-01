"""Snapshot persistence — JSONL storage of game snapshots."""
import asyncio
import json
from datetime import datetime
from pathlib import Path

from .models import RawSnapshot


class SnapshotStore:
    """Persists RawSnapshot objects to JSONL files for backtesting replay."""

    def __init__(self, base_dir: str | Path = "data/snapshots") -> None:
        """Initialize store with base directory for snapshot files.

        Args:
            base_dir: Root directory for storing snapshots.
                Files are organized as: base_dir/{YYYY-MM-DD}/{game_id}.jsonl
        """
        self.base_dir = Path(base_dir)

    def _path(self, game_id: str, date: str) -> Path:
        """Return full path for a game's snapshot file.

        Args:
            game_id: NBA game ID (e.g. "0022500001")
            date: Date string in YYYY-MM-DD format

        Returns:
            Path object: base_dir/{date}/{game_id}.jsonl
        """
        return self.base_dir / date / f"{game_id}.jsonl"

    async def persist(self, snapshot: RawSnapshot, date: str | None = None) -> None:
        """Append snapshot to JSONL file asynchronously.

        Creates the directory if needed. Each snapshot is written as one JSON line.

        Args:
            snapshot: RawSnapshot to persist
            date: Date in YYYY-MM-DD format. If None, uses today's date.
        """
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        path = self._path(snapshot.game_id, date)

        # Serialize snapshot for storage
        line = json.dumps({
            "game_id": snapshot.game_id,
            "payload": snapshot.payload,
            "fetched_at": snapshot.fetched_at,
        })

        # Run file I/O in thread pool to avoid blocking event loop
        await asyncio.to_thread(self._write_line, path, line)

    def _write_line(self, path: Path, line: str) -> None:
        """Write one line to JSONL file (sync, runs in thread pool)."""
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a") as f:
            f.write(line + "\n")

    def load(self, game_id: str, date: str) -> list[RawSnapshot]:
        """Load all snapshots for a game on a given date.

        Args:
            game_id: NBA game ID
            date: Date in YYYY-MM-DD format

        Returns:
            List of RawSnapshot objects in order they were persisted.
            Empty list if file doesn't exist.
        """
        path = self._path(game_id, date)

        if not path.exists():
            return []

        snapshots = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                data = json.loads(line)
                snap = RawSnapshot(
                    game_id=data["game_id"],
                    payload=data["payload"],
                    fetched_at=data["fetched_at"],
                )
                snapshots.append(snap)

        return snapshots
