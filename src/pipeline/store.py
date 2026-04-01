"""Snapshot persistence — JSONL ingestion + Parquet compaction."""
import asyncio
import json
import threading
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .models import RawSnapshot

_PARQUET_SCHEMA = pa.schema([
    ("game_id", pa.string()),
    ("payload", pa.string()),
    ("fetched_at", pa.float64()),
])


class SnapshotStore:
    """Persists RawSnapshot objects to JSONL files, compacts to Parquet."""

    def __init__(self, base_dir: str | Path = "data/snapshots") -> None:
        self.base_dir = Path(base_dir)
        self._lock = threading.Lock()

    def _path(self, game_id: str, date: str) -> Path:
        """JSONL path: base_dir/{date}/{game_id}.jsonl"""
        return self.base_dir / date / f"{game_id}.jsonl"

    def _parquet_path(self, game_id: str, date: str) -> Path:
        """Parquet path: base_dir/{date}/{game_id}.parquet"""
        return self.base_dir / date / f"{game_id}.parquet"

    # ------------------------------------------------------------------
    # Live ingestion (unchanged)
    # ------------------------------------------------------------------

    async def persist(self, snapshot: RawSnapshot, date: str | None = None) -> None:
        """Append snapshot to JSONL file asynchronously."""
        if date is None:
            date = datetime.now().strftime("%Y-%m-%d")

        path = self._path(snapshot.game_id, date)
        line = json.dumps({
            "game_id": snapshot.game_id,
            "payload": snapshot.payload,
            "fetched_at": snapshot.fetched_at,
        })
        await asyncio.to_thread(self._write_line, path, line)

    def _write_line(self, path: Path, line: str) -> None:
        """Write one line to JSONL file (sync, runs in thread pool)."""
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a") as f:
                f.write(line + "\n")

    # ------------------------------------------------------------------
    # Compaction — JSONL → Parquet
    # ------------------------------------------------------------------

    async def compact(self, game_id: str, date: str) -> Path:
        """Convert a game's JSONL file to Parquet with zstd compression.

        Reads all snapshots from JSONL, writes a single Parquet file,
        then removes the source JSONL.

        Raises:
            FileNotFoundError: No JSONL file for this game/date.
            ValueError: JSONL file exists but contains no snapshots.
        """
        return await asyncio.to_thread(self._do_compact, game_id, date)

    def _do_compact(self, game_id: str, date: str) -> Path:
        with self._lock:
            jsonl_path = self._path(game_id, date)
            if not jsonl_path.exists():
                raise FileNotFoundError(f"No JSONL file: {jsonl_path}")

            snapshots = self._load_jsonl(jsonl_path)
            if not snapshots:
                raise ValueError(f"JSONL file is empty: {jsonl_path}")

            table = pa.table(
                {
                    "game_id": [s.game_id for s in snapshots],
                    "payload": [json.dumps(s.payload) for s in snapshots],
                    "fetched_at": [s.fetched_at for s in snapshots],
                },
                schema=_PARQUET_SCHEMA,
            )

            parquet_path = self._parquet_path(game_id, date)
            tmp_path = parquet_path.with_suffix(".parquet.tmp")
            pq.write_table(table, tmp_path, compression="zstd")
            tmp_path.rename(parquet_path)
            jsonl_path.unlink()
            return parquet_path

    # ------------------------------------------------------------------
    # Loading — Parquet preferred, JSONL fallback
    # ------------------------------------------------------------------

    def load(self, game_id: str, date: str) -> list[RawSnapshot]:
        """Load all snapshots for a game on a given date.

        Prefers Parquet if available, falls back to JSONL.
        Returns empty list if neither file exists.
        """
        parquet_path = self._parquet_path(game_id, date)
        if parquet_path.exists():
            return self._load_parquet(parquet_path)

        jsonl_path = self._path(game_id, date)
        if not jsonl_path.exists():
            return []
        return self._load_jsonl(jsonl_path)

    def _load_parquet(self, path: Path) -> list[RawSnapshot]:
        table = pq.read_table(path)
        d = table.to_pydict()
        return [
            RawSnapshot(
                game_id=gid,
                payload=json.loads(p),
                fetched_at=ts,
            )
            for gid, p, ts in zip(d["game_id"], d["payload"], d["fetched_at"])
        ]

    def _load_jsonl(self, path: Path) -> list[RawSnapshot]:
        snapshots = []
        with open(path, "r") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                data = json.loads(line)
                snapshots.append(RawSnapshot(
                    game_id=data["game_id"],
                    payload=data["payload"],
                    fetched_at=data["fetched_at"],
                ))
        return snapshots
