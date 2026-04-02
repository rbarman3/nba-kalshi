"""Snapshot persistence — JSONL ingestion + Parquet compaction.

Directory layout:
    base_dir/{season}/{game_type}/{date}/{game_id}.jsonl   (live)
    base_dir/{season}/{game_type}/{date}/{game_id}.parquet (compacted)

Season and game type are parsed from the NBA game ID:
    Positions [0:3]  → game type code (002=regular, 004=playoffs, etc.)
    Positions [3:5]  → season start year (25 → 2025-26)
"""
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

_GAME_TYPE_CODES = {
    "001": "preseason",
    "002": "regular",
    "003": "allstar",
    "004": "playoffs",
    "005": "playin",
}


def parse_game_id(game_id: str) -> tuple[str, str]:
    """Extract season and game type from an NBA game ID.

    Args:
        game_id: 10-character NBA game ID (e.g. "0022500001").

    Returns:
        (season, game_type) tuple, e.g. ("2025-26", "regular").

    Raises:
        ValueError: If game_id is too short or has unknown type code.
    """
    if len(game_id) < 5:
        raise ValueError(f"Game ID too short: {game_id!r}")

    type_code = game_id[:3]
    game_type = _GAME_TYPE_CODES.get(type_code)
    if game_type is None:
        raise ValueError(f"Unknown game type code {type_code!r} in {game_id!r}")

    start_year = int(game_id[3:5])
    season = f"20{start_year:02d}-{start_year + 1:02d}"

    return season, game_type


class SnapshotStore:
    """Persists RawSnapshot objects to JSONL files, compacts to Parquet.

    Path structure: base_dir/{season}/{game_type}/{date}/{game_id}.ext
    """

    def __init__(self, base_dir: str | Path = "data/snapshots") -> None:
        self.base_dir = Path(base_dir)
        self._lock = threading.Lock()

    def _dir(self, game_id: str, date: str) -> Path:
        """Return the directory for a game's snapshot files."""
        season, game_type = parse_game_id(game_id)
        return self.base_dir / season / game_type / date

    def _path(self, game_id: str, date: str) -> Path:
        """JSONL path: base_dir/{season}/{game_type}/{date}/{game_id}.jsonl"""
        return self._dir(game_id, date) / f"{game_id}.jsonl"

    def _parquet_path(self, game_id: str, date: str) -> Path:
        """Parquet path: base_dir/{season}/{game_type}/{date}/{game_id}.parquet"""
        return self._dir(game_id, date) / f"{game_id}.parquet"

    # ------------------------------------------------------------------
    # Live ingestion
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

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def list_games(self, date: str, season: str | None = None,
                   game_type: str | None = None) -> list[str]:
        """Return game IDs available for a given date.

        Args:
            date: Date string YYYY-MM-DD.
            season: Filter to season (e.g. "2025-26"). If None, searches all.
            game_type: Filter to type (e.g. "regular"). If None, searches all.

        Returns:
            Sorted, deduplicated list of game IDs.
        """
        game_ids: set[str] = set()

        if season and game_type:
            # Fast path: exact directory
            date_dir = self.base_dir / season / game_type / date
            self._scan_dir(date_dir, game_ids)
        else:
            # Scan matching directories
            if not self.base_dir.exists():
                return []
            for season_dir in sorted(self.base_dir.iterdir()):
                if not season_dir.is_dir():
                    continue
                if season and season_dir.name != season:
                    continue
                for type_dir in sorted(season_dir.iterdir()):
                    if not type_dir.is_dir():
                        continue
                    if game_type and type_dir.name != game_type:
                        continue
                    date_dir = type_dir / date
                    self._scan_dir(date_dir, game_ids)

        return sorted(game_ids)

    def list_dates(self, season: str | None = None,
                   game_type: str | None = None) -> list[str]:
        """Return all dates that have stored snapshots.

        Args:
            season: Filter to season. If None, searches all.
            game_type: Filter to type. If None, searches all.

        Returns:
            Sorted list of date strings (YYYY-MM-DD).
        """
        dates: set[str] = set()

        if not self.base_dir.exists():
            return []

        for season_dir in sorted(self.base_dir.iterdir()):
            if not season_dir.is_dir():
                continue
            if season and season_dir.name != season:
                continue
            for type_dir in sorted(season_dir.iterdir()):
                if not type_dir.is_dir():
                    continue
                if game_type and type_dir.name != game_type:
                    continue
                for date_dir in sorted(type_dir.iterdir()):
                    if date_dir.is_dir():
                        dates.add(date_dir.name)

        return sorted(dates)

    @staticmethod
    def _scan_dir(date_dir: Path, game_ids: set[str]) -> None:
        if not date_dir.exists():
            return
        for path in date_dir.iterdir():
            if path.suffix in (".parquet", ".jsonl"):
                game_ids.add(path.stem)

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
