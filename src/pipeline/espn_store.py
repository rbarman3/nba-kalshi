"""ESPN play-by-play persistence — JSONL storage for scraped game data.

Independent storage layer for ESPN play-by-play data. Stores both raw
snapshots and processed events so they can be loaded for backtesting.

Directory layout:
    base_dir/espn/{date}/{espn_game_id}.jsonl       (raw plays)
    base_dir/espn/{date}/{espn_game_id}.events.jsonl (processed events)
"""
import json
import threading
from dataclasses import asdict
from pathlib import Path

from .models import (
    ESPNPlayByPlaySnapshot,
    FoulEvent,
    PeriodEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)

_EVENT_TYPE_MAP = {
    "ScoringPlayEvent": ScoringPlayEvent,
    "FoulEvent": FoulEvent,
    "TimeoutEvent": TimeoutEvent,
    "TurnoverEvent": TurnoverEvent,
    "PeriodEvent": PeriodEvent,
    "SubstitutionEvent": SubstitutionEvent,
}


class ESPNStore:
    """Persist and load ESPN play-by-play data for backtesting.

    Stores raw ESPN snapshots and classified events as JSONL files,
    organized by date and ESPN game ID.

    Path structure:
        base_dir/espn/{date}/{espn_game_id}.jsonl        — raw snapshot
        base_dir/espn/{date}/{espn_game_id}.events.jsonl  — processed events
    """

    def __init__(self, base_dir: str | Path = "data/espn") -> None:
        self.base_dir = Path(base_dir)
        self._lock = threading.Lock()

    def _date_dir(self, date: str) -> Path:
        return self.base_dir / date

    def _snapshot_path(self, espn_game_id: str, date: str) -> Path:
        return self._date_dir(date) / f"{espn_game_id}.jsonl"

    def _events_path(self, espn_game_id: str, date: str) -> Path:
        return self._date_dir(date) / f"{espn_game_id}.events.jsonl"

    # ------------------------------------------------------------------
    # Save
    # ------------------------------------------------------------------

    def save_snapshot(
        self, snapshot: ESPNPlayByPlaySnapshot, date: str
    ) -> Path:
        """Save a raw ESPN play-by-play snapshot to JSONL.

        Args:
            snapshot: Raw ESPN snapshot to persist.
            date: Date string YYYY-MM-DD.

        Returns:
            Path to the written file.
        """
        path = self._snapshot_path(snapshot.espn_game_id, date)
        data = json.dumps({
            "espn_game_id": snapshot.espn_game_id,
            "payload": snapshot.payload,
            "fetched_at": snapshot.fetched_at,
        })
        self._write(path, data)
        return path

    def save_events(
        self, espn_game_id: str, date: str, events: list
    ) -> Path:
        """Save processed events to JSONL (one event per line).

        Args:
            espn_game_id: ESPN game identifier.
            date: Date string YYYY-MM-DD.
            events: List of typed event dataclasses.

        Returns:
            Path to the written file.
        """
        path = self._events_path(espn_game_id, date)
        lines = []
        for event in events:
            record = asdict(event)
            record["_type"] = type(event).__name__
            lines.append(json.dumps(record))
        self._write(path, "\n".join(lines))
        return path

    def _write(self, path: Path, content: str) -> None:
        with self._lock:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content + "\n")

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load_snapshot(
        self, espn_game_id: str, date: str
    ) -> ESPNPlayByPlaySnapshot | None:
        """Load a raw ESPN snapshot from storage.

        Returns None if not found.
        """
        path = self._snapshot_path(espn_game_id, date)
        if not path.exists():
            return None

        data = json.loads(path.read_text().strip())
        return ESPNPlayByPlaySnapshot(
            espn_game_id=data["espn_game_id"],
            payload=data["payload"],
            fetched_at=data["fetched_at"],
        )

    def load_events(self, espn_game_id: str, date: str) -> list:
        """Load processed events for a game.

        Returns empty list if not found.
        """
        path = self._events_path(espn_game_id, date)
        if not path.exists():
            return []

        events = []
        for line in path.read_text().strip().split("\n"):
            if not line:
                continue
            record = json.loads(line)
            event_type_name = record.pop("_type", None)
            event_cls = _EVENT_TYPE_MAP.get(event_type_name)
            if event_cls is None:
                continue
            events.append(event_cls(**record))
        return events

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def list_games(self, date: str) -> list[str]:
        """Return ESPN game IDs available for a given date.

        Returns:
            Sorted list of ESPN game IDs.
        """
        date_dir = self._date_dir(date)
        if not date_dir.exists():
            return []

        game_ids: set[str] = set()
        for path in date_dir.iterdir():
            if path.suffix == ".jsonl" and not path.stem.endswith(".events"):
                game_ids.add(path.stem)
        return sorted(game_ids)

    def list_dates(self) -> list[str]:
        """Return all dates that have stored ESPN data.

        Returns:
            Sorted list of date strings (YYYY-MM-DD).
        """
        if not self.base_dir.exists():
            return []

        dates = []
        for d in sorted(self.base_dir.iterdir()):
            if d.is_dir():
                dates.append(d.name)
        return dates
