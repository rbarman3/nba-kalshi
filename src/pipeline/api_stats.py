"""API stats collector — tracks HTTP poll results with SLO compliance.

Runs as a sidecar alongside NBATransport. The transport calls
``stats.record(result)`` synchronously after each HTTP poll; this service
accumulates results in memory and flushes to disk every FLUSH_INTERVAL seconds.

Output files (in base_dir):
    YYYY-MM-DD.jsonl          — append-only raw PollResult events
    YYYY-MM-DD_summary.json   — aggregated stats with SLO compliance
"""
import asyncio
import json
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path

from .models import PollResult

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SLO thresholds — starting values, calibrate against real CDN behavior
# ---------------------------------------------------------------------------
SLO_LATENCY_P50_MS = 100.0           # Median poll < 100ms
SLO_LATENCY_P95_MS = 500.0           # 95th percentile < 500ms
SLO_LATENCY_P99_MS = 2000.0          # 99th percentile < 2s (httpx timeout is 10s)
SLO_AVAILABILITY_PCT = 99.0           # 99% of polls return 200
SLO_MAX_CONSECUTIVE_FAILURES = 5      # Sustained outage vs transient blip
SLO_MAX_DATA_AGE_S = 3.0             # Max game-state staleness at trade time; enforced via FeedQualitySignal.is_tradeable()

FLUSH_INTERVAL = 30.0                 # seconds between disk flushes


def _today_str() -> str:
    """Return today's date as YYYY-MM-DD. Extracted for test patching."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _compute_percentile(sorted_values: list[float], pct: float) -> float:
    """Index-based percentile on a pre-sorted list. Returns 0.0 if empty."""
    if not sorted_values:
        return 0.0
    idx = int(len(sorted_values) * pct / 100)
    idx = min(idx, len(sorted_values) - 1)
    return sorted_values[idx]


class ApiStatsCollector:
    """Accumulates PollResult events and flushes daily stats + SLO compliance."""

    def __init__(self, base_dir: str | Path = "data/api_stats") -> None:
        self.base_dir = Path(base_dir)
        self._buffer: list[PollResult] = []
        self._consecutive_failures: dict[str, int] = {}
        self._max_consecutive_failures: dict[str, int] = {}
        self._lock = threading.Lock()
        self._current_date: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def record(self, result: PollResult) -> None:
        """Record a poll result. Synchronous — safe to call from transport."""
        self._buffer.append(result)
        self._track_consecutive(result)

    async def run(self) -> None:
        """Flush loop — runs forever via asyncio.gather."""
        while True:
            await asyncio.sleep(FLUSH_INTERVAL)
            await self.flush()

    async def flush(self) -> None:
        """Swap buffer and write to disk. Safe to call from tests."""
        if not self._buffer:
            return

        # Immutable swap — new records during flush go to a fresh list
        results = self._buffer
        self._buffer = []

        today = _today_str()

        # Day rollover — reset consecutive failure trackers and re-derive
        # from just the new day's results
        if self._current_date is not None and self._current_date != today:
            self._consecutive_failures = {}
            self._max_consecutive_failures = {}
            for r in results:
                self._track_consecutive(r)
        self._current_date = today

        await asyncio.to_thread(
            self._sync_write, results, today
        )

    # ------------------------------------------------------------------
    # Consecutive failure tracking
    # ------------------------------------------------------------------

    def _track_consecutive(self, result: PollResult) -> None:
        gid = result.game_id
        if result.status_code == 200:
            self._consecutive_failures[gid] = 0
        else:
            current = self._consecutive_failures.get(gid, 0) + 1
            self._consecutive_failures[gid] = current
            prev_max = self._max_consecutive_failures.get(gid, 0)
            self._max_consecutive_failures[gid] = max(prev_max, current)

    # ------------------------------------------------------------------
    # Disk I/O (runs in thread pool)
    # ------------------------------------------------------------------

    def _sync_write(self, results: list[PollResult], date: str) -> None:
        with self._lock:
            self.base_dir.mkdir(parents=True, exist_ok=True)

            # Append raw events to JSONL
            jsonl_path = self.base_dir / f"{date}.jsonl"
            with open(jsonl_path, "a") as f:
                for r in results:
                    f.write(json.dumps({
                        "game_id": r.game_id,
                        "status_code": r.status_code,
                        "error_type": r.error_type,
                        "response_time_ms": r.response_time_ms,
                        "timestamp": r.timestamp,
                    }) + "\n")

            # Recompute summary from full JSONL (single source of truth)
            all_results = self._load_jsonl(jsonl_path)
            summary = self._build_summary(all_results, date)

            # Atomic write
            summary_path = self.base_dir / f"{date}_summary.json"
            tmp_path = summary_path.with_suffix(".json.tmp")
            tmp_path.write_text(json.dumps(summary, indent=2))
            tmp_path.rename(summary_path)

    def _load_jsonl(self, path: Path) -> list[dict]:
        rows = []
        with open(path, "r") as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    # ------------------------------------------------------------------
    # Summary computation
    # ------------------------------------------------------------------

    def _build_summary(self, rows: list[dict], date: str) -> dict:
        total = len(rows)
        by_game: dict[str, dict] = {}
        agg_status: dict[str, int] = {}
        agg_errors: dict[str, int] = {}
        all_times: list[float] = []

        for row in rows:
            gid = row["game_id"]
            sc = str(row["status_code"])
            rt = row["response_time_ms"]

            # Per-game accumulation
            if gid not in by_game:
                by_game[gid] = {"total": 0, "by_status": {}, "times": []}
            by_game[gid]["total"] += 1
            by_game[gid]["by_status"][sc] = by_game[gid]["by_status"].get(sc, 0) + 1
            by_game[gid]["times"].append(rt)

            # Aggregate accumulation
            agg_status[sc] = agg_status.get(sc, 0) + 1
            all_times.append(rt)

            if row["error_type"]:
                agg_errors[row["error_type"]] = agg_errors.get(row["error_type"], 0) + 1

        # Compute aggregate latency
        sorted_times = sorted(all_times)
        agg_latency = self._latency_stats(sorted_times)

        # Compute per-game stats
        success_count_200 = agg_status.get("200", 0)
        availability_pct = (success_count_200 / total * 100) if total > 0 else 0.0

        game_summaries = {}
        for gid, gdata in by_game.items():
            game_times = sorted(gdata["times"])
            game_200 = gdata["by_status"].get("200", 0)
            game_total = gdata["total"]
            game_summaries[gid] = {
                "total": game_total,
                "by_status": gdata["by_status"],
                "availability_pct": round(game_200 / game_total * 100, 2) if game_total > 0 else 0.0,
                "max_consecutive_failures": self._max_consecutive_failures.get(gid, 0),
                "latency": self._latency_stats(game_times),
            }

        # Overall max consecutive failures
        overall_max_consec = max(self._max_consecutive_failures.values()) if self._max_consecutive_failures else 0

        # SLO compliance
        slo = {
            "latency_p50": {
                "target_ms": SLO_LATENCY_P50_MS,
                "actual_ms": agg_latency["p50_ms"],
                "met": agg_latency["p50_ms"] <= SLO_LATENCY_P50_MS,
            },
            "latency_p95": {
                "target_ms": SLO_LATENCY_P95_MS,
                "actual_ms": agg_latency["p95_ms"],
                "met": agg_latency["p95_ms"] <= SLO_LATENCY_P95_MS,
            },
            "latency_p99": {
                "target_ms": SLO_LATENCY_P99_MS,
                "actual_ms": agg_latency["p99_ms"],
                "met": agg_latency["p99_ms"] <= SLO_LATENCY_P99_MS,
            },
            "availability": {
                "target_pct": SLO_AVAILABILITY_PCT,
                "actual_pct": round(availability_pct, 2),
                "met": availability_pct >= SLO_AVAILABILITY_PCT,
            },
            "max_consecutive_failures": {
                "target": SLO_MAX_CONSECUTIVE_FAILURES,
                "actual": overall_max_consec,
                "met": overall_max_consec <= SLO_MAX_CONSECUTIVE_FAILURES,
            },
        }

        return {
            "date": date,
            "total_polls": total,
            "by_game": game_summaries,
            "aggregate": {
                "by_status": agg_status,
                "error_types": agg_errors,
                "availability_pct": round(availability_pct, 2),
                "latency": agg_latency,
            },
            "slo_compliance": slo,
        }

    @staticmethod
    def _latency_stats(sorted_times: list[float]) -> dict:
        return {
            "p50_ms": round(_compute_percentile(sorted_times, 50), 2),
            "p95_ms": round(_compute_percentile(sorted_times, 95), 2),
            "p99_ms": round(_compute_percentile(sorted_times, 99), 2),
            "min_ms": round(sorted_times[0], 2) if sorted_times else 0.0,
            "max_ms": round(sorted_times[-1], 2) if sorted_times else 0.0,
        }
