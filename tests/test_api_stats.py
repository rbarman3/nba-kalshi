"""Tests for the API stats collector service and SLO tracking."""
import asyncio
import json
import time
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import patch

import pytest

from pipeline.models import PollResult
from pipeline.api_stats import (
    ApiStatsCollector,
    SLO_AVAILABILITY_PCT,
    SLO_LATENCY_P50_MS,
    SLO_LATENCY_P95_MS,
    SLO_LATENCY_P99_MS,
    SLO_MAX_CONSECUTIVE_FAILURES,
    _compute_percentile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_result(
    game_id: str = "0022500001",
    status_code: int = 200,
    error_type: str | None = None,
    response_time_ms: float = 50.0,
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
# TestPollResult
# ---------------------------------------------------------------------------

class TestPollResult:
    def test_frozen(self):
        result = _make_result()
        with pytest.raises(FrozenInstanceError):
            result.status_code = 500  # type: ignore[misc]

    def test_fields(self):
        result = _make_result(
            game_id="0022500099",
            status_code=403,
            error_type=None,
            response_time_ms=120.5,
            timestamp=1000.0,
        )
        assert result.game_id == "0022500099"
        assert result.status_code == 403
        assert result.error_type is None
        assert result.response_time_ms == 120.5
        assert result.timestamp == 1000.0

    def test_error_result(self):
        result = _make_result(status_code=-1, error_type="TimeoutException", response_time_ms=10000.0)
        assert result.status_code == -1
        assert result.error_type == "TimeoutException"


# ---------------------------------------------------------------------------
# TestRecord
# ---------------------------------------------------------------------------

class TestRecord:
    def test_record_appends_to_buffer(self):
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        r1 = _make_result()
        r2 = _make_result(status_code=403)
        collector.record(r1)
        collector.record(r2)
        assert len(collector._buffer) == 2

    def test_record_is_synchronous(self):
        """record() should not be a coroutine — it's called inline by transport."""
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        result = collector.record(_make_result())
        assert not asyncio.iscoroutine(result)


# ---------------------------------------------------------------------------
# TestConsecutiveFailures
# ---------------------------------------------------------------------------

class TestConsecutiveFailures:
    def test_increments_on_non_200(self):
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        collector.record(_make_result(status_code=403))
        collector.record(_make_result(status_code=-1, error_type="TimeoutException"))
        assert collector._consecutive_failures["0022500001"] == 2

    def test_resets_on_200(self):
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        collector.record(_make_result(status_code=403))
        collector.record(_make_result(status_code=403))
        collector.record(_make_result(status_code=200))
        assert collector._consecutive_failures["0022500001"] == 0

    def test_tracks_high_water_mark(self):
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        for _ in range(3):
            collector.record(_make_result(status_code=500))
        collector.record(_make_result(status_code=200))
        collector.record(_make_result(status_code=500))

        assert collector._consecutive_failures["0022500001"] == 1
        assert collector._max_consecutive_failures["0022500001"] == 3

    def test_per_game_isolation(self):
        collector = ApiStatsCollector(base_dir="/tmp/unused")
        collector.record(_make_result(game_id="0022500001", status_code=500))
        collector.record(_make_result(game_id="0022500002", status_code=200))
        assert collector._consecutive_failures["0022500001"] == 1
        assert collector._consecutive_failures["0022500002"] == 0


# ---------------------------------------------------------------------------
# TestFlush
# ---------------------------------------------------------------------------

class TestFlush:
    @pytest.mark.asyncio
    async def test_creates_jsonl_file(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result(timestamp=1000.0))
        collector.record(_make_result(status_code=403, timestamp=1001.0))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        jsonl_path = tmp_path / "2026-04-11.jsonl"
        assert jsonl_path.exists()
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) == 2

        first = json.loads(lines[0])
        assert first["status_code"] == 200
        assert first["game_id"] == "0022500001"

    @pytest.mark.asyncio
    async def test_creates_summary_json(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(10):
            collector.record(_make_result(response_time_ms=50.0))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary_path = tmp_path / "2026-04-11_summary.json"
        assert summary_path.exists()
        summary = json.loads(summary_path.read_text())
        assert summary["date"] == "2026-04-11"
        assert summary["total_polls"] == 10

    @pytest.mark.asyncio
    async def test_atomic_write_no_tmp_left(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result())

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        tmp_files = list(tmp_path.glob("*.tmp"))
        assert tmp_files == []

    @pytest.mark.asyncio
    async def test_flush_clears_buffer(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result())

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        assert len(collector._buffer) == 0


# ---------------------------------------------------------------------------
# TestSummaryComputation
# ---------------------------------------------------------------------------

class TestSummaryComputation:
    @pytest.mark.asyncio
    async def test_by_game_and_by_status_counts(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(5):
            collector.record(_make_result(game_id="0022500001", status_code=200))
        for _ in range(3):
            collector.record(_make_result(game_id="0022500001", status_code=403))
        for _ in range(2):
            collector.record(_make_result(game_id="0022500002", status_code=200))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        assert summary["by_game"]["0022500001"]["by_status"]["200"] == 5
        assert summary["by_game"]["0022500001"]["by_status"]["403"] == 3
        assert summary["by_game"]["0022500002"]["by_status"]["200"] == 2
        assert summary["aggregate"]["by_status"]["200"] == 7
        assert summary["aggregate"]["by_status"]["403"] == 3

    @pytest.mark.asyncio
    async def test_error_types_populated(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result(status_code=-1, error_type="TimeoutException"))
        collector.record(_make_result(status_code=-1, error_type="ConnectError"))
        collector.record(_make_result(status_code=-1, error_type="TimeoutException"))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        assert summary["aggregate"]["error_types"]["TimeoutException"] == 2
        assert summary["aggregate"]["error_types"]["ConnectError"] == 1

    @pytest.mark.asyncio
    async def test_availability_pct(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(99):
            collector.record(_make_result(status_code=200))
        collector.record(_make_result(status_code=500))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        assert summary["aggregate"]["availability_pct"] == 99.0

    @pytest.mark.asyncio
    async def test_latency_percentiles(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for i in range(1, 101):
            collector.record(_make_result(response_time_ms=float(i)))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        latency = summary["aggregate"]["latency"]
        assert latency["p50_ms"] == pytest.approx(50.0, abs=1.5)
        assert latency["p95_ms"] == pytest.approx(95.0, abs=1.5)
        assert latency["p99_ms"] == pytest.approx(99.0, abs=1.5)
        assert latency["min_ms"] == 1.0
        assert latency["max_ms"] == 100.0

    @pytest.mark.asyncio
    async def test_per_game_latency(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for i in range(1, 11):
            collector.record(_make_result(game_id="0022500001", response_time_ms=float(i) * 10))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        game_latency = summary["by_game"]["0022500001"]["latency"]
        assert game_latency["min_ms"] == 10.0
        assert game_latency["max_ms"] == 100.0


# ---------------------------------------------------------------------------
# TestSloCompliance
# ---------------------------------------------------------------------------

class TestSloCompliance:
    @pytest.mark.asyncio
    async def test_all_slos_met(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(100):
            collector.record(_make_result(status_code=200, response_time_ms=30.0))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        slo = summary["slo_compliance"]
        assert slo["latency_p50"]["met"] is True
        assert slo["latency_p95"]["met"] is True
        assert slo["latency_p99"]["met"] is True
        assert slo["availability"]["met"] is True
        assert slo["max_consecutive_failures"]["met"] is True

    @pytest.mark.asyncio
    async def test_latency_slo_breached(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(100):
            collector.record(_make_result(status_code=200, response_time_ms=600.0))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        slo = summary["slo_compliance"]
        assert slo["latency_p50"]["met"] is False
        assert slo["latency_p95"]["met"] is False
        assert slo["latency_p99"]["met"] is True

    @pytest.mark.asyncio
    async def test_availability_slo_breached(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(95):
            collector.record(_make_result(status_code=200))
        for _ in range(5):
            collector.record(_make_result(status_code=500))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        assert summary["slo_compliance"]["availability"]["met"] is False
        assert summary["slo_compliance"]["availability"]["actual_pct"] == 95.0

    @pytest.mark.asyncio
    async def test_consecutive_failures_slo_breached(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        for _ in range(6):
            collector.record(_make_result(status_code=500))
        collector.record(_make_result(status_code=200))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        summary = json.loads((tmp_path / "2026-04-11_summary.json").read_text())
        assert summary["slo_compliance"]["max_consecutive_failures"]["met"] is False
        assert summary["slo_compliance"]["max_consecutive_failures"]["actual"] == 6


# ---------------------------------------------------------------------------
# TestDayRollover
# ---------------------------------------------------------------------------

class TestDayRollover:
    @pytest.mark.asyncio
    async def test_rollover_produces_two_files(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result())

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        collector.record(_make_result(status_code=403))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-12"):
            await collector.flush()

        assert (tmp_path / "2026-04-11.jsonl").exists()
        assert (tmp_path / "2026-04-12.jsonl").exists()

    @pytest.mark.asyncio
    async def test_rollover_resets_consecutive_failures(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result(status_code=500))
        collector.record(_make_result(status_code=500))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        assert collector._max_consecutive_failures.get("0022500001", 0) == 2

        collector.record(_make_result(status_code=500))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-12"):
            await collector.flush()

        assert collector._max_consecutive_failures["0022500001"] == 1


# ---------------------------------------------------------------------------
# TestFlushSwapsBuffer
# ---------------------------------------------------------------------------

class TestFlushSwapsBuffer:
    @pytest.mark.asyncio
    async def test_records_during_flush_not_lost(self, tmp_path: Path):
        collector = ApiStatsCollector(base_dir=str(tmp_path))
        collector.record(_make_result(timestamp=1000.0))

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        collector.record(_make_result(timestamp=2000.0))
        assert len(collector._buffer) == 1

        with patch("pipeline.api_stats._today_str", return_value="2026-04-11"):
            await collector.flush()

        jsonl_path = tmp_path / "2026-04-11.jsonl"
        lines = jsonl_path.read_text().strip().split("\n")
        assert len(lines) == 2


# ---------------------------------------------------------------------------
# TestComputePercentile
# ---------------------------------------------------------------------------

class TestComputePercentile:
    def test_p50_of_sorted_range(self):
        values = list(range(1, 101))
        assert _compute_percentile(values, 50) == pytest.approx(50.0, abs=1.0)

    def test_p95_of_sorted_range(self):
        values = list(range(1, 101))
        assert _compute_percentile(values, 95) == pytest.approx(95.0, abs=1.0)

    def test_single_value(self):
        assert _compute_percentile([42.0], 50) == 42.0

    def test_empty_returns_zero(self):
        assert _compute_percentile([], 50) == 0.0
