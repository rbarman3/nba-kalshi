"""Tests for pipeline.store — snapshot persistence (JSONL + Parquet)."""
import json
import tempfile
from pathlib import Path

import pytest

from pipeline.models import RawSnapshot
from pipeline.store import SnapshotStore


@pytest.fixture
def temp_store_dir():
    """Temporary directory for test snapshots."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def store(temp_store_dir):
    """SnapshotStore pointing to temp directory."""
    return SnapshotStore(base_dir=temp_store_dir)


@pytest.fixture
def snapshot():
    """Sample RawSnapshot."""
    return RawSnapshot(
        game_id="0022500001",
        payload={"game": {"gameStatus": 2}},
        fetched_at=1000.0,
    )


# ------------------------------------------------------------------
# Path generation
# ------------------------------------------------------------------


class TestPathGeneration:
    def test_path_structure(self, store, temp_store_dir):
        """_path returns base_dir/YYYY-MM-DD/game_id.jsonl"""
        path = store._path("0022500001", "2026-03-29")
        assert path == temp_store_dir / "2026-03-29" / "0022500001.jsonl"


class TestParquetPath:
    def test_parquet_path_structure(self, store, temp_store_dir):
        """_parquet_path returns base_dir/YYYY-MM-DD/game_id.parquet"""
        path = store._parquet_path("0022500001", "2026-03-29")
        assert path == temp_store_dir / "2026-03-29" / "0022500001.parquet"


# ------------------------------------------------------------------
# JSONL persist (unchanged behavior)
# ------------------------------------------------------------------


class TestPersist:
    @pytest.mark.asyncio
    async def test_persist_creates_directory(self, store, temp_store_dir, snapshot):
        """persist() creates the directory if it doesn't exist."""
        await store.persist(snapshot, date="2026-03-29")
        assert (temp_store_dir / "2026-03-29").exists()

    @pytest.mark.asyncio
    async def test_persist_writes_json_line(self, store, temp_store_dir, snapshot):
        """persist() writes a valid JSON line."""
        await store.persist(snapshot, date="2026-03-29")
        path = temp_store_dir / "2026-03-29" / "0022500001.jsonl"
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["game_id"] == "0022500001"
        assert data["fetched_at"] == 1000.0

    @pytest.mark.asyncio
    async def test_persist_appends(self, store, temp_store_dir, snapshot):
        """persist() appends — multiple calls create multiple lines."""
        snap1 = RawSnapshot(game_id="game1", payload={"a": 1}, fetched_at=1000.0)
        snap2 = RawSnapshot(game_id="game1", payload={"b": 2}, fetched_at=1001.0)

        await store.persist(snap1, date="2026-03-29")
        await store.persist(snap2, date="2026-03-29")

        path = temp_store_dir / "2026-03-29" / "game1.jsonl"
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2

    @pytest.mark.asyncio
    async def test_persist_preserves_payload_dict(self, store, temp_store_dir):
        """persist() round-trips the full payload dict."""
        payload = {"game": {"gameStatus": 2, "homeTeam": {"score": 100}}}
        snap = RawSnapshot(game_id="game1", payload=payload, fetched_at=1000.0)

        await store.persist(snap, date="2026-03-29")

        path = temp_store_dir / "2026-03-29" / "game1.jsonl"
        data = json.loads(path.read_text())
        assert data["payload"] == payload


# ------------------------------------------------------------------
# JSONL load (unchanged behavior)
# ------------------------------------------------------------------


class TestLoad:
    @pytest.mark.asyncio
    async def test_load_returns_list_of_snapshots(self, store, temp_store_dir, snapshot):
        """load() returns list[RawSnapshot] in insertion order."""
        snap1 = RawSnapshot(game_id="game1", payload={"a": 1}, fetched_at=1000.0)
        snap2 = RawSnapshot(game_id="game1", payload={"b": 2}, fetched_at=1001.0)

        await store.persist(snap1, date="2026-03-29")
        await store.persist(snap2, date="2026-03-29")

        loaded = store.load("game1", "2026-03-29")
        assert len(loaded) == 2
        assert loaded[0].payload == {"a": 1}
        assert loaded[1].payload == {"b": 2}

    def test_load_round_trips_fields(self, store, temp_store_dir):
        """load() preserves game_id, payload, fetched_at."""
        payload = {"game": {"gameStatus": 2}}
        snap = RawSnapshot(game_id="game1", payload=payload, fetched_at=1000.5)

        # Manually write JSON to simulate persisted snapshot
        date = "2026-03-29"
        path = temp_store_dir / date / "game1.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps({
            "game_id": snap.game_id,
            "payload": snap.payload,
            "fetched_at": snap.fetched_at,
        })
        path.write_text(line + "\n")

        loaded = store.load("game1", date)
        assert len(loaded) == 1
        assert loaded[0].game_id == "game1"
        assert loaded[0].payload == payload
        assert loaded[0].fetched_at == 1000.5

    def test_load_missing_file_returns_empty(self, store):
        """load() returns empty list if file doesn't exist."""
        loaded = store.load("nonexistent", "2026-03-29")
        assert loaded == []

    def test_load_preserves_order(self, store, temp_store_dir):
        """load() returns snapshots in the order they were written."""
        date = "2026-03-29"
        path = temp_store_dir / date / "game1.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)

        # Write three snapshots in order
        for i, ts in enumerate([1000.0, 1001.0, 1002.0]):
            line = json.dumps({"game_id": "game1", "payload": {"i": i}, "fetched_at": ts})
            with open(path, "a") as f:
                f.write(line + "\n")

        loaded = store.load("game1", date)
        assert [s.payload["i"] for s in loaded] == [0, 1, 2]


# ------------------------------------------------------------------
# Compaction — JSONL → Parquet
# ------------------------------------------------------------------


class TestCompact:
    @pytest.mark.asyncio
    async def test_compact_creates_parquet_file(self, store, temp_store_dir):
        """compact() produces a .parquet file."""
        snap = RawSnapshot(game_id="game1", payload={"a": 1}, fetched_at=1000.0)
        await store.persist(snap, date="2026-03-29")

        path = await store.compact("game1", "2026-03-29")
        assert path.suffix == ".parquet"
        assert path.exists()

    @pytest.mark.asyncio
    async def test_compact_removes_jsonl(self, store, temp_store_dir):
        """compact() deletes the source JSONL file."""
        snap = RawSnapshot(game_id="game1", payload={"a": 1}, fetched_at=1000.0)
        await store.persist(snap, date="2026-03-29")

        jsonl_path = store._path("game1", "2026-03-29")
        assert jsonl_path.exists()

        await store.compact("game1", "2026-03-29")
        assert not jsonl_path.exists()

    @pytest.mark.asyncio
    async def test_compact_round_trip(self, store):
        """compact() preserves all fields through JSONL → Parquet → load."""
        payload = {"game": {"homeTeam": {"score": 110}, "gameStatus": 2}}
        snap = RawSnapshot(game_id="game1", payload=payload, fetched_at=1234.5)
        await store.persist(snap, date="2026-03-29")

        await store.compact("game1", "2026-03-29")

        loaded = store.load("game1", "2026-03-29")
        assert len(loaded) == 1
        assert loaded[0].game_id == "game1"
        assert loaded[0].payload == payload
        assert loaded[0].fetched_at == 1234.5

    @pytest.mark.asyncio
    async def test_compact_preserves_order(self, store):
        """compact() maintains insertion order."""
        for i in range(5):
            snap = RawSnapshot(
                game_id="game1",
                payload={"seq": i},
                fetched_at=1000.0 + i,
            )
            await store.persist(snap, date="2026-03-29")

        await store.compact("game1", "2026-03-29")
        loaded = store.load("game1", "2026-03-29")

        assert [s.payload["seq"] for s in loaded] == [0, 1, 2, 3, 4]
        assert [s.fetched_at for s in loaded] == [1000.0, 1001.0, 1002.0, 1003.0, 1004.0]

    @pytest.mark.asyncio
    async def test_compact_missing_file_raises(self, store):
        """compact() raises FileNotFoundError when no JSONL exists."""
        with pytest.raises(FileNotFoundError):
            await store.compact("nonexistent", "2026-03-29")

    @pytest.mark.asyncio
    async def test_compact_empty_file_raises(self, store, temp_store_dir):
        """compact() raises ValueError when JSONL is empty."""
        date = "2026-03-29"
        path = store._path("game1", date)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")

        with pytest.raises(ValueError):
            await store.compact("game1", date)


# ------------------------------------------------------------------
# Dual-format loading — Parquet preferred over JSONL
# ------------------------------------------------------------------


class TestLoadDualFormat:
    @pytest.mark.asyncio
    async def test_load_prefers_parquet(self, store, temp_store_dir):
        """load() returns parquet data when both formats exist."""
        date = "2026-03-29"

        # Write JSONL with payload_a
        snap_jsonl = RawSnapshot(game_id="game1", payload={"src": "jsonl"}, fetched_at=1.0)
        await store.persist(snap_jsonl, date=date)

        # Write a different parquet file with payload_b
        snap_pq = RawSnapshot(game_id="game1", payload={"src": "parquet"}, fetched_at=2.0)
        await store.persist(snap_pq, date=date)  # adds second line to JSONL

        # Compact creates parquet from both lines
        await store.compact("game1", date)

        # Re-create JSONL with different data to prove parquet wins
        snap_new = RawSnapshot(game_id="game1", payload={"src": "new_jsonl"}, fetched_at=3.0)
        await store.persist(snap_new, date=date)

        loaded = store.load("game1", date)
        # Should get the 2 parquet rows, NOT the 1 new JSONL row
        assert len(loaded) == 2
        assert loaded[0].payload == {"src": "jsonl"}
        assert loaded[1].payload == {"src": "parquet"}

    @pytest.mark.asyncio
    async def test_load_falls_back_to_jsonl(self, store):
        """load() reads JSONL when no parquet file exists."""
        snap = RawSnapshot(game_id="game1", payload={"fmt": "jsonl"}, fetched_at=1.0)
        await store.persist(snap, date="2026-03-29")

        loaded = store.load("game1", "2026-03-29")
        assert len(loaded) == 1
        assert loaded[0].payload == {"fmt": "jsonl"}

    @pytest.mark.asyncio
    async def test_parquet_payload_is_dict(self, store):
        """After compact, load() returns payload as dict, not string."""
        payload = {"game": {"nested": {"deep": True}}}
        snap = RawSnapshot(game_id="game1", payload=payload, fetched_at=1.0)
        await store.persist(snap, date="2026-03-29")
        await store.compact("game1", "2026-03-29")

        loaded = store.load("game1", "2026-03-29")
        assert isinstance(loaded[0].payload, dict)
        assert loaded[0].payload == payload
