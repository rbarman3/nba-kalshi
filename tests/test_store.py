"""Tests for pipeline.store — snapshot persistence (JSONL + Parquet)."""
import json
import tempfile
from pathlib import Path

import pytest

from pipeline.models import RawSnapshot
from pipeline.store import SnapshotStore, parse_game_id

# Valid NBA game IDs for tests
GAME_1 = "0022500001"  # 2025-26 regular season game 1
GAME_2 = "0022500002"  # 2025-26 regular season game 2
PLAYOFF = "0042500001"  # 2025-26 playoffs game 1
DATE = "2026-03-29"


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
        game_id=GAME_1,
        payload={"game": {"gameStatus": 2}},
        fetched_at=1000.0,
    )


# ------------------------------------------------------------------
# parse_game_id
# ------------------------------------------------------------------


class TestParseGameId:
    def test_regular_season(self):
        season, game_type = parse_game_id("0022500001")
        assert season == "2025-26"
        assert game_type == "regular"

    def test_playoffs(self):
        season, game_type = parse_game_id("0042500001")
        assert season == "2025-26"
        assert game_type == "playoffs"

    def test_preseason(self):
        season, game_type = parse_game_id("0012400001")
        assert season == "2024-25"
        assert game_type == "preseason"

    def test_playin(self):
        season, game_type = parse_game_id("0052500001")
        assert season == "2025-26"
        assert game_type == "playin"

    def test_short_id_raises(self):
        with pytest.raises(ValueError, match="too short"):
            parse_game_id("002")

    def test_unknown_type_raises(self):
        with pytest.raises(ValueError, match="Unknown game type"):
            parse_game_id("0092500001")


# ------------------------------------------------------------------
# Path generation
# ------------------------------------------------------------------


class TestPathGeneration:
    def test_path_structure(self, store, temp_store_dir):
        """_path includes season/type/date hierarchy."""
        path = store._path(GAME_1, DATE)
        assert path == temp_store_dir / "2025-26" / "regular" / DATE / f"{GAME_1}.jsonl"

    def test_parquet_path_structure(self, store, temp_store_dir):
        path = store._parquet_path(GAME_1, DATE)
        assert path == temp_store_dir / "2025-26" / "regular" / DATE / f"{GAME_1}.parquet"

    def test_playoff_path(self, store, temp_store_dir):
        path = store._path(PLAYOFF, DATE)
        assert path == temp_store_dir / "2025-26" / "playoffs" / DATE / f"{PLAYOFF}.jsonl"


# ------------------------------------------------------------------
# JSONL persist
# ------------------------------------------------------------------


class TestPersist:
    @pytest.mark.asyncio
    async def test_persist_creates_directory(self, store, temp_store_dir, snapshot):
        await store.persist(snapshot, date=DATE)
        expected_dir = temp_store_dir / "2025-26" / "regular" / DATE
        assert expected_dir.exists()

    @pytest.mark.asyncio
    async def test_persist_writes_json_line(self, store, snapshot):
        await store.persist(snapshot, date=DATE)
        path = store._path(GAME_1, DATE)
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 1
        data = json.loads(lines[0])
        assert data["game_id"] == GAME_1
        assert data["fetched_at"] == 1000.0

    @pytest.mark.asyncio
    async def test_persist_appends(self, store):
        snap1 = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1000.0)
        snap2 = RawSnapshot(game_id=GAME_1, payload={"b": 2}, fetched_at=1001.0)

        await store.persist(snap1, date=DATE)
        await store.persist(snap2, date=DATE)

        path = store._path(GAME_1, DATE)
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2

    @pytest.mark.asyncio
    async def test_persist_preserves_payload_dict(self, store):
        payload = {"game": {"gameStatus": 2, "homeTeam": {"score": 100}}}
        snap = RawSnapshot(game_id=GAME_1, payload=payload, fetched_at=1000.0)

        await store.persist(snap, date=DATE)

        path = store._path(GAME_1, DATE)
        data = json.loads(path.read_text())
        assert data["payload"] == payload


# ------------------------------------------------------------------
# JSONL load
# ------------------------------------------------------------------


class TestLoad:
    @pytest.mark.asyncio
    async def test_load_returns_list_of_snapshots(self, store):
        snap1 = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1000.0)
        snap2 = RawSnapshot(game_id=GAME_1, payload={"b": 2}, fetched_at=1001.0)

        await store.persist(snap1, date=DATE)
        await store.persist(snap2, date=DATE)

        loaded = store.load(GAME_1, DATE)
        assert len(loaded) == 2
        assert loaded[0].payload == {"a": 1}
        assert loaded[1].payload == {"b": 2}

    @pytest.mark.asyncio
    async def test_load_round_trips_fields(self, store):
        payload = {"game": {"gameStatus": 2}}
        snap = RawSnapshot(game_id=GAME_1, payload=payload, fetched_at=1000.5)
        await store.persist(snap, date=DATE)

        loaded = store.load(GAME_1, DATE)
        assert len(loaded) == 1
        assert loaded[0].game_id == GAME_1
        assert loaded[0].payload == payload
        assert loaded[0].fetched_at == 1000.5

    def test_load_missing_file_returns_empty(self, store):
        loaded = store.load(GAME_2, DATE)
        assert loaded == []

    @pytest.mark.asyncio
    async def test_load_preserves_order(self, store):
        for i, ts in enumerate([1000.0, 1001.0, 1002.0]):
            snap = RawSnapshot(game_id=GAME_1, payload={"i": i}, fetched_at=ts)
            await store.persist(snap, date=DATE)

        loaded = store.load(GAME_1, DATE)
        assert [s.payload["i"] for s in loaded] == [0, 1, 2]


# ------------------------------------------------------------------
# Compaction — JSONL → Parquet
# ------------------------------------------------------------------


class TestCompact:
    @pytest.mark.asyncio
    async def test_compact_creates_parquet_file(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1000.0)
        await store.persist(snap, date=DATE)

        path = await store.compact(GAME_1, DATE)
        assert path.suffix == ".parquet"
        assert path.exists()

    @pytest.mark.asyncio
    async def test_compact_removes_jsonl(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1000.0)
        await store.persist(snap, date=DATE)

        jsonl_path = store._path(GAME_1, DATE)
        assert jsonl_path.exists()

        await store.compact(GAME_1, DATE)
        assert not jsonl_path.exists()

    @pytest.mark.asyncio
    async def test_compact_round_trip(self, store):
        payload = {"game": {"homeTeam": {"score": 110}, "gameStatus": 2}}
        snap = RawSnapshot(game_id=GAME_1, payload=payload, fetched_at=1234.5)
        await store.persist(snap, date=DATE)

        await store.compact(GAME_1, DATE)

        loaded = store.load(GAME_1, DATE)
        assert len(loaded) == 1
        assert loaded[0].game_id == GAME_1
        assert loaded[0].payload == payload
        assert loaded[0].fetched_at == 1234.5

    @pytest.mark.asyncio
    async def test_compact_preserves_order(self, store):
        for i in range(5):
            snap = RawSnapshot(
                game_id=GAME_1,
                payload={"seq": i},
                fetched_at=1000.0 + i,
            )
            await store.persist(snap, date=DATE)

        await store.compact(GAME_1, DATE)
        loaded = store.load(GAME_1, DATE)

        assert [s.payload["seq"] for s in loaded] == [0, 1, 2, 3, 4]
        assert [s.fetched_at for s in loaded] == [1000.0, 1001.0, 1002.0, 1003.0, 1004.0]

    @pytest.mark.asyncio
    async def test_compact_missing_file_raises(self, store):
        with pytest.raises(FileNotFoundError):
            await store.compact(GAME_2, DATE)

    @pytest.mark.asyncio
    async def test_compact_empty_file_raises(self, store):
        path = store._path(GAME_1, DATE)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("")

        with pytest.raises(ValueError):
            await store.compact(GAME_1, DATE)


# ------------------------------------------------------------------
# Dual-format loading — Parquet preferred over JSONL
# ------------------------------------------------------------------


class TestLoadDualFormat:
    @pytest.mark.asyncio
    async def test_load_prefers_parquet(self, store):
        snap_a = RawSnapshot(game_id=GAME_1, payload={"src": "jsonl"}, fetched_at=1.0)
        snap_b = RawSnapshot(game_id=GAME_1, payload={"src": "parquet"}, fetched_at=2.0)
        await store.persist(snap_a, date=DATE)
        await store.persist(snap_b, date=DATE)

        await store.compact(GAME_1, DATE)

        # Re-create JSONL with different data to prove parquet wins
        snap_new = RawSnapshot(game_id=GAME_1, payload={"src": "new_jsonl"}, fetched_at=3.0)
        await store.persist(snap_new, date=DATE)

        loaded = store.load(GAME_1, DATE)
        assert len(loaded) == 2
        assert loaded[0].payload == {"src": "jsonl"}
        assert loaded[1].payload == {"src": "parquet"}

    @pytest.mark.asyncio
    async def test_load_falls_back_to_jsonl(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"fmt": "jsonl"}, fetched_at=1.0)
        await store.persist(snap, date=DATE)

        loaded = store.load(GAME_1, DATE)
        assert len(loaded) == 1
        assert loaded[0].payload == {"fmt": "jsonl"}

    @pytest.mark.asyncio
    async def test_parquet_payload_is_dict(self, store):
        payload = {"game": {"nested": {"deep": True}}}
        snap = RawSnapshot(game_id=GAME_1, payload=payload, fetched_at=1.0)
        await store.persist(snap, date=DATE)
        await store.compact(GAME_1, DATE)

        loaded = store.load(GAME_1, DATE)
        assert isinstance(loaded[0].payload, dict)
        assert loaded[0].payload == payload


# ------------------------------------------------------------------
# Discovery — list_games, list_dates
# ------------------------------------------------------------------


class TestListGames:
    @pytest.mark.asyncio
    async def test_list_games_finds_jsonl(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1.0)
        await store.persist(snap, date=DATE)

        games = store.list_games(DATE)
        assert games == [GAME_1]

    @pytest.mark.asyncio
    async def test_list_games_finds_parquet(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1.0)
        await store.persist(snap, date=DATE)
        await store.compact(GAME_1, DATE)

        games = store.list_games(DATE)
        assert games == [GAME_1]

    @pytest.mark.asyncio
    async def test_list_games_multiple(self, store):
        for gid in [GAME_1, GAME_2]:
            snap = RawSnapshot(game_id=gid, payload={"a": 1}, fetched_at=1.0)
            await store.persist(snap, date=DATE)

        games = store.list_games(DATE)
        assert games == sorted([GAME_1, GAME_2])

    def test_list_games_empty_date(self, store):
        assert store.list_games("2099-01-01") == []

    @pytest.mark.asyncio
    async def test_list_games_filter_by_season(self, store):
        snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1.0)
        await store.persist(snap, date=DATE)

        assert store.list_games(DATE, season="2025-26") == [GAME_1]
        assert store.list_games(DATE, season="2024-25") == []

    @pytest.mark.asyncio
    async def test_list_games_filter_by_type(self, store):
        for gid in [GAME_1, PLAYOFF]:
            snap = RawSnapshot(game_id=gid, payload={"a": 1}, fetched_at=1.0)
            await store.persist(snap, date=DATE)

        assert store.list_games(DATE, game_type="regular") == [GAME_1]
        assert store.list_games(DATE, game_type="playoffs") == [PLAYOFF]


class TestListDates:
    @pytest.mark.asyncio
    async def test_list_dates(self, store):
        for d in ["2026-03-29", "2026-03-30"]:
            snap = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1.0)
            await store.persist(snap, date=d)

        dates = store.list_dates()
        assert dates == ["2026-03-29", "2026-03-30"]

    @pytest.mark.asyncio
    async def test_list_dates_filter_by_type(self, store):
        snap_reg = RawSnapshot(game_id=GAME_1, payload={"a": 1}, fetched_at=1.0)
        snap_po = RawSnapshot(game_id=PLAYOFF, payload={"a": 1}, fetched_at=1.0)
        await store.persist(snap_reg, date="2026-03-29")
        await store.persist(snap_po, date="2026-04-20")

        assert store.list_dates(game_type="regular") == ["2026-03-29"]
        assert store.list_dates(game_type="playoffs") == ["2026-04-20"]

    def test_list_dates_empty(self, store):
        assert store.list_dates() == []
