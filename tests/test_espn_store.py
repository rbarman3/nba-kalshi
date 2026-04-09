"""Tests for pipeline.espn_store — ESPN play-by-play persistence."""
import tempfile
from pathlib import Path

import pytest

from pipeline.espn_store import ESPNStore
from pipeline.models import (
    ESPNPlayByPlaySnapshot,
    FoulEvent,
    PeriodEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)

DATE = "2026-03-29"
GAME_ID = "401584793"


@pytest.fixture
def temp_store_dir():
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)


@pytest.fixture
def store(temp_store_dir):
    return ESPNStore(base_dir=temp_store_dir)


@pytest.fixture
def snapshot():
    return ESPNPlayByPlaySnapshot(
        espn_game_id=GAME_ID,
        payload={"gamepackageJSON": {"plays": []}},
        fetched_at=1000.0,
    )


@pytest.fixture
def sample_events():
    common = dict(
        game_id=GAME_ID,
        espn_play_id="1",
        sequence=1,
        period=1,
        clock="10:00",
        home_score=0,
        away_score=2,
        text="test play",
        wallclock="2026-03-29T00:01:00Z",
        observed_at=1000.0,
    )
    return [
        ScoringPlayEvent(
            **common,
            score_value=2,
            team_id="2",
            player_id="4065648",
            play_type="Jump Shot",
        ),
        FoulEvent(
            **{**common, "espn_play_id": "2", "sequence": 2},
            team_id="13",
            player_id="4066261",
            foul_type="Shooting Foul",
        ),
        TimeoutEvent(
            **{**common, "espn_play_id": "3", "sequence": 3},
            team_id="13",
            timeout_type="Full Timeout",
        ),
        TurnoverEvent(
            **{**common, "espn_play_id": "4", "sequence": 4},
            team_id="13",
            player_id="1966",
            turnover_type="Bad Pass Turnover",
        ),
        PeriodEvent(
            **{**common, "espn_play_id": "5", "sequence": 5},
            event_type="start",
        ),
        SubstitutionEvent(
            **{**common, "espn_play_id": "6", "sequence": 6},
            team_id="13",
            player_in_id="4395725",
            player_out_id="4066354",
        ),
    ]


# ------------------------------------------------------------------
# Snapshot save/load
# ------------------------------------------------------------------


class TestSnapshotPersistence:
    def test_save_creates_file(self, store, snapshot, temp_store_dir):
        path = store.save_snapshot(snapshot, DATE)
        assert path.exists()
        assert temp_store_dir in path.parents

    def test_round_trip(self, store, snapshot):
        store.save_snapshot(snapshot, DATE)
        loaded = store.load_snapshot(GAME_ID, DATE)

        assert loaded is not None
        assert loaded.espn_game_id == GAME_ID
        assert loaded.payload == snapshot.payload
        assert loaded.fetched_at == 1000.0

    def test_load_missing_returns_none(self, store):
        assert store.load_snapshot("nonexistent", DATE) is None


# ------------------------------------------------------------------
# Event save/load
# ------------------------------------------------------------------


class TestEventPersistence:
    def test_save_creates_file(self, store, sample_events, temp_store_dir):
        path = store.save_events(GAME_ID, DATE, sample_events)
        assert path.exists()

    def test_round_trip_all_types(self, store, sample_events):
        store.save_events(GAME_ID, DATE, sample_events)
        loaded = store.load_events(GAME_ID, DATE)

        assert len(loaded) == len(sample_events)
        types = [type(e).__name__ for e in loaded]
        assert "ScoringPlayEvent" in types
        assert "FoulEvent" in types
        assert "TimeoutEvent" in types
        assert "TurnoverEvent" in types
        assert "PeriodEvent" in types
        assert "SubstitutionEvent" in types

    def test_scoring_event_fields_preserved(self, store, sample_events):
        store.save_events(GAME_ID, DATE, sample_events)
        loaded = store.load_events(GAME_ID, DATE)
        scoring = [e for e in loaded if isinstance(e, ScoringPlayEvent)][0]

        assert scoring.game_id == GAME_ID
        assert scoring.score_value == 2
        assert scoring.player_id == "4065648"
        assert scoring.play_type == "Jump Shot"

    def test_substitution_event_fields_preserved(self, store, sample_events):
        store.save_events(GAME_ID, DATE, sample_events)
        loaded = store.load_events(GAME_ID, DATE)
        sub = [e for e in loaded if isinstance(e, SubstitutionEvent)][0]

        assert sub.player_in_id == "4395725"
        assert sub.player_out_id == "4066354"
        assert sub.team_id == "13"

    def test_load_missing_returns_empty(self, store):
        assert store.load_events("nonexistent", DATE) == []

    def test_empty_events_round_trip(self, store):
        store.save_events(GAME_ID, DATE, [])
        loaded = store.load_events(GAME_ID, DATE)
        assert loaded == []


# ------------------------------------------------------------------
# Discovery
# ------------------------------------------------------------------


class TestDiscovery:
    def test_list_games(self, store, snapshot):
        store.save_snapshot(snapshot, DATE)
        games = store.list_games(DATE)
        assert games == [GAME_ID]

    def test_list_games_multiple(self, store):
        for gid in ["401584793", "401584794"]:
            snap = ESPNPlayByPlaySnapshot(
                espn_game_id=gid, payload={}, fetched_at=1.0
            )
            store.save_snapshot(snap, DATE)

        games = store.list_games(DATE)
        assert games == ["401584793", "401584794"]

    def test_list_games_empty(self, store):
        assert store.list_games("2099-01-01") == []

    def test_list_dates(self, store, snapshot):
        store.save_snapshot(snapshot, DATE)
        store.save_snapshot(
            ESPNPlayByPlaySnapshot(espn_game_id="x", payload={}, fetched_at=1.0),
            "2026-03-30",
        )
        dates = store.list_dates()
        assert dates == ["2026-03-29", "2026-03-30"]

    def test_list_dates_empty(self, store):
        assert store.list_dates() == []
