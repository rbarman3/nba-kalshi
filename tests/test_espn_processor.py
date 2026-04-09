"""Tests for pipeline.espn_processor — play-by-play classification."""
import json
from pathlib import Path

import pytest

from pipeline.espn_processor import ESPNPlayByPlayProcessor
from pipeline.models import (
    ESPNPlayByPlaySnapshot,
    FoulEvent,
    PeriodEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def pbp_payload() -> dict:
    return json.loads((FIXTURES / "espn_playbyplay_sample.json").read_text())


def _make_snapshot(payload: dict, game_id: str = "401584793") -> ESPNPlayByPlaySnapshot:
    return ESPNPlayByPlaySnapshot(
        espn_game_id=game_id,
        payload=payload,
        fetched_at=1000.0,
    )


def _payload_with_plays(plays: list) -> dict:
    return {
        "gamepackageJSON": {
            "header": {
                "id": "401584793",
                "competitions": [
                    {
                        "status": {"type": {"id": "3", "state": "post"}},
                        "competitors": [],
                    }
                ],
            },
            "plays": plays,
        }
    }


# ------------------------------------------------------------------
# Play classification
# ------------------------------------------------------------------


class TestClassification:
    def test_scoring_play(self):
        plays = [
            {
                "id": "2", "sequenceNumber": 2,
                "type": {"id": "574", "text": "Jump Shot"},
                "text": "Tatum makes jump shot", "period": {"number": 1},
                "clock": {"displayValue": "11:30"},
                "homeScore": "0", "awayScore": "2",
                "scoringPlay": True, "scoreValue": 2,
                "wallclock": "2026-03-29T00:01:00Z",
                "team": {"id": "2"},
                "participants": [{"athlete": {"id": "4065648"}}],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        event = events[0]
        assert isinstance(event, ScoringPlayEvent)
        assert event.score_value == 2
        assert event.player_id == "4065648"
        assert event.play_type == "Jump Shot"
        assert event.home_score == 0
        assert event.away_score == 2

    def test_foul_play(self):
        plays = [
            {
                "id": "3", "sequenceNumber": 3,
                "type": {"id": "44", "text": "Shooting Foul"},
                "text": "Davis shooting foul", "period": {"number": 1},
                "clock": {"displayValue": "10:45"},
                "homeScore": "0", "awayScore": "2",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:02:00Z",
                "team": {"id": "13"},
                "participants": [{"athlete": {"id": "4066261"}}],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], FoulEvent)
        assert events[0].foul_type == "Shooting Foul"
        assert events[0].player_id == "4066261"

    def test_timeout_play(self):
        plays = [
            {
                "id": "6", "sequenceNumber": 6,
                "type": {"id": "16", "text": "Full Timeout"},
                "text": "Lakers Full Timeout", "period": {"number": 1},
                "clock": {"displayValue": "9:15"},
                "homeScore": "0", "awayScore": "3",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:04:10Z",
                "team": {"id": "13"},
                "participants": [],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], TimeoutEvent)
        assert events[0].timeout_type == "Full Timeout"
        assert events[0].team_id == "13"

    def test_turnover_play(self):
        plays = [
            {
                "id": "5", "sequenceNumber": 5,
                "type": {"id": "62", "text": "Bad Pass Turnover"},
                "text": "LeBron bad pass turnover", "period": {"number": 1},
                "clock": {"displayValue": "9:20"},
                "homeScore": "0", "awayScore": "3",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:04:00Z",
                "team": {"id": "13"},
                "participants": [{"athlete": {"id": "1966"}}],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], TurnoverEvent)
        assert events[0].turnover_type == "Bad Pass Turnover"
        assert events[0].player_id == "1966"

    def test_period_end(self):
        plays = [
            {
                "id": "10", "sequenceNumber": 10,
                "type": {"id": "412", "text": "End Period"},
                "text": "End of 1st Quarter", "period": {"number": 1},
                "clock": {"displayValue": "0:00"},
                "homeScore": "28", "awayScore": "30",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:45:00Z",
                "team": {"id": "0"},
                "participants": [],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], PeriodEvent)
        assert events[0].event_type == "end"

    def test_period_start(self):
        plays = [
            {
                "id": "1", "sequenceNumber": 1,
                "type": {"id": "412", "text": "Start Period"},
                "text": "1st Quarter", "period": {"number": 1},
                "clock": {"displayValue": "12:00"},
                "homeScore": "0", "awayScore": "0",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:00:00Z",
                "team": {"id": "0"},
                "participants": [],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], PeriodEvent)
        assert events[0].event_type == "start"

    def test_substitution_play(self):
        plays = [
            {
                "id": "8", "sequenceNumber": 8,
                "type": {"id": "584", "text": "Substitution"},
                "text": "Reaves enters for Russell", "period": {"number": 1},
                "clock": {"displayValue": "8:30"},
                "homeScore": "3", "awayScore": "3",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:06:30Z",
                "team": {"id": "13"},
                "participants": [
                    {"athlete": {"id": "4395725"}},
                    {"athlete": {"id": "4066354"}},
                ],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], SubstitutionEvent)
        assert events[0].player_in_id == "4395725"
        assert events[0].player_out_id == "4066354"
        assert events[0].team_id == "13"

    def test_substitution_no_participants(self):
        plays = [
            {
                "id": "8", "sequenceNumber": 8,
                "type": {"id": "584", "text": "Substitution"},
                "text": "Reaves enters for Russell", "period": {"number": 1},
                "clock": {"displayValue": "8:30"},
                "homeScore": "3", "awayScore": "3",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:06:30Z",
                "team": {"id": "13"},
                "participants": [],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)

        assert len(events) == 1
        assert isinstance(events[0], SubstitutionEvent)
        assert events[0].player_in_id == ""
        assert events[0].player_out_id == ""

    def test_unknown_type_skipped(self):
        plays = [
            {
                "id": "999", "sequenceNumber": 99,
                "type": {"id": "9999", "text": "Unknown Thing"},
                "text": "Something happened", "period": {"number": 1},
                "clock": {"displayValue": "6:00"},
                "homeScore": "3", "awayScore": "3",
                "scoringPlay": False, "scoreValue": 0,
                "wallclock": "2026-03-29T00:07:00Z",
                "team": {"id": "0"},
                "participants": [],
            }
        ]

        proc = ESPNPlayByPlayProcessor()
        events = proc.process_plays("401584793", plays, 1000.0)
        assert events == []


# ------------------------------------------------------------------
# Full snapshot processing
# ------------------------------------------------------------------


class TestProcessSnapshot:
    def test_extracts_plays_from_snapshot(self, pbp_payload):
        proc = ESPNPlayByPlayProcessor()
        snapshot = _make_snapshot(pbp_payload)
        events = proc.process_snapshot(snapshot)

        # Fixture has: 2 period events (start+end), 3 scoring plays,
        # 2 fouls, 1 turnover, 1 timeout, 1 substitution = 10 events
        event_types = [type(e).__name__ for e in events]
        assert "ScoringPlayEvent" in event_types
        assert "FoulEvent" in event_types
        assert "TimeoutEvent" in event_types
        assert "TurnoverEvent" in event_types
        assert "PeriodEvent" in event_types
        assert "SubstitutionEvent" in event_types

    def test_events_ordered_by_sequence(self, pbp_payload):
        proc = ESPNPlayByPlayProcessor()
        snapshot = _make_snapshot(pbp_payload)
        events = proc.process_snapshot(snapshot)

        sequences = [e.sequence for e in events]
        assert sequences == sorted(sequences)

    def test_game_id_set_from_snapshot(self, pbp_payload):
        proc = ESPNPlayByPlayProcessor()
        snapshot = _make_snapshot(pbp_payload)
        events = proc.process_snapshot(snapshot)

        for event in events:
            assert event.game_id == "401584793"

    def test_empty_plays_returns_empty(self):
        payload = _payload_with_plays([])
        proc = ESPNPlayByPlayProcessor()
        events = proc.process_snapshot(_make_snapshot(payload))
        assert events == []
