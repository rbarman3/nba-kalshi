"""Tests for pipeline.processor — lineup diffing, score detection, and action parsing."""
import pytest
from src.pipeline.models import (
    RawSnapshot, LineupChangeEvent, ScoreChangeEvent,
    ScoringPlayEvent, FoulEvent, TimeoutEvent,
    TurnoverEvent, PeriodEvent, SubstitutionEvent,
)
from src.pipeline.processor import (
    extract_lineup, diff_lineups, extract_scores, diff_scores,
    extract_new_actions, classify_action,
)


@pytest.fixture
def base_payload():
    """Fixture: minimal valid boxscore payload."""
    return {
        "game": {
            "gameId": "0022500001",
            "period": 1,
            "gameClock": "PT05M32.00S",
            "homeTeam": {
                "score": 0,
                "players": [
                    {"personId": "201939", "oncourt": "1"},  # on court
                    {"personId": "2544", "oncourt": "0"},    # on bench
                    {"personId": "203078", "oncourt": "1"},  # on court
                ]
            },
            "awayTeam": {
                "score": 0,
                "players": [
                    {"personId": "2544", "oncourt": "1"},    # on court
                    {"personId": "201950", "oncourt": "1"},  # on court
                    {"personId": "101107", "oncourt": "0"},  # on bench
                ]
            },
        }
    }


@pytest.fixture
def snapshot(base_payload):
    """Fixture: RawSnapshot with base payload."""
    return RawSnapshot(
        game_id="0022500001",
        payload=base_payload,
        fetched_at=1000.0,
    )


class TestExtractLineup:
    """Tests for extract_lineup()."""

    def test_extract_lineup_returns_dict_with_home_away(self, snapshot):
        """extract_lineup returns dict with 'home' and 'away' keys."""
        result = extract_lineup(snapshot)
        assert isinstance(result, dict)
        assert "home" in result
        assert "away" in result

    def test_extract_lineup_identifies_oncourt_players(self, snapshot):
        """extract_lineup finds all players with oncourt='1'."""
        result = extract_lineup(snapshot)
        assert result["home"] == frozenset(["201939", "203078"])
        assert result["away"] == frozenset(["2544", "201950"])

    def test_extract_lineup_ignores_bench_players(self, snapshot):
        """extract_lineup ignores players with oncourt='0'."""
        result = extract_lineup(snapshot)
        assert "101107" not in result["away"]  # Bench player not included

    def test_extract_lineup_handles_missing_oncourt_field(self, base_payload):
        """extract_lineup treats missing oncourt field as '0'."""
        # Remove oncourt from one player
        base_payload["game"]["homeTeam"]["players"][0].pop("oncourt", None)
        snapshot = RawSnapshot(
            game_id="0022500001",
            payload=base_payload,
            fetched_at=1000.0,
        )
        result = extract_lineup(snapshot)
        assert "201939" not in result["home"]

    def test_extract_lineup_handles_empty_teams(self):
        """extract_lineup handles teams with no players."""
        payload = {
            "game": {
                "homeTeam": {"players": []},
                "awayTeam": {"players": []},
            }
        }
        snapshot = RawSnapshot(
            game_id="0022500001",
            payload=payload,
            fetched_at=1000.0,
        )
        result = extract_lineup(snapshot)
        assert result["home"] == frozenset()
        assert result["away"] == frozenset()


class TestDiffLineups:
    """Tests for diff_lineups()."""

    def test_no_change_returns_none(self, snapshot):
        """diff_lineups returns None when lineup unchanged."""
        lineup = extract_lineup(snapshot)
        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=lineup,
            curr_lineup=lineup,
            snapshot=snapshot,
        )
        assert event is None

    def test_player_entering_court(self, base_payload, snapshot):
        """diff_lineups detects player entering the court."""
        prev_lineup = extract_lineup(snapshot)

        # Player 2544 (was on away bench) now on court
        base_payload["game"]["awayTeam"]["players"][2]["oncourt"] = "1"
        curr_lineup = extract_lineup(snapshot)

        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=prev_lineup,
            curr_lineup=curr_lineup,
            snapshot=snapshot,
        )
        assert event is not None
        assert "101107" in event.players_in
        assert not event.players_out

    def test_player_leaving_court(self, base_payload, snapshot):
        """diff_lineups detects player leaving the court."""
        prev_lineup = extract_lineup(snapshot)

        # Player 201939 (was on home court) now on bench
        base_payload["game"]["homeTeam"]["players"][0]["oncourt"] = "0"
        curr_lineup = extract_lineup(snapshot)

        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=prev_lineup,
            curr_lineup=curr_lineup,
            snapshot=snapshot,
        )
        assert event is not None
        assert not event.players_in
        assert "201939" in event.players_out

    def test_simultaneous_in_out(self, base_payload):
        """diff_lineups handles simultaneous substitutions."""
        # Create snapshot with initial lineup
        snapshot1 = RawSnapshot(
            game_id="0022500001",
            payload=base_payload,
            fetched_at=1000.0,
        )
        prev_lineup = extract_lineup(snapshot1)

        # Modify payload: Player 201939 out, player 999999 (bench) in
        payload2 = {
            "game": {
                "period": 1,
                "gameClock": "PT05M32.00S",
                "homeTeam": {
                    "players": [
                        {"personId": "201939", "oncourt": "0"},  # now on bench
                        {"personId": "999999", "oncourt": "1"},  # now on court
                        {"personId": "203078", "oncourt": "1"},  # still on court
                    ]
                },
                "awayTeam": base_payload["game"]["awayTeam"],
            }
        }
        snapshot2 = RawSnapshot(
            game_id="0022500001",
            payload=payload2,
            fetched_at=1001.0,
        )
        curr_lineup = extract_lineup(snapshot2)

        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=prev_lineup,
            curr_lineup=curr_lineup,
            snapshot=snapshot2,
        )
        assert event is not None
        assert "999999" in event.players_in
        assert "201939" in event.players_out

    def test_event_contains_correct_metadata(self, base_payload, snapshot):
        """diff_lineups includes period, clock, timestamp in event."""
        prev_lineup = extract_lineup(snapshot)
        base_payload["game"]["homeTeam"]["players"][0]["oncourt"] = "0"
        curr_lineup = extract_lineup(snapshot)

        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=prev_lineup,
            curr_lineup=curr_lineup,
            snapshot=snapshot,
        )
        assert event.game_id == "0022500001"
        assert event.period == 1
        assert event.clock == "PT05M32.00S"
        assert event.observed_at == 1000.0

    def test_event_is_frozen(self, base_payload, snapshot):
        """LineupChangeEvent is immutable."""
        prev_lineup = extract_lineup(snapshot)
        base_payload["game"]["homeTeam"]["players"][0]["oncourt"] = "0"
        curr_lineup = extract_lineup(snapshot)

        event = diff_lineups(
            game_id="0022500001",
            prev_lineup=prev_lineup,
            curr_lineup=curr_lineup,
            snapshot=snapshot,
        )
        with pytest.raises(AttributeError):
            event.period = 2  # type: ignore


class TestExtractScores:
    """Tests for extract_scores()."""

    def test_returns_dict_with_home_away(self, snapshot):
        """extract_scores returns dict with 'home' and 'away' keys."""
        result = extract_scores(snapshot)
        assert isinstance(result, dict)
        assert "home" in result
        assert "away" in result

    def test_extracts_scores_correctly(self, base_payload, snapshot):
        """extract_scores reads homeTeam.score and awayTeam.score."""
        base_payload["game"]["homeTeam"]["score"] = 55
        base_payload["game"]["awayTeam"]["score"] = 48
        result = extract_scores(snapshot)
        assert result["home"] == 55
        assert result["away"] == 48

    def test_defaults_to_zero_when_missing(self):
        """extract_scores defaults to 0 when score field missing."""
        payload = {
            "game": {
                "homeTeam": {"players": []},
                "awayTeam": {"players": []},
            }
        }
        snapshot = RawSnapshot(
            game_id="0022500001",
            payload=payload,
            fetched_at=1000.0,
        )
        result = extract_scores(snapshot)
        assert result["home"] == 0
        assert result["away"] == 0


class TestDiffScores:
    """Tests for diff_scores()."""

    def test_no_change_returns_none(self, snapshot):
        """diff_scores returns None when scores unchanged."""
        scores = extract_scores(snapshot)
        event = diff_scores(
            game_id="0022500001",
            prev_scores=scores,
            curr_scores=scores,
            snapshot=snapshot,
        )
        assert event is None

    def test_home_score_change(self, base_payload, snapshot):
        """diff_scores detects home score change."""
        prev_scores = {"home": 50, "away": 48}
        base_payload["game"]["homeTeam"]["score"] = 55
        base_payload["game"]["awayTeam"]["score"] = 48
        curr_scores = extract_scores(snapshot)

        event = diff_scores(
            game_id="0022500001",
            prev_scores=prev_scores,
            curr_scores=curr_scores,
            snapshot=snapshot,
        )
        assert event is not None
        assert event.home_score == 55
        assert event.home_prev == 50
        assert event.away_score == 48
        assert event.away_prev == 48

    def test_away_score_change(self, base_payload, snapshot):
        """diff_scores detects away score change."""
        prev_scores = {"home": 50, "away": 48}
        base_payload["game"]["homeTeam"]["score"] = 50
        base_payload["game"]["awayTeam"]["score"] = 52
        curr_scores = extract_scores(snapshot)

        event = diff_scores(
            game_id="0022500001",
            prev_scores=prev_scores,
            curr_scores=curr_scores,
            snapshot=snapshot,
        )
        assert event is not None
        assert event.away_score == 52
        assert event.away_prev == 48

    def test_both_scores_change(self, base_payload, snapshot):
        """diff_scores detects both scores changing."""
        prev_scores = {"home": 50, "away": 48}
        base_payload["game"]["homeTeam"]["score"] = 52
        base_payload["game"]["awayTeam"]["score"] = 50
        curr_scores = extract_scores(snapshot)

        event = diff_scores(
            game_id="0022500001",
            prev_scores=prev_scores,
            curr_scores=curr_scores,
            snapshot=snapshot,
        )
        assert event is not None
        assert event.home_score == 52
        assert event.away_score == 50

    def test_event_is_frozen(self, base_payload, snapshot):
        """ScoreChangeEvent is immutable."""
        prev_scores = {"home": 50, "away": 48}
        base_payload["game"]["homeTeam"]["score"] = 55
        curr_scores = extract_scores(snapshot)

        event = diff_scores(
            game_id="0022500001",
            prev_scores=prev_scores,
            curr_scores=curr_scores,
            snapshot=snapshot,
        )
        with pytest.raises(AttributeError):
            event.home_score = 60  # type: ignore

    def test_event_contains_correct_metadata(self, base_payload, snapshot):
        """diff_scores includes period, clock, game_id, timestamp."""
        prev_scores = {"home": 50, "away": 48}
        base_payload["game"]["homeTeam"]["score"] = 55
        curr_scores = extract_scores(snapshot)

        event = diff_scores(
            game_id="0022500001",
            prev_scores=prev_scores,
            curr_scores=curr_scores,
            snapshot=snapshot,
        )
        assert event.game_id == "0022500001"
        assert event.period == 1
        assert event.clock == "PT05M32.00S"
        assert event.observed_at == 1000.0


# ---------------------------------------------------------------------------
# Action parsing tests
# ---------------------------------------------------------------------------

def make_action(**kwargs) -> dict:
    """Build a minimal action dict with sensible defaults."""
    defaults = {
        "actionNumber": 1,
        "clock": "PT10M00.00S",
        "period": 1,
        "teamId": 1610612744,
        "personId": 2544,
        "actionType": "2pt",
        "subType": "Driving Layup",
        "shotResult": "Made",
        "scoreHome": "2",
        "scoreAway": "0",
        "pointsTotal": 2,
        "description": "James 5' Driving Layup (2 PTS)",
    }
    defaults.update(kwargs)
    return defaults


def make_snapshot_with_actions(actions: list, game_id: str = "0022500001") -> RawSnapshot:
    """Build a RawSnapshot containing the given actions list."""
    return RawSnapshot(
        game_id=game_id,
        payload={
            "game": {
                "period": 1,
                "gameClock": "PT10M00.00S",
                "homeTeam": {"score": 2, "players": []},
                "awayTeam": {"score": 0, "players": []},
                "actions": actions,
            }
        },
        fetched_at=1000.0,
    )


class TestExtractNewActions:
    """Tests for extract_new_actions()."""

    def test_returns_all_when_last_is_minus_one(self):
        """All actions returned when last_action_number is -1."""
        snapshot = make_snapshot_with_actions([
            make_action(actionNumber=1),
            make_action(actionNumber=2),
        ])
        result = extract_new_actions(snapshot, -1)
        assert len(result) == 2

    def test_filters_already_seen_actions(self):
        """Actions with actionNumber <= last_action_number are excluded."""
        snapshot = make_snapshot_with_actions([
            make_action(actionNumber=1),
            make_action(actionNumber=2),
            make_action(actionNumber=3),
        ])
        result = extract_new_actions(snapshot, 2)
        assert len(result) == 1
        assert result[0]["actionNumber"] == 3

    def test_returns_empty_when_all_seen(self):
        """Returns empty list when no new actions."""
        snapshot = make_snapshot_with_actions([
            make_action(actionNumber=1),
        ])
        result = extract_new_actions(snapshot, 5)
        assert result == []

    def test_returns_sorted_ascending(self):
        """Actions are sorted ascending by actionNumber."""
        snapshot = make_snapshot_with_actions([
            make_action(actionNumber=3),
            make_action(actionNumber=1),
            make_action(actionNumber=2),
        ])
        result = extract_new_actions(snapshot, -1)
        numbers = [a["actionNumber"] for a in result]
        assert numbers == [1, 2, 3]

    def test_no_actions_field_returns_empty(self):
        """Missing game.actions key returns empty list."""
        snapshot = RawSnapshot(
            game_id="0022500001",
            payload={"game": {"period": 1, "gameClock": "PT10M00.00S",
                              "homeTeam": {"score": 0, "players": []},
                              "awayTeam": {"score": 0, "players": []}}},
            fetched_at=1000.0,
        )
        result = extract_new_actions(snapshot, -1)
        assert result == []


class TestClassifyAction:
    """Tests for classify_action()."""

    def test_two_point_made_returns_scoring_event(self):
        """2pt Made → ScoringPlayEvent with score_value=2."""
        action = make_action(actionType="2pt", shotResult="Made", pointsTotal=2)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, ScoringPlayEvent)
        assert event.score_value == 2
        assert event.action_type == "2pt"

    def test_three_point_made_returns_scoring_event(self):
        """3pt Made → ScoringPlayEvent with score_value=3."""
        action = make_action(
            actionType="3pt", subType="Jump Shot",
            shotResult="Made", pointsTotal=3,
            scoreHome="5", scoreAway="0",
        )
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, ScoringPlayEvent)
        assert event.score_value == 3

    def test_freethrow_made_returns_scoring_event(self):
        """Free throw Made → ScoringPlayEvent with score_value=1."""
        action = make_action(
            actionType="freethrow", subType="Free Throw 1 of 2",
            shotResult="Made", pointsTotal=1,
        )
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, ScoringPlayEvent)
        assert event.score_value == 1

    def test_missed_shot_returns_none(self):
        """Missed shot (shotResult=Missed) → None."""
        action = make_action(actionType="2pt", shotResult="Missed")
        event = classify_action("0022500001", action, 1000.0)
        assert event is None

    def test_foul_action_returns_foul_event(self):
        """actionType=foul → FoulEvent."""
        action = make_action(actionType="foul", subType="shooting",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, FoulEvent)
        assert event.foul_type == "shooting"

    def test_foul_defaults_to_personal(self):
        """Foul with empty subType defaults to 'personal'."""
        action = make_action(actionType="foul", subType="",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, FoulEvent)
        assert event.foul_type == "personal"

    def test_timeout_action_returns_timeout_event(self):
        """actionType=timeout → TimeoutEvent."""
        action = make_action(actionType="timeout", subType="full",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, TimeoutEvent)
        assert event.timeout_type == "full"

    def test_turnover_action_returns_turnover_event(self):
        """actionType=turnover → TurnoverEvent."""
        action = make_action(actionType="turnover", subType="bad pass",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, TurnoverEvent)
        assert event.turnover_type == "bad pass"

    def test_period_start_returns_period_event(self):
        """actionType=period, subType=start → PeriodEvent(event_type='start')."""
        action = make_action(actionType="period", subType="start",
                             shotResult=None, pointsTotal=None, teamId=None, personId=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, PeriodEvent)
        assert event.event_type == "start"

    def test_period_end_returns_period_event(self):
        """actionType=period, subType=end → PeriodEvent(event_type='end')."""
        action = make_action(actionType="period", subType="end",
                             shotResult=None, pointsTotal=None, teamId=None, personId=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, PeriodEvent)
        assert event.event_type == "end"

    def test_substitution_in_returns_substitution_event(self):
        """actionType=substitution, subType=in → SubstitutionEvent(sub_type='in')."""
        action = make_action(actionType="substitution", subType="in",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, SubstitutionEvent)
        assert event.sub_type == "in"

    def test_substitution_out_returns_substitution_event(self):
        """actionType=substitution, subType=out → SubstitutionEvent(sub_type='out')."""
        action = make_action(actionType="substitution", subType="out",
                             shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert isinstance(event, SubstitutionEvent)
        assert event.sub_type == "out"

    def test_unknown_action_type_returns_none(self):
        """Unrecognized actionType → None."""
        action = make_action(actionType="jumpball", shotResult=None, pointsTotal=None)
        event = classify_action("0022500001", action, 1000.0)
        assert event is None

    def test_event_fields_populated_correctly(self):
        """classify_action maps action fields onto event fields correctly."""
        action = make_action(
            actionNumber=42, period=3, clock="PT05M30.00S",
            scoreHome="80", scoreAway="75",
            teamId=1610612744, personId=2544,
            description="James Driving Layup",
            actionType="2pt", shotResult="Made", pointsTotal=2,
        )
        event = classify_action("0022500001", action, 1234.5)
        assert event.action_number == 42
        assert event.period == 3
        assert event.clock == "PT05M30.00S"
        assert event.home_score == 80
        assert event.away_score == 75
        assert event.team_id == 1610612744
        assert event.player_id == 2544
        assert event.description == "James Driving Layup"
        assert event.observed_at == 1234.5

    def test_all_action_events_are_frozen(self):
        """All action event dataclasses are frozen (immutable)."""
        action = make_action(actionType="2pt", shotResult="Made", pointsTotal=2)
        event = classify_action("0022500001", action, 1000.0)
        with pytest.raises(AttributeError):
            event.period = 2  # type: ignore

    def test_null_score_fields_default_to_zero(self):
        """None scoreHome/scoreAway coerce to 0."""
        action = make_action(
            actionType="period", subType="start",
            scoreHome=None, scoreAway=None,
            shotResult=None, pointsTotal=None, teamId=None, personId=None,
        )
        event = classify_action("0022500001", action, 1000.0)
        assert event.home_score == 0
        assert event.away_score == 0
