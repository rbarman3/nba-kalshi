"""Tests for pipeline.processor — lineup diffing and score change detection."""
import pytest
from src.pipeline.models import RawSnapshot, LineupChangeEvent, ScoreChangeEvent
from src.pipeline.processor import extract_lineup, diff_lineups, extract_scores, diff_scores


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
