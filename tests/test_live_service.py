from unittest.mock import MagicMock, patch

import pytest

from nba.live_service import get_live_lineup, get_live_scoreboard
from nba.models import GameSummary, PlayerOnCourt

SAMPLE_GAME = {
    "gameId": "0022300001",
    "homeTeam": {"teamCity": "Los Angeles", "teamName": "Lakers", "score": 110},
    "awayTeam": {"teamCity": "Boston", "teamName": "Celtics", "score": 105},
    "gameStatusText": "Final",
    "period": 4,
    "gameClock": "",
}

SAMPLE_GAME_IN_PROGRESS = {
    "gameId": "0022300002",
    "homeTeam": {"teamCity": "Golden State", "teamName": "Warriors", "score": 58},
    "awayTeam": {"teamCity": "Miami", "teamName": "Heat", "score": 55},
    "gameStatusText": "Q3 5:32",
    "period": 3,
    "gameClock": "PT05M32.00S",
}

SAMPLE_GAME_NO_SCORE = {
    "gameId": "0022300003",
    "homeTeam": {"teamCity": "Chicago", "teamName": "Bulls"},
    "awayTeam": {"teamCity": "Denver", "teamName": "Nuggets"},
    "gameStatusText": "7:30 pm ET",
    "period": 0,
    "gameClock": "",
}


def _make_board(games: list[dict]) -> MagicMock:
    board = MagicMock()
    board.games.get_dict.return_value = games
    return board


class TestGetLiveScoreboard:
    def test_returns_list_of_game_summaries(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([SAMPLE_GAME])
            result = get_live_scoreboard()
        assert isinstance(result, list)
        assert all(isinstance(g, GameSummary) for g in result)

    def test_fields_mapped_correctly(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([SAMPLE_GAME])
            result = get_live_scoreboard()
        g = result[0]
        assert g.game_id == "0022300001"
        assert g.home_team == "Los Angeles Lakers"
        assert g.away_team == "Boston Celtics"
        assert g.home_score == 110
        assert g.away_score == 105
        assert g.status == "Final"
        assert g.period == 4
        assert g.clock == ""

    def test_in_progress_fields(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([SAMPLE_GAME_IN_PROGRESS])
            result = get_live_scoreboard()
        g = result[0]
        assert g.status == "Q3 5:32"
        assert g.period == 3
        assert g.clock == "PT05M32.00S"

    def test_empty_games_returns_empty_list(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([])
            result = get_live_scoreboard()
        assert result == []

    def test_missing_score_defaults_to_zero(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([SAMPLE_GAME_NO_SCORE])
            result = get_live_scoreboard()
        g = result[0]
        assert g.home_score == 0
        assert g.away_score == 0

    def test_multiple_games_returned(self):
        with patch("nba.live_service.live_scoreboard.ScoreBoard") as mock_sb:
            mock_sb.return_value = _make_board([SAMPLE_GAME, SAMPLE_GAME_IN_PROGRESS])
            result = get_live_scoreboard()
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Helpers for get_live_lineup tests
# ---------------------------------------------------------------------------

def _make_player(name: str, jersey: str, position: str, oncourt: str,
                 pts: int = 0, ast: int = 0, reb: int = 0) -> dict:
    return {
        "name": name,
        "jerseyNum": jersey,
        "position": position,
        "oncourt": oncourt,
        "statistics": {"points": pts, "assists": ast, "reboundsTotal": reb},
    }


def _make_boxscore(home_players: list[dict], away_players: list[dict]) -> MagicMock:
    box = MagicMock()
    box.home_team_player_stats.get_dict.return_value = home_players
    box.away_team_player_stats.get_dict.return_value = away_players
    return box


class TestGetLiveLineup:
    def test_returns_only_oncourt_players(self):
        home = [
            _make_player("LeBron James", "23", "F", "1"),
            _make_player("Anthony Davis", "3", "C", "0"),
        ]
        away = [
            _make_player("Jayson Tatum", "0", "F", "1"),
            _make_player("Al Horford", "42", "C", "0"),
        ]
        with patch("nba.live_service.live_boxscore.BoxScore") as mock_bs:
            mock_bs.return_value = _make_boxscore(home, away)
            home_result, away_result = get_live_lineup("0022300001")

        assert len(home_result) == 1
        assert home_result[0].name == "LeBron James"
        assert len(away_result) == 1
        assert away_result[0].name == "Jayson Tatum"

    def test_fields_mapped_correctly(self):
        home = [_make_player("LeBron James", "23", "F", "1", pts=25, ast=7, reb=8)]
        away = [_make_player("Jayson Tatum", "0", "SF", "1", pts=30, ast=4, reb=9)]
        with patch("nba.live_service.live_boxscore.BoxScore") as mock_bs:
            mock_bs.return_value = _make_boxscore(home, away)
            home_result, away_result = get_live_lineup("0022300001")

        p = home_result[0]
        assert isinstance(p, PlayerOnCourt)
        assert p.name == "LeBron James"
        assert p.jersey_num == "23"
        assert p.position == "F"
        assert p.points == 25
        assert p.assists == 7
        assert p.rebounds == 8

    def test_empty_when_game_not_started(self):
        home = [_make_player("LeBron James", "23", "F", "0")]
        away = [_make_player("Jayson Tatum", "0", "F", "0")]
        with patch("nba.live_service.live_boxscore.BoxScore") as mock_bs:
            mock_bs.return_value = _make_boxscore(home, away)
            home_result, away_result = get_live_lineup("0022300001")

        assert home_result == []
        assert away_result == []
