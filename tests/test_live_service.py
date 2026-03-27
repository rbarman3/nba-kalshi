from unittest.mock import MagicMock, patch

import pytest

from nba.live_service import get_live_scoreboard
from nba.models import GameSummary

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
