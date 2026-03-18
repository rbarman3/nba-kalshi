"""Integration tests for the CLI layer using typer.testing.CliRunner.

Service functions are patched at their import site in cli.py to avoid
any network calls. Rich ANSI output is stripped; tests use string containment.
"""
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from nba.cli import app
from nba.models import GameSummary, PlayerOnCourt

runner = CliRunner()

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

GAME_FINAL = GameSummary(
    game_id="0022301234",
    home_team="Los Angeles Lakers",
    away_team="Boston Celtics",
    home_score=110,
    away_score=105,
    status="Final",
    period=4,
    clock="",
)
GAME_LIVE = GameSummary(
    game_id="0022301235",
    home_team="Miami Heat",
    away_team="Golden State Warriors",
    home_score=55,
    away_score=58,
    status="Q3 5:32",
    period=3,
    clock="PT05M32.00S",
)
HOME_PLAYERS = [PlayerOnCourt("LeBron James", "23", "F", 25, 7, 8)]
AWAY_PLAYERS = [PlayerOnCourt("Jayson Tatum", "0", "SF", 30, 4, 9)]


# ---------------------------------------------------------------------------
# scores command
# ---------------------------------------------------------------------------

class TestScoresCommand:
    def test_renders_team_names(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL]):
            result = runner.invoke(app, ["scores"])
        assert result.exit_code == 0
        assert "Los Angeles Lakers" in result.output
        assert "Boston Celtics" in result.output

    def test_renders_scores(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL]):
            result = runner.invoke(app, ["scores"])
        assert "110" in result.output
        assert "105" in result.output

    def test_renders_status(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_LIVE]):
            result = runner.invoke(app, ["scores"])
        assert "Q3 5:32" in result.output

    def test_no_games_prints_notice(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[]):
            result = runner.invoke(app, ["scores"])
        assert "No games scheduled today" in result.output

    def test_multiple_games_all_rendered(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL, GAME_LIVE]):
            result = runner.invoke(app, ["scores"])
        assert "Los Angeles Lakers" in result.output
        assert "Golden State Warriors" in result.output


# ---------------------------------------------------------------------------
# lineup command
# ---------------------------------------------------------------------------

class TestLineupCommand:
    def test_renders_player_names(self):
        with patch("nba.cli.get_live_lineup", return_value=(HOME_PLAYERS, AWAY_PLAYERS)):
            result = runner.invoke(app, ["lineup", "0022301234"])
        assert "LeBron James" in result.output
        assert "Jayson Tatum" in result.output

    def test_renders_game_id_in_title(self):
        with patch("nba.cli.get_live_lineup", return_value=(HOME_PLAYERS, AWAY_PLAYERS)):
            result = runner.invoke(app, ["lineup", "0022301234"])
        assert "0022301234" in result.output

    def test_no_players_prints_notice(self):
        with patch("nba.cli.get_live_lineup", return_value=([], [])):
            result = runner.invoke(app, ["lineup", "0022301234"])
        assert "No players on court" in result.output

    def test_renders_player_stats(self):
        with patch("nba.cli.get_live_lineup", return_value=(HOME_PLAYERS, AWAY_PLAYERS)):
            result = runner.invoke(app, ["lineup", "0022301234"])
        # Away team columns appear first (left side), so their stats are always visible
        assert "Jayson Tatum" in result.output
        assert "30" in result.output  # Tatum's points in the away (left) columns


# ---------------------------------------------------------------------------
# watch command
# ---------------------------------------------------------------------------

class TestWatchCommand:
    def _make_poller_mock(self, is_running_side_effect=None):
        mock_instance = MagicMock()
        if is_running_side_effect is None:
            is_running_side_effect = [True, False]
        mock_instance.is_running.side_effect = is_running_side_effect
        return mock_instance

    def test_explicit_game_id_starts_poller(self):
        mock_instance = self._make_poller_mock()
        with patch("nba.cli.Poller", return_value=mock_instance):
            result = runner.invoke(app, ["watch", "0022301234"])
        mock_instance.start.assert_called_once()
        mock_instance.stop.assert_called_once()
        assert "0022301234" in result.output
        assert result.exit_code == 0

    def test_no_game_id_fetches_scoreboard_and_picks(self):
        mock_instance = self._make_poller_mock()
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL]):
            with patch("nba.cli.Poller", return_value=mock_instance):
                with patch("typer.prompt", return_value=1):
                    result = runner.invoke(app, ["watch"])
        assert result.exit_code == 0

    def test_no_game_id_no_games_exits_cleanly(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[]):
            result = runner.invoke(app, ["watch"])
        assert "No games scheduled today" in result.output

    def test_custom_interval_passed_to_poller(self):
        mock_instance = self._make_poller_mock()
        captured_kwargs = {}

        def capture_poller(*args, **kwargs):
            captured_kwargs.update(kwargs)
            return mock_instance

        with patch("nba.cli.Poller", side_effect=capture_poller):
            runner.invoke(app, ["watch", "0022301234", "--interval", "10"])

        assert captured_kwargs.get("interval") == 10

    def test_stop_called_in_finally(self):
        mock_instance = self._make_poller_mock(is_running_side_effect=[False])
        with patch("nba.cli.Poller", return_value=mock_instance):
            runner.invoke(app, ["watch", "0022301234"])
        mock_instance.stop.assert_called_once()
