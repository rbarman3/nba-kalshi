"""Integration tests for the CLI layer using typer.testing.CliRunner.

Service functions are patched at their import site in cli.py to avoid
any network calls. Rich ANSI output is stripped; tests use string containment.
"""
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from nba.cli import app
from nba.models import GameSummary

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


# ---------------------------------------------------------------------------
# scores command
# ---------------------------------------------------------------------------

class TestScoresCommand:
    def test_renders_team_names(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL]):
            result = runner.invoke(app, [])
        assert result.exit_code == 0
        assert "Los Angeles Lakers" in result.output
        assert "Boston Celtics" in result.output

    def test_renders_scores(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL]):
            result = runner.invoke(app, [])
        assert "110" in result.output
        assert "105" in result.output

    def test_renders_status(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_LIVE]):
            result = runner.invoke(app, [])
        assert "Q3 5:32" in result.output

    def test_no_games_prints_notice(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[]):
            result = runner.invoke(app, [])
        assert "No games scheduled today" in result.output

    def test_multiple_games_all_rendered(self):
        with patch("nba.cli.get_live_scoreboard", return_value=[GAME_FINAL, GAME_LIVE]):
            result = runner.invoke(app, [])
        assert "Los Angeles Lakers" in result.output
        assert "Golden State Warriors" in result.output
