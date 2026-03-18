from io import StringIO
from unittest.mock import patch

import pytest
import typer
from rich.console import Console
from rich.table import Table

from nba.cli import _pick_game, _render_lineup_table
from nba.models import GameSummary, PlayerOnCourt


def _make_player(name: str, num: str = "1", pos: str = "G", pts: int = 0, ast: int = 0, reb: int = 0) -> PlayerOnCourt:
    return PlayerOnCourt(name=name, jersey_num=num, position=pos, points=pts, assists=ast, rebounds=reb)


def _render_to_str(table: Table) -> str:
    buf = StringIO()
    c = Console(file=buf, width=200)
    c.print(table)
    return buf.getvalue()


class TestRenderLineupTable:
    def test_returns_rich_table(self) -> None:
        home = [_make_player("Alice")]
        away = [_make_player("Bob")]
        result = _render_lineup_table("0022301234", home, away)
        assert isinstance(result, Table)

    def test_includes_player_names(self) -> None:
        home = [_make_player("LeBron James", "23", "F", 10, 5, 8)]
        away = [_make_player("Stephen Curry", "30", "G", 15, 6, 3)]
        table = _render_lineup_table("0022301234", home, away)
        rendered = _render_to_str(table)
        assert "LeBron James" in rendered
        assert "Stephen Curry" in rendered

    def test_handles_unequal_team_sizes(self) -> None:
        home = [_make_player(f"Home{i}") for i in range(5)]
        away = [_make_player(f"Away{i}") for i in range(3)]
        # Should not raise IndexError
        table = _render_lineup_table("0022301234", home, away)
        rendered = _render_to_str(table)
        assert "Home4" in rendered
        assert "Away2" in rendered


def _make_game(game_id: str, away: str = "Away", home: str = "Home") -> GameSummary:
    return GameSummary(
        game_id=game_id,
        home_team=home,
        away_team=away,
        home_score=0,
        away_score=0,
        status="Scheduled",
        period=0,
        clock="",
    )


class TestPickGame:
    def test_pick_game_returns_correct_game_id(self) -> None:
        games = [_make_game("GAME1", "TeamA", "TeamB"), _make_game("GAME2", "TeamC", "TeamD")]
        with patch("nba.cli.typer.prompt", return_value=2):
            result = _pick_game(games)
        assert result == "GAME2"

    def test_pick_game_exits_on_invalid_choice(self) -> None:
        import click

        games = [_make_game("GAME1"), _make_game("GAME2")]
        with patch("nba.cli.typer.prompt", return_value=99):
            with pytest.raises((SystemExit, click.exceptions.Exit)):
                _pick_game(games)
