"""
Typer-based command-line interface for NBA live scores.

Commands
--------
scores               -- print today's scoreboard table
lineup <game_id>     -- print the live on-court lineup for one game
watch [game_id]      -- continuously refresh the lineup (Ctrl+C to stop)

Install as ``nba-scores`` via the script entry-point in pyproject.toml.
"""
import threading
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from nba.live_service import get_live_lineup, get_live_scoreboard
from nba.models import GameSummary, PlayerOnCourt
from nba.poller import Poller

app = typer.Typer(help="NBA live scores CLI")
console = Console()


def _render_lineup_table(
    game_id: str,
    home_players: list[PlayerOnCourt],
    away_players: list[PlayerOnCourt],
) -> Table:
    """Build a Rich Table showing side-by-side away/home on-court players.

    Parameters
    ----------
    game_id:
        NBA game identifier shown in the table title.
    home_players:
        Players currently on court for the home team.
    away_players:
        Players currently on court for the away team.

    Returns
    -------
    Table
        A Rich Table ready to be printed via ``console.print``.
    """
    table = Table(title=f"Live Lineup — Game {game_id}", show_lines=True)
    table.add_column("Away", style="cyan", min_width=20)
    table.add_column("#", justify="center", min_width=4)
    table.add_column("Pos", justify="center", min_width=5)
    table.add_column("PTS", justify="center", min_width=5)
    table.add_column("AST", justify="center", min_width=5)
    table.add_column("REB", justify="center", min_width=5)
    table.add_column("Home", style="green", min_width=20)
    table.add_column("#", justify="center", min_width=4)
    table.add_column("Pos", justify="center", min_width=5)
    table.add_column("PTS", justify="center", min_width=5)
    table.add_column("AST", justify="center", min_width=5)
    table.add_column("REB", justify="center", min_width=5)

    rows = max(len(away_players), len(home_players))
    for i in range(rows):
        a = away_players[i] if i < len(away_players) else None
        h = home_players[i] if i < len(home_players) else None
        table.add_row(
            a.name if a else "",
            a.jersey_num if a else "",
            a.position if a else "",
            str(a.points) if a else "",
            str(a.assists) if a else "",
            str(a.rebounds) if a else "",
            h.name if h else "",
            h.jersey_num if h else "",
            h.position if h else "",
            str(h.points) if h else "",
            str(h.assists) if h else "",
            str(h.rebounds) if h else "",
        )

    return table


def _pick_game(games: list[GameSummary]) -> str:
    """Print a numbered game list, prompt the user, and return the selected game_id.

    Parameters
    ----------
    games:
        Non-empty list of today's games. Caller is responsible for ensuring this is not empty.

    Returns
    -------
    str
        The ``game_id`` of the user's chosen game.

    Raises
    ------
    typer.Exit
        If the user enters a number outside the valid range.
    """
    for i, g in enumerate(games, 1):
        console.print(f"  [bold]{i}.[/bold] {g.away_team} @ {g.home_team}  ({g.status})")
    idx = typer.prompt("Select a game", type=int)
    if not 1 <= idx <= len(games):
        console.print(f"[red]Invalid choice. Enter 1–{len(games)}.[/red]")
        raise typer.Exit(1)
    return games[idx - 1].game_id


@app.command()
def scores() -> None:
    """Print today's live NBA scoreboard."""
    with console.status("Fetching live scores...", spinner="dots"):
        games = get_live_scoreboard()

    if not games:
        console.print("[yellow]No games scheduled today.[/yellow]")
        raise typer.Exit()

    table = Table(title="NBA Live Scoreboard", show_lines=True)
    table.add_column("Away", style="cyan", min_width=22)
    table.add_column("Score", justify="center", style="bold white", min_width=7)
    table.add_column("Home", style="green", min_width=22)
    table.add_column("Status", justify="center", min_width=14)
    table.add_column("Period", justify="center", min_width=6)
    table.add_column("Clock", justify="center", min_width=10)

    for g in games:
        table.add_row(
            g.away_team,
            f"{g.away_score} - {g.home_score}",
            g.home_team,
            g.status,
            str(g.period) if g.period else "-",
            g.clock or "-",
        )

    console.print(table)


@app.command()
def lineup(game_id: str = typer.Argument(..., help="Game ID from 'nba-scores scores'")) -> None:
    """Print the players currently on the court for a live game."""
    with console.status("Fetching live lineup...", spinner="dots"):
        home_players, away_players = get_live_lineup(game_id)

    if not home_players and not away_players:
        console.print("[yellow]No players on court — game may not be live yet.[/yellow]")
        raise typer.Exit()

    console.print(_render_lineup_table(game_id, home_players, away_players))


@app.command()
def watch(
    game_id: Optional[str] = typer.Argument(None, help="Game ID (omit to pick interactively)"),
    interval: int = typer.Option(30, help="Seconds between refreshes"),
) -> None:
    """Continuously refresh the live lineup for a game (Ctrl+C to stop)."""
    if game_id is None:
        with console.status("Fetching games...", spinner="dots"):
            games = get_live_scoreboard()
        if not games:
            console.print("[yellow]No games scheduled today.[/yellow]")
            raise typer.Exit()
        game_id = _pick_game(games)

    def _callback(result: tuple) -> None:
        home, away = result
        console.clear()
        if not home and not away:
            console.print("[yellow]No players on court — game may not be live yet.[/yellow]")
        else:
            console.print(_render_lineup_table(game_id, home, away))

    poller = Poller(
        fetch_fn=lambda: get_live_lineup(game_id),
        callback=_callback,
        interval=interval,
    )

    console.print(f"[dim]Watching game {game_id} every {interval}s — Ctrl+C to stop[/dim]")
    poller.start()
    try:
        while poller.is_running():
            threading.Event().wait(timeout=1)
    except KeyboardInterrupt:
        pass
    finally:
        poller.stop()
        console.print("[dim]Stopped.[/dim]")
