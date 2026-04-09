"""
Typer-based command-line interface for NBA live scores.

Commands
--------
scores               -- print today's scoreboard table

Install as ``nba-scores`` via the script entry-point in pyproject.toml.
"""
import typer
from rich.console import Console
from rich.table import Table

from nba.live_service import get_live_scoreboard

app = typer.Typer(help="NBA live scores CLI")
console = Console()


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
