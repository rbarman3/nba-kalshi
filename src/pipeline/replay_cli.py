"""CLI for replaying stored snapshots through the pipeline.

Usage:
    nba-replay 2026-03-29
    nba-replay 2026-03-29 --game 0022500001
    nba-replay 2026-03-29 --mode realtime
    nba-replay 2026-03-29 --store-dir data/snapshots
"""
import asyncio
import logging

import typer
from rich.console import Console
from rich.table import Table

from pipeline.replayer import SnapshotReplayer, ReplayResult
from pipeline.store import SnapshotStore

app = typer.Typer(help="Replay stored NBA snapshots for backtesting.")
console = Console()
logger = logging.getLogger(__name__)


def _print_result(result: ReplayResult) -> None:
    """Print a single ReplayResult to the console."""
    lineup_events = [e for e in result.events if hasattr(e, "players_in")]
    score_events = [e for e in result.events if hasattr(e, "home_score")]

    console.print(
        f"  [bold]{result.game_id}[/bold]  "
        f"snapshots={result.snapshot_count}  "
        f"lineup_changes={len(lineup_events)}  "
        f"score_changes={len(score_events)}  "
        f"time={result.duration_seconds:.3f}s"
    )

    for event in result.events:
        if hasattr(event, "players_in"):
            console.print(
                f"    [cyan]LINEUP[/cyan] P{event.period} {event.clock}  "
                f"in={set(event.players_in)}  out={set(event.players_out)}"
            )
        elif hasattr(event, "home_score"):
            console.print(
                f"    [yellow]SCORE[/yellow]  P{event.period} {event.clock}  "
                f"{event.home_prev}-{event.away_prev} → {event.home_score}-{event.away_score}"
            )


def _print_summary(results: list[ReplayResult]) -> None:
    """Print a summary table for all replayed games."""
    table = Table(title="Replay Summary")
    table.add_column("Game ID", style="bold")
    table.add_column("Snapshots", justify="right")
    table.add_column("Events", justify="right")
    table.add_column("Time", justify="right")

    total_snaps = 0
    total_events = 0

    for r in results:
        total_snaps += r.snapshot_count
        total_events += len(r.events)
        table.add_row(
            r.game_id,
            str(r.snapshot_count),
            str(len(r.events)),
            f"{r.duration_seconds:.3f}s",
        )

    console.print()
    console.print(table)
    console.print(
        f"\n[bold]Total:[/bold] {len(results)} game(s), "
        f"{total_snaps} snapshots, {total_events} events"
    )


@app.command()
def replay(
    date: str = typer.Argument(help="Date to replay (YYYY-MM-DD)."),
    game: str = typer.Option(None, "--game", "-g", help="Replay a single game ID."),
    mode: str = typer.Option("fast", "--mode", "-m", help="Replay mode: fast or realtime."),
    store_dir: str = typer.Option("data/snapshots", "--store-dir", "-d", help="Snapshot store directory."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Show individual events."),
) -> None:
    """Replay stored snapshots for a given date."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    store = SnapshotStore(base_dir=store_dir)
    replayer = SnapshotReplayer(store=store, mode=mode)

    if game:
        console.print(f"Replaying game [bold]{game}[/bold] on {date} ({mode} mode)")
        result = asyncio.run(replayer.replay_game(game, date))
        if result.snapshot_count == 0:
            console.print(f"[red]No snapshots found for {game} on {date}[/red]")
            raise typer.Exit(code=1)
        if verbose:
            _print_result(result)
        _print_summary([result])
    else:
        available = store.list_games(date)
        if not available:
            console.print(f"[red]No games found for {date} in {store_dir}[/red]")
            raise typer.Exit(code=1)

        console.print(f"Replaying {len(available)} game(s) on {date} ({mode} mode)\n")
        results = asyncio.run(replayer.replay_date(date))

        if verbose:
            for result in results:
                _print_result(result)

        _print_summary(results)


if __name__ == "__main__":
    app()
