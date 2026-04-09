"""CLI for ESPN play-by-play scraping and event extraction.

An independent tool for gathering NBA play-by-play data from ESPN.
Scrapes historical games, classifies plays into typed events, and
stores results for consumption by the backtesting pipeline.

Usage:
    nba-espn scrape 2026-03-29
    nba-espn scrape 2026-03-29 --game 401584793
    nba-espn scrape 2026-03-29 --store-dir data/espn
    nba-espn list-games 2026-03-29
    nba-espn list-dates
    nba-espn show 2026-03-29 401584793
"""
import asyncio
import logging

import typer
from rich.console import Console
from rich.table import Table

from pipeline.espn_id_map import discover_espn_game_ids
from pipeline.espn_processor import ESPNPlayByPlayProcessor
from pipeline.espn_store import ESPNStore
from pipeline.espn_transport import ESPNScraper
from pipeline.models import (
    FoulEvent,
    PeriodEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)

app = typer.Typer(help="ESPN play-by-play scraper for NBA backtesting data.")
console = Console()
logger = logging.getLogger(__name__)


def _event_label(event: object) -> tuple[str, str]:
    """Return (style, label) for an event type."""
    if isinstance(event, ScoringPlayEvent):
        return "yellow", f"SCORE +{event.score_value}"
    if isinstance(event, FoulEvent):
        return "red", "FOUL"
    if isinstance(event, TimeoutEvent):
        return "cyan", "TIMEOUT"
    if isinstance(event, TurnoverEvent):
        return "magenta", "TURNOVER"
    if isinstance(event, PeriodEvent):
        return "green", f"PERIOD {event.event_type.upper()}"
    if isinstance(event, SubstitutionEvent):
        return "blue", "SUBSTITUTION"
    return "white", "UNKNOWN"


@app.command()
def scrape(
    date: str = typer.Argument(help="Date to scrape (YYYY-MM-DD)."),
    game: str = typer.Option(
        None, "--game", "-g", help="Scrape a single ESPN game ID."
    ),
    store_dir: str = typer.Option(
        "data/espn", "--store-dir", "-d", help="Output directory."
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show individual events."
    ),
) -> None:
    """Scrape ESPN play-by-play for a date and store events."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    asyncio.run(_do_scrape(date, game, store_dir, verbose))


async def _do_scrape(
    date: str,
    game: str | None,
    store_dir: str,
    verbose: bool,
) -> None:
    store = ESPNStore(base_dir=store_dir)
    scraper = ESPNScraper()
    processor = ESPNPlayByPlayProcessor()

    if game:
        espn_game_ids = [game]
    else:
        console.print(f"Discovering ESPN games for [bold]{date}[/bold]...")
        espn_games = await discover_espn_game_ids(date=date)
        completed = [g for g in espn_games if g.status == "post"]
        if not completed:
            console.print(f"[red]No completed games found for {date}[/red]")
            raise typer.Exit(code=1)
        espn_game_ids = [g.espn_game_id for g in completed]
        console.print(
            f"Found [bold]{len(completed)}[/bold] completed game(s)\n"
        )

    results = []
    for espn_game_id in espn_game_ids:
        console.print(f"Scraping [bold]{espn_game_id}[/bold]...", end=" ")
        snapshot = await scraper.fetch_game(espn_game_id)
        events = processor.process_snapshot(snapshot)

        store.save_snapshot(snapshot, date)
        store.save_events(espn_game_id, date, events)

        results.append((espn_game_id, events))
        console.print(f"[green]{len(events)} events[/green]")

        if verbose:
            for event in events:
                style, label = _event_label(event)
                console.print(
                    f"  [{style}]{label:>15}[/{style}]  "
                    f"P{event.period} {event.clock}  {event.text}"
                )

    _print_summary(results, store_dir)


def _print_summary(
    results: list[tuple[str, list]], store_dir: str
) -> None:
    table = Table(title="ESPN Scrape Summary")
    table.add_column("ESPN Game ID", style="bold")
    table.add_column("Scoring", justify="right")
    table.add_column("Fouls", justify="right")
    table.add_column("Timeouts", justify="right")
    table.add_column("Turnovers", justify="right")
    table.add_column("Periods", justify="right")
    table.add_column("Subs", justify="right")
    table.add_column("Total", justify="right")

    for game_id, events in results:
        counts = {
            "scoring": sum(1 for e in events if isinstance(e, ScoringPlayEvent)),
            "fouls": sum(1 for e in events if isinstance(e, FoulEvent)),
            "timeouts": sum(1 for e in events if isinstance(e, TimeoutEvent)),
            "turnovers": sum(1 for e in events if isinstance(e, TurnoverEvent)),
            "periods": sum(1 for e in events if isinstance(e, PeriodEvent)),
            "subs": sum(1 for e in events if isinstance(e, SubstitutionEvent)),
        }
        table.add_row(
            game_id,
            str(counts["scoring"]),
            str(counts["fouls"]),
            str(counts["timeouts"]),
            str(counts["turnovers"]),
            str(counts["periods"]),
            str(counts["subs"]),
            str(len(events)),
        )

    console.print()
    console.print(table)
    console.print(f"\n[bold]Stored in:[/bold] {store_dir}/")


@app.command("list-games")
def list_games(
    date: str = typer.Argument(help="Date to list (YYYY-MM-DD)."),
    store_dir: str = typer.Option(
        "data/espn", "--store-dir", "-d", help="Store directory."
    ),
) -> None:
    """List scraped ESPN games for a date."""
    store = ESPNStore(base_dir=store_dir)
    games = store.list_games(date)
    if not games:
        console.print(f"[red]No games stored for {date}[/red]")
        raise typer.Exit(code=1)

    console.print(f"[bold]{len(games)}[/bold] game(s) stored for {date}:\n")
    for game_id in games:
        events = store.load_events(game_id, date)
        console.print(f"  {game_id}  ({len(events)} events)")


@app.command("list-dates")
def list_dates(
    store_dir: str = typer.Option(
        "data/espn", "--store-dir", "-d", help="Store directory."
    ),
) -> None:
    """List all dates with scraped ESPN data."""
    store = ESPNStore(base_dir=store_dir)
    dates = store.list_dates()
    if not dates:
        console.print("[red]No ESPN data stored[/red]")
        raise typer.Exit(code=1)

    console.print(f"[bold]{len(dates)}[/bold] date(s) with ESPN data:\n")
    for d in dates:
        games = store.list_games(d)
        console.print(f"  {d}  ({len(games)} games)")


@app.command()
def show(
    date: str = typer.Argument(help="Date (YYYY-MM-DD)."),
    game_id: str = typer.Argument(help="ESPN game ID."),
    store_dir: str = typer.Option(
        "data/espn", "--store-dir", "-d", help="Store directory."
    ),
    event_type: str = typer.Option(
        None, "--type", "-t",
        help="Filter: scoring, foul, timeout, turnover, period, substitution.",
    ),
) -> None:
    """Show stored events for a specific game."""
    store = ESPNStore(base_dir=store_dir)
    events = store.load_events(game_id, date)
    if not events:
        console.print(f"[red]No events found for {game_id} on {date}[/red]")
        raise typer.Exit(code=1)

    type_filter = {
        "scoring": ScoringPlayEvent,
        "foul": FoulEvent,
        "timeout": TimeoutEvent,
        "turnover": TurnoverEvent,
        "period": PeriodEvent,
        "substitution": SubstitutionEvent,
    }
    if event_type and event_type in type_filter:
        events = [e for e in events if isinstance(e, type_filter[event_type])]

    console.print(
        f"[bold]{len(events)}[/bold] event(s) for {game_id} on {date}:\n"
    )
    for event in events:
        style, label = _event_label(event)
        console.print(
            f"  [{style}]{label:>15}[/{style}]  "
            f"P{event.period} {event.clock}  {event.text}"
        )


if __name__ == "__main__":
    app()
