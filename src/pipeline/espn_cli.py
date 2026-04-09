"""CLI for ESPN play-by-play scraping and event extraction.

An independent tool for gathering NBA play-by-play data from ESPN.
Scrapes historical games, classifies plays into typed events, and
stores results for consumption by the backtesting pipeline.

Usage:
    # Scraping commands
    nba-espn scrape 2026-03-29
    nba-espn scrape 2026-03-29 --game 401584793
    nba-espn scrape 2026-03-29 --store-dir data/espn

    # Discovery commands
    nba-espn list-games 2026-03-29
    nba-espn list-dates
    nba-espn show 2026-03-29 401584793

    # Replay/backtesting commands
    nba-espn replay 2026-03-29
    nba-espn replay 2026-03-29 --game 401584793 --mode realtime
    nba-espn replay 2026-03-29 --mode realtime --signal-delay 1.0
"""
import asyncio
import logging

import typer
from rich.console import Console
from rich.table import Table

from pipeline.espn_id_map import discover_espn_game_ids
from pipeline.espn_processor import ESPNPlayByPlayProcessor
from pipeline.espn_replayer import ESPNReplayer
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


@app.command()
def replay(
    date: str = typer.Argument(help="Date to replay (YYYY-MM-DD)."),
    game: str = typer.Option(
        None, "--game", "-g", help="Replay a single ESPN game ID."
    ),
    mode: str = typer.Option(
        "fast", "--mode", "-m", help="Replay mode: fast or realtime."
    ),
    store_dir: str = typer.Option(
        "data/espn", "--store-dir", "-d", help="Store directory."
    ),
    signal_delay: float = typer.Option(
        0.0, "--signal-delay", "-s",
        help="Seconds to add to observed_at (latency simulation).",
    ),
    speed: float = typer.Option(
        1.0, "--speed", "-x",
        help="Realtime speed multiplier (e.g. 60 = 60x faster). Only applies in realtime mode.",
    ),
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Show individual events."
    ),
    event_type: str = typer.Option(
        None, "--type", "-t",
        help="Filter: scoring, foul, timeout, turnover, period, substitution.",
    ),
) -> None:
    """Replay ESPN events for backtesting.

    In realtime mode events are printed live as they arrive — use --speed to
    compress time (e.g. --speed 60 replays a 2-hour game in ~2 minutes).
    In fast mode all events are collected first, then the summary is shown.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
    )
    asyncio.run(
        _do_replay(date, game, mode, store_dir, signal_delay, speed, verbose, event_type)
    )


async def _do_replay(
    date: str,
    game: str | None,
    mode: str,
    store_dir: str,
    signal_delay: float,
    speed: float,
    verbose: bool,
    event_type: str | None,
) -> None:
    store = ESPNStore(base_dir=store_dir)
    replayer = ESPNReplayer(store=store, mode=mode, signal_delay=signal_delay, speed=speed)

    type_filter = {
        "scoring": ScoringPlayEvent,
        "foul": FoulEvent,
        "timeout": TimeoutEvent,
        "turnover": TurnoverEvent,
        "period": PeriodEvent,
        "substitution": SubstitutionEvent,
    }

    game_ids = [game] if game else store.list_games(date)
    if not game_ids:
        console.print(f"[red]No games stored for {date}[/red]")
        raise typer.Exit(code=1)

    speed_label = f" at [bold]{speed}x[/bold]" if speed != 1.0 else ""
    console.print(
        f"Replaying [bold]{len(game_ids)}[/bold] game(s) for [bold]{date}[/bold]"
        f" in [bold]{mode}[/bold] mode{speed_label}...\n"
    )

    if mode == "realtime":
        # Stream events live — print each one as it arrives so user sees progress
        results = await _stream_realtime(
            replayer, game_ids, date, type_filter, event_type
        )
    else:
        # Fast mode: collect all events, then print summary
        results = await _collect_fast(replayer, game_ids, date, verbose, type_filter, event_type)

    if not results or not any(r.events for r in results):
        console.print(f"[red]No events found[/red]")
        raise typer.Exit(code=1)

    _print_replay_summary(results, store_dir)


async def _stream_realtime(
    replayer: ESPNReplayer,
    game_ids: list[str],
    date: str,
    type_filter: dict,
    event_type: str | None,
) -> list:
    """Stream events live, printing each as it arrives. Used for realtime mode."""
    from datetime import datetime, timezone
    from .models import ReplayResult

    results = []
    for game_id in game_ids:
        console.print(f"[bold]{game_id}[/bold]")
        events_list = []
        start = datetime.now(timezone.utc).timestamp()

        async for event in replayer.stream_events(game_id, date):
            events_list.append(event)
            # Skip if type filter set and event doesn't match
            if event_type and event_type in type_filter:
                if not isinstance(event, type_filter[event_type]):
                    continue
            style, label = _event_label(event)
            console.print(
                f"  [{style}]{label:>15}[/{style}]  "
                f"P{event.period} {event.clock}  {event.text}"
            )

        end = datetime.now(timezone.utc).timestamp()
        results.append(
            ReplayResult(
                game_id=game_id,
                events=events_list,
                snapshot_count=len(events_list),
                duration_seconds=end - start,
            )
        )
        console.print()

    return results


async def _collect_fast(
    replayer: ESPNReplayer,
    game_ids: list[str],
    date: str,
    verbose: bool,
    type_filter: dict,
    event_type: str | None,
) -> list:
    """Collect all events then display. Used for fast mode."""
    results = []
    for game_id in game_ids:
        result = await replayer.replay_game(game_id, date)
        results.append(result)

    if verbose:
        for result in results:
            if not result.events:
                continue
            console.print(f"[bold]{result.game_id}[/bold] ({len(result.events)} events):")
            for event in result.events:
                if event_type and event_type in type_filter:
                    if not isinstance(event, type_filter[event_type]):
                        continue
                style, label = _event_label(event)
                console.print(
                    f"  [{style}]{label:>15}[/{style}]  "
                    f"P{event.period} {event.clock}  {event.text}"
                )
            console.print()

    return results


def _print_replay_summary(results: list, store_dir: str) -> None:
    """Print summary table of replay results."""
    table = Table(title="ESPN Replay Summary")
    table.add_column("ESPN Game ID", style="bold")
    table.add_column("Events", justify="right")
    table.add_column("Duration (s)", justify="right")
    table.add_column("Scoring", justify="right")
    table.add_column("Fouls", justify="right")
    table.add_column("Timeouts", justify="right")
    table.add_column("Turnovers", justify="right")
    table.add_column("Periods", justify="right")
    table.add_column("Subs", justify="right")

    total_events = 0
    for result in results:
        if not result.events:
            continue
        counts = {
            "scoring": sum(1 for e in result.events if isinstance(e, ScoringPlayEvent)),
            "fouls": sum(1 for e in result.events if isinstance(e, FoulEvent)),
            "timeouts": sum(1 for e in result.events if isinstance(e, TimeoutEvent)),
            "turnovers": sum(1 for e in result.events if isinstance(e, TurnoverEvent)),
            "periods": sum(1 for e in result.events if isinstance(e, PeriodEvent)),
            "subs": sum(1 for e in result.events if isinstance(e, SubstitutionEvent)),
        }
        total_events += len(result.events)
        table.add_row(
            result.game_id,
            str(len(result.events)),
            f"{result.duration_seconds:.3f}",
            str(counts["scoring"]),
            str(counts["fouls"]),
            str(counts["timeouts"]),
            str(counts["turnovers"]),
            str(counts["periods"]),
            str(counts["subs"]),
        )

    console.print(table)
    console.print(f"\n[bold]Total events replayed:[/bold] {total_events}")
    console.print(f"[bold]Store directory:[/bold] {store_dir}/")


if __name__ == "__main__":
    app()
