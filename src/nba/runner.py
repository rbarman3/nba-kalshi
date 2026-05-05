"""Pipeline runner — wires and orchestrates the market data feed.

Entry point: nba-pipeline (or: PYTHONPATH=src python3 -m nba.runner)

Discovers today's live games, spins up transport → processor → watchdog layers,
and logs events/health until interrupted (Ctrl+C).

Environment variables:
  SNAPSHOT_STORE_ENABLED  — "true" to persist snapshots to disk (default: "false")
  SNAPSHOT_STORE_DIR      — base directory for snapshots (default: "data/snapshots")
  API_STATS_DIR           — base directory for API stats (default: "data/api_stats")
  PIPELINE_POLL_MIN       — min poll interval seconds (default: "0.6")
  PIPELINE_POLL_MAX       — max poll interval seconds (default: "1.2")
"""
import asyncio
import logging
import os
from pathlib import Path

from pipeline.models import (
    FeedHealthEvent,
    FoulEvent,
    LineupChangeEvent,
    PeriodEvent,
    ScoreChangeEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    TimeoutEvent,
    TurnoverEvent,
)
from pipeline.api_stats import ApiStatsCollector
from pipeline.processor import NBAProcessor
from pipeline.signal import FeedQualitySignal
from pipeline.slo_writer import SLOWriter
from pipeline.store import SnapshotStore
from pipeline.transport import NBATransport
from nba.live_service import get_live_scoreboard

try:
    from kalshi.auth import load_credentials_from_env, KalshiSigner
    from kalshi.transport import KalshiTransport
    from kalshi.ticker_map import GameTickerResolver, all_tickers
    from kalshi.correlator import LatencyCorrelator
    from kalshi.recorder import ReactionRecorder
    _KALSHI_AVAILABLE = True
except ImportError:
    _KALSHI_AVAILABLE = False

try:
    from pipeline.watchdog import FeedWatchdog
    _WATCHDOG_AVAILABLE = True
except ImportError:
    _WATCHDOG_AVAILABLE = False

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


async def _log_events(
    event_queue: asyncio.Queue,
    signal: 'FeedQualitySignal | None' = None,
    fanout: list[asyncio.Queue] | None = None,
) -> None:
    """Log pipeline events; optionally feed signal latency hist + fan-out queues."""
    while True:
        event = await event_queue.get()
        if signal is not None:
            signal.on_event(event)
        if fanout:
            for q in fanout:
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    pass
        if isinstance(event, LineupChangeEvent):
            logger.info(
                f"[LINEUP]  game={event.game_id} period={event.period} "
                f"clock={event.clock} in={len(event.players_in)} out={len(event.players_out)}"
            )
        elif isinstance(event, ScoreChangeEvent):
            logger.info(
                f"[SCORE]   game={event.game_id} period={event.period} "
                f"clock={event.clock} home={event.home_score} away={event.away_score}"
            )
        elif isinstance(event, FoulEvent):
            logger.info(
                f"[FOUL]    game={event.game_id} period={event.period} "
                f"clock={event.clock} {event.team_tricode} {event.player_name} "
                f"fouls={event.curr_fouls}"
            )
        elif isinstance(event, TimeoutEvent):
            logger.info(
                f"[TIMEOUT] game={event.game_id} period={event.period} "
                f"clock={event.clock} {event.team_tricode} "
                f"remaining={event.curr_timeouts}"
            )
        elif isinstance(event, TurnoverEvent):
            logger.info(
                f"[TOVER]   game={event.game_id} period={event.period} "
                f"clock={event.clock} {event.team_tricode} {event.player_name}"
            )
        elif isinstance(event, PeriodEvent):
            logger.info(
                f"[PERIOD]  game={event.game_id} {event.prev_period}→{event.curr_period}"
            )
        elif isinstance(event, ScoringPlayEvent):
            logger.info(
                f"[BASKET]  game={event.game_id} period={event.period} "
                f"clock={event.clock} {event.team_tricode} {event.player_name} "
                f"+{event.score_delta}"
            )
        elif isinstance(event, SubstitutionEvent):
            logger.info(
                f"[SUB]     game={event.game_id} period={event.period} "
                f"clock={event.clock} {event.team_tricode} {event.player_name} "
                f"{event.sub_type}"
            )


async def _log_health(health_queue: asyncio.Queue) -> None:
    """Log FeedHealthEvents from the health queue until cancelled."""
    while True:
        event = await health_queue.get()
        if isinstance(event, FeedHealthEvent):
            logger.info(f"Feed health: {event.message}")


async def run_pipeline(game_ids: list[str]) -> None:
    """Wire and run all pipeline layers concurrently.

    Args:
        game_ids: List of NBA game IDs to monitor.
    """
    raw_queue = asyncio.Queue()
    event_queue = asyncio.Queue()
    health_queue = asyncio.Queue()

    # Create store if enabled
    store = None
    if os.getenv("SNAPSHOT_STORE_ENABLED") == "true":
        store_dir = os.getenv("SNAPSHOT_STORE_DIR", "data/snapshots")
        store = SnapshotStore(base_dir=store_dir)
        logger.info(f"Snapshots will be persisted to {store_dir}")

    # Poll interval configuration
    poll_min = float(os.getenv("PIPELINE_POLL_MIN", "0.6"))
    poll_max = float(os.getenv("PIPELINE_POLL_MAX", "1.2"))

    # API stats collector — always enabled for operational visibility
    stats_dir = os.getenv("API_STATS_DIR", "data/api_stats")
    stats = ApiStatsCollector(base_dir=stats_dir)
    logger.info(f"API stats will be written to {stats_dir}")

    # Feed quality signal — real-time confidence scoring for strategy layer
    signal = FeedQualitySignal()

    # Wire layers
    transport = NBATransport(
        game_ids=game_ids,
        queue=raw_queue,
        poll_interval_range=(poll_min, poll_max),
        store=store,
        stats=stats,
        signal=signal,
    )
    processor = NBAProcessor(in_queue=raw_queue, out_queue=event_queue)

    logger.info(f"Starting pipeline with {len(game_ids)} game(s)")

    fanout_queues: list[asyncio.Queue] = []
    kalshi_tasks: list = []

    if _KALSHI_AVAILABLE:
        creds = load_credentials_from_env()
        if creds is None:
            logger.info("Kalshi creds not found (env or ~/.kalshi/); skipping Kalshi layer")
        else:
            signer = KalshiSigner(creds)
            resolver = GameTickerResolver(signer=signer)
            tickers = all_tickers(resolver, game_ids)
            market_queue: asyncio.Queue = asyncio.Queue()
            kalshi_transport = KalshiTransport(signer=signer, market_tickers=tickers, queue=market_queue)
            kalshi_tasks.append(kalshi_transport.run())

            # Always run recorder when Kalshi is wired (raw dataset for offline analysis)
            recorder_event_queue: asyncio.Queue = asyncio.Queue()
            fanout_queues.append(recorder_event_queue)
            recorder = ReactionRecorder(
                event_queue=recorder_event_queue,
                market_queue=market_queue,
            )
            kalshi_tasks.append(recorder.run())
            logger.info("Kalshi recorder wired (%d tickers); writes to data/reactions/", len(tickers))

            # Correlator gated — off until reaction definition is finalized
            if os.getenv("KALSHI_CORRELATOR_ENABLED", "").lower() == "true":
                correlator_event_queue: asyncio.Queue = asyncio.Queue()
                fanout_queues.append(correlator_event_queue)
                # NB: market_queue is single-consumer here; recorder gets it.
                # If both are needed simultaneously, a market fan-out is the next step.
                logger.warning("KALSHI_CORRELATOR_ENABLED=true but market_queue is consumed by recorder; correlator disabled until market fan-out is added")

    fanout = fanout_queues if fanout_queues else None
    slo_writer = SLOWriter(
        signal=signal,
        game_ids=game_ids,
        base_dir=os.getenv("SLO_DIR", "data/slo"),
        interval_s=float(os.getenv("SLO_FLUSH_INTERVAL_S", "5.0")),
    )
    tasks = [
        transport.run(),
        processor.run(),
        stats.run(),
        _log_events(event_queue, signal=signal, fanout=fanout),
        slo_writer.run(),
    ]
    tasks += kalshi_tasks

    if _WATCHDOG_AVAILABLE:
        watchdog = FeedWatchdog(transport=transport, health_queue=health_queue)
        tasks += [watchdog.run(), _log_health(health_queue)]

    await asyncio.gather(*tasks)


def main() -> None:
    """Entry point: discover today's games and start the pipeline."""
    logger.info("Discovering today's games...")
    games = get_live_scoreboard()
    game_ids = [g.game_id for g in games]

    if not game_ids:
        logger.info("No games scheduled today.")
        return

    logger.info(f"Found {len(game_ids)} game(s):")
    for g in games:
        logger.info(f"  {g.away_team:15} @ {g.home_team:15}  [{g.status:12}]  {g.game_id}")

    try:
        asyncio.run(run_pipeline(game_ids))
    except KeyboardInterrupt:
        logger.info("Pipeline stopped.")


if __name__ == "__main__":
    main()
