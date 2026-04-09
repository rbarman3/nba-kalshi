"""Data models for the trading pipeline.

All models are immutable (frozen) dataclasses unless mutability is required.
These models flow through the pipeline layers: transport → processor → strategy → execution.

NBA CDN models: RawSnapshot, LineupChangeEvent, ScoreChangeEvent
ESPN models: ESPNPlayByPlaySnapshot, ScoringPlayEvent, FoulEvent, TimeoutEvent,
             TurnoverEvent, PeriodEvent, SubstitutionEvent
"""
from dataclasses import dataclass


@dataclass(frozen=False)
class RawSnapshot:
    """Raw game snapshot from ESPN/CDN boxscore endpoint.

    Emitted by NBATransport. Consumed and diffed by NBAProcessor.
    frozen=False because payload is a dict (mutable).
    """
    game_id: str
    payload: dict      # Full game dict from NBA CDN boxscore endpoint
    fetched_at: float  # Unix timestamp — used for staleness gate


@dataclass(frozen=True)
class LineupChangeEvent:
    """Emitted when a substitution changes the on-court lineup.

    Emitted by NBAProcessor when diff between consecutive RawSnapshots
    shows players entering or leaving the court.
    """
    game_id: str
    period: int
    clock: str                      # ISO 8601 duration remaining in period
    players_in: frozenset[str]      # personIds now on court
    players_out: frozenset[str]     # personIds now off court
    observed_at: float              # Unix timestamp when change detected


@dataclass(frozen=True)
class ScoreChangeEvent:
    """Emitted when the score changes between consecutive snapshots.

    Emitted by NBAProcessor when either team's score changes.
    """
    game_id: str
    home_score: int                 # Current home team score
    away_score: int                 # Current away team score
    home_prev: int                  # Previous home team score
    away_prev: int                  # Previous away team score
    period: int                     # Current period
    clock: str                      # ISO 8601 time remaining in period
    observed_at: float              # Unix timestamp when change detected


# ---------------------------------------------------------------------------
# ESPN play-by-play models
# ---------------------------------------------------------------------------


@dataclass(frozen=False)
class ESPNPlayByPlaySnapshot:
    """Raw ESPN play-by-play API response.

    Emitted by ESPNTransport. Consumed by ESPNPlayByPlayProcessor.
    frozen=False because payload is a dict (mutable).
    """
    espn_game_id: str
    payload: dict      # Full ESPN play-by-play JSON response
    fetched_at: float  # Unix timestamp when fetched


@dataclass(frozen=True)
class ScoringPlayEvent:
    """A made basket or free throw from ESPN play-by-play.

    Score differential drives game-outcome markets on Polymarket.
    Enables run detection (e.g. 10-0 run in last 2 minutes).
    """
    game_id: str
    espn_play_id: str
    sequence: int               # ESPN sequenceNumber for ordering
    period: int
    clock: str                  # ESPN display format "7:23"
    home_score: int
    away_score: int
    score_value: int            # 1, 2, or 3
    team_id: str                # ESPN team ID
    player_id: str              # ESPN athlete ID
    play_type: str              # "Jump Shot", "Driving Layup", etc.
    text: str                   # Human-readable play description
    wallclock: str              # ISO timestamp from ESPN
    observed_at: float


@dataclass(frozen=True)
class FoulEvent:
    """A foul called on a player from ESPN play-by-play.

    Foul trouble (5th foul) on a star player shifts game odds.
    Bonus status changes scoring dynamics.
    """
    game_id: str
    espn_play_id: str
    sequence: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: str
    player_id: str
    foul_type: str              # "Shooting Foul", "Technical Foul", etc.
    text: str
    wallclock: str
    observed_at: float


@dataclass(frozen=True)
class TimeoutEvent:
    """A timeout called by a team or official from ESPN play-by-play.

    Teams call timeouts to stop opponent runs — correlates with momentum shifts.
    """
    game_id: str
    espn_play_id: str
    sequence: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: str
    timeout_type: str           # "Full Timeout", "20 Second Timeout"
    text: str
    wallclock: str
    observed_at: float


@dataclass(frozen=True)
class TurnoverEvent:
    """A turnover by a player from ESPN play-by-play.

    Burst of turnovers late in close games is highly market-moving.
    """
    game_id: str
    espn_play_id: str
    sequence: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: str
    player_id: str
    turnover_type: str          # "Bad Pass", "Lost Ball", "Traveling"
    text: str
    wallclock: str
    observed_at: float


@dataclass(frozen=True)
class PeriodEvent:
    """Period start or end from ESPN play-by-play.

    Period boundaries are natural market re-evaluation points (4Q start, OT).
    """
    game_id: str
    espn_play_id: str
    sequence: int
    period: int
    clock: str
    home_score: int
    away_score: int
    event_type: str             # "start" or "end"
    text: str
    wallclock: str
    observed_at: float


@dataclass(frozen=True)
class SubstitutionEvent:
    """A player substitution from ESPN play-by-play.

    Captures lineup changes with player-level detail from the ESPN feed.
    """
    game_id: str
    espn_play_id: str
    sequence: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: str
    player_in_id: str           # ESPN athlete ID entering
    player_out_id: str          # ESPN athlete ID leaving
    text: str                   # e.g. "Reaves enters the game for Russell"
    wallclock: str
    observed_at: float


@dataclass(frozen=True)
class ReplayResult:
    """Result of replaying a single game's events.

    Used by both ESPNReplayer and SnapshotReplayer to summarize
    the output of a backtesting replay session.
    """
    game_id: str
    events: list                # List of event instances (type varies)
    snapshot_count: int         # Number of snapshots/events processed
    duration_seconds: float     # Wall-clock time elapsed during replay
