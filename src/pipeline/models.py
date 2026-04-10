"""Data models for the trading pipeline.

All models are immutable (frozen) dataclasses unless mutability is required.
These models flow through the pipeline layers: transport → processor → strategy → execution.
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
class FeedHealthEvent:
    """Emitted by FeedWatchdog when feed health status changes.

    game_id=None means overall feed health across all games.
    """
    status: str           # "HEALTHY", "DEGRADED", "DEAD"
    game_id: str | None   # None = overall, str = per-game
    last_success: float   # Unix timestamp of last successful poll
    observed_at: float    # Unix timestamp when this event was emitted
    message: str


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
# Snapshot-diff events — derived by comparing consecutive boxscore snapshots
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class FoulEvent:
    """Emitted when a player's foulsPersonal increases between snapshots.

    Foul trouble on a star player shifts game odds.
    """
    game_id: str
    period: int
    clock: str
    team_id: int
    team_tricode: str
    player_id: str
    player_name: str
    prev_fouls: int
    curr_fouls: int
    home_score: int
    away_score: int
    observed_at: float


@dataclass(frozen=True)
class TimeoutEvent:
    """Emitted when a team's timeoutsRemaining decreases between snapshots.

    Correlates with momentum shifts.
    """
    game_id: str
    period: int
    clock: str
    team_id: int
    team_tricode: str
    prev_timeouts: int
    curr_timeouts: int
    home_score: int
    away_score: int
    observed_at: float


@dataclass(frozen=True)
class TurnoverEvent:
    """Emitted when a player's turnovers stat increases between snapshots.

    Late-game turnover bursts are market-moving.
    """
    game_id: str
    period: int
    clock: str
    team_id: int
    team_tricode: str
    player_id: str
    player_name: str
    prev_turnovers: int
    curr_turnovers: int
    home_score: int
    away_score: int
    observed_at: float


@dataclass(frozen=True)
class PeriodEvent:
    """Emitted when game.period changes between snapshots.

    Period boundaries are natural market re-evaluation points (4Q start, OT).
    """
    game_id: str
    prev_period: int
    curr_period: int
    clock: str
    home_score: int
    away_score: int
    game_status: int                # 1=not started, 2=live, 3=final
    observed_at: float


@dataclass(frozen=True)
class ScoringPlayEvent:
    """Emitted when a player's points stat increases between snapshots.

    Identifies WHO scored and the point delta. Cannot determine shot type
    (2pt vs 3pt vs FT) from boxscore stats alone — use score_delta instead.
    """
    game_id: str
    period: int
    clock: str
    team_id: int
    team_tricode: str
    player_id: str
    player_name: str
    prev_points: int
    curr_points: int
    score_delta: int                # curr_points - prev_points (1, 2, 3, or more if gap)
    home_score: int
    away_score: int
    observed_at: float


@dataclass(frozen=True)
class SubstitutionEvent:
    """Emitted for each individual player entering or leaving the court.

    Derived from lineup diff. sub_type is "in" or "out".
    Provides per-player granularity that LineupChangeEvent aggregates.
    """
    game_id: str
    period: int
    clock: str
    team_id: int
    team_tricode: str
    player_id: str
    player_name: str
    sub_type: str                   # "in" or "out"
    home_score: int
    away_score: int
    observed_at: float


@dataclass(frozen=True)
class ReplayResult:
    """Result of replaying a single game's events.

    Shared between ESPNReplayer and SnapshotReplayer.
    """
    game_id: str
    events: list                    # Any event instances
    snapshot_count: int             # Snapshots or events processed
    duration_seconds: float         # Wall-clock time elapsed during replay
