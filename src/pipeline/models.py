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
# NBA CDN action events — parsed from game.actions[] in each snapshot
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ScoringPlayEvent:
    """A made field goal or free throw from NBA CDN game.actions[].

    Emitted when actionType in ("2pt", "3pt", "freethrow") and shotResult == "Made".
    Provides play-level detail that ScoreChangeEvent does not (individual shot vs. aggregate).
    """
    game_id: str
    action_number: int              # NBA CDN actionNumber — monotonically increasing
    period: int
    clock: str                      # ISO 8601 duration e.g. "PT05M32.00S"
    home_score: int
    away_score: int
    score_value: int                # 1, 2, or 3
    team_id: int
    player_id: int
    action_type: str                # "2pt", "3pt", or "freethrow"
    sub_type: str                   # "Driving Layup", "Jump Shot", "Free Throw", etc.
    description: str
    observed_at: float


@dataclass(frozen=True)
class FoulEvent:
    """A foul called from NBA CDN game.actions[].

    Emitted when actionType == "foul". Foul trouble on a star player shifts game odds.
    """
    game_id: str
    action_number: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: int
    player_id: int
    foul_type: str                  # "personal", "shooting", "technical", "flagrant"
    description: str
    observed_at: float


@dataclass(frozen=True)
class TimeoutEvent:
    """A timeout from NBA CDN game.actions[].

    Emitted when actionType == "timeout". Correlates with momentum shifts.
    """
    game_id: str
    action_number: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: int
    timeout_type: str               # "full", "short", "official"
    description: str
    observed_at: float


@dataclass(frozen=True)
class TurnoverEvent:
    """A turnover from NBA CDN game.actions[].

    Emitted when actionType == "turnover". Late-game turnover bursts are market-moving.
    """
    game_id: str
    action_number: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: int
    player_id: int
    turnover_type: str              # "bad pass", "lost ball", "traveling", "shot clock"
    description: str
    observed_at: float


@dataclass(frozen=True)
class PeriodEvent:
    """Period start or end from NBA CDN game.actions[].

    Emitted when actionType == "period". Period boundaries are natural
    market re-evaluation points (4Q start, OT).
    """
    game_id: str
    action_number: int
    period: int
    clock: str
    home_score: int
    away_score: int
    event_type: str                 # "start" or "end"
    description: str
    observed_at: float


@dataclass(frozen=True)
class SubstitutionEvent:
    """A player substitution from NBA CDN game.actions[].

    Emitted for each substitution action (actionType == "substitution").
    sub_type is "in" (player entering) or "out" (player leaving).
    NBA CDN represents subs as individual in/out actions — see LineupChangeEvent
    for the net lineup diff between snapshots.
    """
    game_id: str
    action_number: int
    period: int
    clock: str
    home_score: int
    away_score: int
    team_id: int
    player_id: int
    sub_type: str                   # "in" or "out"
    description: str
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


