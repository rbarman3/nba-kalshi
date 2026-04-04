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


