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
class FeedHealthEvent:
    """Emitted by FeedWatchdog when feed health status changes.

    game_id=None means overall feed health across all games.
    """
    status: str           # "HEALTHY", "DEGRADED", "DEAD"
    game_id: str | None   # None = overall, str = per-game
    last_success: float   # Unix timestamp of last successful poll
    observed_at: float    # Unix timestamp when this event was emitted
    message: str
