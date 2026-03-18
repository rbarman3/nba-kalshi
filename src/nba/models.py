"""
Domain models for the NBA scores CLI.

All models are immutable frozen dataclasses. No business logic lives here;
they are plain data carriers passed between the service layer and the CLI.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class GameSummary:
    game_id: str
    home_team: str
    away_team: str
    home_score: int
    away_score: int
    status: str   # e.g. "Final", "In Progress", "Scheduled"
    period: int   # 0 = not started, 1-4 = regulation, 5+ = overtime
    clock: str    # ISO 8601 duration, e.g. "PT05M32.00S"; empty when game is over


@dataclass(frozen=True)
class Player:
    id: int
    full_name: str
    first_name: str
    last_name: str
    is_active: bool  # False for retired/waived players still in the static dataset


@dataclass(frozen=True)
class PlayerOnCourt:
    name: str
    jersey_num: str  # stored as str to preserve leading zeros, e.g. "00"
    position: str
    points: int
    assists: int
    rebounds: int


@dataclass(frozen=True)
class Team:
    id: int
    full_name: str
    abbreviation: str
    nickname: str
    city: str
    state: str
    year_founded: int  # year franchise was established (may differ from current city)
