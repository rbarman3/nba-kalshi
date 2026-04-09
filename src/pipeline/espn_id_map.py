"""ESPN game discovery via scoreboard API.

Fetches the ESPN NBA scoreboard for a given date and returns game metadata.
Used to discover ESPN game IDs for historical play-by-play scraping.
"""
import logging
from dataclasses import dataclass
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
)


@dataclass(frozen=True)
class ESPNGame:
    """Metadata for a single ESPN game."""
    espn_game_id: str
    home_team: str       # Team abbreviation (e.g., "LAL")
    away_team: str       # Team abbreviation (e.g., "BOS")
    status: str          # "pre", "in", or "post"


def _parse_event(event: dict) -> ESPNGame:
    """Parse a single scoreboard event into an ESPNGame."""
    competition = event["competitions"][0]
    competitors = competition["competitors"]

    home = next(c for c in competitors if c["homeAway"] == "home")
    away = next(c for c in competitors if c["homeAway"] == "away")

    return ESPNGame(
        espn_game_id=event["id"],
        home_team=home["team"]["abbreviation"],
        away_team=away["team"]["abbreviation"],
        status=competition["status"]["type"]["state"],
    )


async def discover_espn_game_ids(
    date: Optional[str] = None,
) -> list[ESPNGame]:
    """Fetch ESPN scoreboard and return game metadata.

    Args:
        date: Date string YYYY-MM-DD. If None, returns today's games.

    Returns:
        List of ESPNGame with IDs, teams, and status.
    """
    params = {}
    if date:
        params["dates"] = date.replace("-", "")

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.get(SCOREBOARD_URL, params=params)
        resp.raise_for_status()
        data = resp.json()

    events = data.get("events", [])
    games = [_parse_event(e) for e in events]

    logger.info("Discovered %d ESPN games%s", len(games), f" for {date}" if date else "")
    return games
