"""ESPN play-by-play scraper for historical game data.

One-shot fetcher for completed games — designed for backtesting, not live polling.
Fetches the full play-by-play JSON for a game and returns an ESPNPlayByPlaySnapshot.
"""
import logging
import time

import httpx

from .models import ESPNPlayByPlaySnapshot

logger = logging.getLogger(__name__)

PLAYBYPLAY_URL = "https://cdn.espn.com/core/nba/playbyplay"


class ESPNScraper:
    """Fetch ESPN play-by-play data for historical games.

    Usage:
        scraper = ESPNScraper()
        snapshot = await scraper.fetch_game("401584793")
        snapshots = await scraper.fetch_games(["401584793", "401584794"])
    """

    def __init__(self, timeout: float = 15.0) -> None:
        self.timeout = timeout

    async def fetch_game(self, espn_game_id: str) -> ESPNPlayByPlaySnapshot:
        """Fetch play-by-play for a single game.

        Args:
            espn_game_id: ESPN game identifier.

        Returns:
            ESPNPlayByPlaySnapshot with full play-by-play payload.

        Raises:
            httpx.HTTPStatusError: On non-2xx response.
        """
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            resp = await client.get(
                PLAYBYPLAY_URL,
                params={"xhr": "1", "gameId": espn_game_id},
                headers={"Accept": "application/json"},
            )
            resp.raise_for_status()
            payload = resp.json()

        logger.info("Fetched play-by-play for ESPN game %s", espn_game_id)

        return ESPNPlayByPlaySnapshot(
            espn_game_id=espn_game_id,
            payload=payload,
            fetched_at=time.time(),
        )

    async def fetch_games(
        self, espn_game_ids: list[str]
    ) -> list[ESPNPlayByPlaySnapshot]:
        """Fetch play-by-play for multiple games sequentially.

        Sequential to be respectful of ESPN's servers. For backtesting
        there is no urgency — correctness over speed.

        Args:
            espn_game_ids: List of ESPN game identifiers.

        Returns:
            List of ESPNPlayByPlaySnapshot, one per game.
        """
        if not espn_game_ids:
            return []

        snapshots = []
        for game_id in espn_game_ids:
            snapshot = await self.fetch_game(game_id)
            snapshots.append(snapshot)

        logger.info("Fetched %d games from ESPN", len(snapshots))
        return snapshots
