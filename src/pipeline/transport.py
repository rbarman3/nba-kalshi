"""
Async NBA boxscore transport layer for the trading pipeline (Layer 1).

NBATransport polls the NBA CDN boxscore endpoint with jittered intervals,
wrapping raw responses into RawSnapshot objects and placing them on an asyncio.Queue.
"""
import asyncio
import logging
import random
import time

import httpx

from pipeline.models import RawSnapshot

logger = logging.getLogger(__name__)


class NBATransport:
    """Poll NBA boxscore endpoint for multiple games concurrently, emit RawSnapshot onto a queue."""

    HEADERS = {
        "Host": "cdn.nba.com",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.nba.com/",
        "Sec-Ch-Ua": '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"',
        "Sec-Ch-Ua-Mobile": "?0",
    }

    def __init__(
        self,
        game_ids: list[str],
        queue: asyncio.Queue,
        poll_interval_range: tuple[float, float] = (0.6, 1.2),
    ) -> None:
        """
        Parameters
        ----------
        game_ids : list[str]
            NBA game IDs to poll.
        queue : asyncio.Queue
            Queue to push RawSnapshot objects onto.
        poll_interval_range : tuple[float, float]
            (min, max) seconds between polls. Each cycle picks a random value in this range.
        """
        self.game_ids = game_ids
        self.queue = queue
        self.poll_interval_range = poll_interval_range

    async def run(self) -> None:
        """Poll all games concurrently forever. Never returns."""
        async with httpx.AsyncClient(timeout=10, headers=self.HEADERS) as client:
            while True:
                await asyncio.gather(*[self._fetch_one(gid, client) for gid in self.game_ids])
                interval = random.uniform(*self.poll_interval_range)
                await asyncio.sleep(interval)

    async def _fetch_one(self, game_id: str, client: httpx.AsyncClient) -> None:
        """Fetch one game, wrap in RawSnapshot, put on queue. Log errors but never crash."""
        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        try:
            resp = await client.get(url)
            resp.raise_for_status()
            snapshot = RawSnapshot(
                game_id=game_id,
                payload=resp.json(),
                fetched_at=time.time(),
            )
            await self.queue.put(snapshot)
        except Exception as e:
            logger.error(f"Failed to fetch game {game_id}: {e}")
