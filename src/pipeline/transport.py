"""
Async NBA boxscore transport layer for the trading pipeline (Layer 1).

NBATransport polls the NBA CDN boxscore endpoint with jittered intervals,
wrapping raw responses into RawSnapshot objects and placing them on an asyncio.Queue.

Game state tracking:
  - UNKNOWN     : never polled
  - NOT_STARTED : 403 response — re-check every 60s
  - LIVE        : 200 + gameStatus==2 — poll every cycle
  - FINAL       : 200 + gameStatus==3 — stop polling entirely
"""
import asyncio
import enum
import hashlib
import json
import logging
import os
import random
import time
from dataclasses import dataclass, field

import httpx

from pipeline.models import RawSnapshot
from pipeline.store import SnapshotStore

logger = logging.getLogger(__name__)

NOT_STARTED_POLL_INTERVAL = 60.0  # seconds between re-checks for 403 games

_DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)
_DEFAULT_SEC_CH_UA = '"Not:A-Brand";v="99", "Google Chrome";v="145", "Chromium";v="145"'


class GameStatus(enum.Enum):
    UNKNOWN = "unknown"
    NOT_STARTED = "not_started"
    LIVE = "live"
    FINAL = "final"


@dataclass
class _PollState:
    status: GameStatus = GameStatus.UNKNOWN
    last_polled: float = 0.0
    last_payload_hash: str = ""


def _hash_payload(payload: dict) -> str:
    return hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()


class NBATransport:
    """Poll NBA boxscore endpoint for multiple games concurrently, emit RawSnapshot onto a queue."""

    @classmethod
    def _build_headers(cls) -> dict[str, str]:
        return {
            "Host": "cdn.nba.com",
            "User-Agent": os.environ.get("NBA_CDN_USER_AGENT", _DEFAULT_USER_AGENT),
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://www.nba.com/",
            "Sec-Ch-Ua": os.environ.get("NBA_CDN_SEC_CH_UA", _DEFAULT_SEC_CH_UA),
            "Sec-Ch-Ua-Mobile": "?0",
        }

    HEADERS = {
        "Host": "cdn.nba.com",
        "User-Agent": _DEFAULT_USER_AGENT,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.nba.com/",
        "Sec-Ch-Ua": _DEFAULT_SEC_CH_UA,
        "Sec-Ch-Ua-Mobile": "?0",
    }

    def __init__(
        self,
        game_ids: list[str],
        queue: asyncio.Queue,
        poll_interval_range: tuple[float, float] = (0.6, 1.2),
        store: SnapshotStore | None = None,
    ) -> None:
        self.game_ids = game_ids
        self.queue = queue
        self.poll_interval_range = poll_interval_range
        self.store = store
        self._poll_states: dict[str, _PollState] = {gid: _PollState() for gid in game_ids}
        self.cache: dict[str, RawSnapshot] = {}

    async def run(self) -> None:
        """Poll all games concurrently forever. Never returns."""
        async with httpx.AsyncClient(timeout=10, headers=self._build_headers()) as client:
            while True:
                await asyncio.gather(*[self._fetch_one(gid, client) for gid in self.game_ids])
                interval = random.uniform(*self.poll_interval_range)
                await asyncio.sleep(interval)

    async def _fetch_one(self, game_id: str, client: httpx.AsyncClient) -> None:
        """Fetch one game, wrap in RawSnapshot, put on queue. Log errors but never crash."""
        state = self._poll_states[game_id]

        if state.status == GameStatus.FINAL:
            return

        if state.status == GameStatus.NOT_STARTED:
            if time.time() - state.last_polled < NOT_STARTED_POLL_INTERVAL:
                return

        url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
        try:
            resp = await client.get(url)
            state.last_polled = time.time()

            if resp.status_code == 403:
                state.status = GameStatus.NOT_STARTED
                return

            resp.raise_for_status()

            payload = resp.json()
            game_status = payload.get("game", {}).get("gameStatus", 2)

            if game_status == 3:
                state.status = GameStatus.FINAL
                return

            state.status = GameStatus.LIVE
            payload_hash = _hash_payload(payload)
            if payload_hash == state.last_payload_hash:
                return

            state.last_payload_hash = payload_hash
            snapshot = RawSnapshot(
                game_id=game_id,
                payload=payload,
                fetched_at=time.time(),
            )
            self.cache[game_id] = snapshot
            await self.queue.put(snapshot)
            if self.store:
                await self.store.persist(snapshot)

        except Exception as e:
            logger.error(f"Failed to fetch game {game_id}: {e}")
