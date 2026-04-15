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
from datetime import datetime

import httpx

from pipeline.api_stats import ApiStatsCollector
from pipeline.models import PollResult, RawSnapshot
from pipeline.signal import FeedQualitySignal
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
        stats: ApiStatsCollector | None = None,
        signal: FeedQualitySignal | None = None,
    ) -> None:
        self.game_ids = game_ids
        self.queue = queue
        self.poll_interval_range = poll_interval_range
        self.store = store
        self.stats = stats
        self.signal = signal
        self._poll_states: dict[str, _PollState] = {gid: _PollState() for gid in game_ids}
        self.cache: dict[str, RawSnapshot] = {}

    def _emit_stats(
        self,
        game_id: str,
        status_code: int,
        error_type: str | None,
        elapsed_ms: float,
    ) -> None:
        """Record a poll result to stats collector and quality signal."""
        if self.stats is None and self.signal is None:
            return
        result = PollResult(
            game_id=game_id,
            status_code=status_code,
            error_type=error_type,
            response_time_ms=elapsed_ms,
            timestamp=time.time(),
        )
        if self.stats is not None:
            self.stats.record(result)
        if self.signal is not None:
            self.signal.on_poll(result)

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
        t0 = time.time()
        try:
            resp = await client.get(url)
            elapsed_ms = (time.time() - t0) * 1000
            state.last_polled = time.time()

            self._emit_stats(game_id, resp.status_code, None, elapsed_ms)

            if resp.status_code == 403:
                state.status = GameStatus.NOT_STARTED
                return

            resp.raise_for_status()

            payload = resp.json()
            game_status = payload.get("game", {}).get("gameStatus", 2)

            if game_status == 3:
                state.status = GameStatus.FINAL
                if self.store:
                    date_str = datetime.now().strftime("%Y-%m-%d")
                    try:
                        await self.store.compact(game_id, date_str)
                        logger.info(f"Compacted snapshots for game {game_id}")
                    except Exception as e:
                        logger.error(f"Compact failed for {game_id}: {e}")
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
            if self.signal is not None:
                self.signal.on_snapshot(game_id, snapshot.fetched_at)

        except Exception as e:
            elapsed_ms = (time.time() - t0) * 1000
            self._emit_stats(game_id, -1, type(e).__name__, elapsed_ms)
            logger.error(f"Failed to fetch game {game_id}: {e}")
