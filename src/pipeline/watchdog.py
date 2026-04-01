"""Feed health watchdog.

Monitors NBATransport cache staleness and emits FeedHealthEvent
when per-game or overall health status changes.
"""
import asyncio
import time

from .models import FeedHealthEvent


class FeedWatchdog:
    """Watches transport cache staleness, emits health events on status change."""

    DEGRADED_THRESHOLD = 5.0   # seconds since last success → DEGRADED
    DEAD_THRESHOLD = 30.0      # seconds since last success → DEAD

    def __init__(self, transport, health_queue: asyncio.Queue) -> None:
        self.transport = transport
        self.health_queue = health_queue
        self._game_statuses: dict[str, str] = {}
        self._overall_status: str | None = None

    async def run(self) -> None:
        """Check health every second forever. Never returns."""
        while True:
            await self.run_once()
            await asyncio.sleep(1.0)

    async def run_once(self) -> None:
        """Single health check — used in tests."""
        self._check_health()

    def _classify_game(self, game_id: str, now: float) -> str:
        """Return HEALTHY / DEGRADED / DEAD for one game based on cache staleness."""
        if game_id not in self.transport.cache:
            return "DEAD"
        age = now - self.transport.cache[game_id].fetched_at
        if age < self.DEGRADED_THRESHOLD:
            return "HEALTHY"
        if age < self.DEAD_THRESHOLD:
            return "DEGRADED"
        return "DEAD"

    def _classify_overall(self, statuses: list[str]) -> str:
        """Best-wins: HEALTHY > DEGRADED > DEAD."""
        if "HEALTHY" in statuses:
            return "HEALTHY"
        if "DEGRADED" in statuses:
            return "DEGRADED"
        return "DEAD"

    def _check_health(self) -> None:
        """Emit FeedHealthEvent for any game or overall status that changed."""
        now = time.time()
        game_statuses = []

        for game_id in self.transport.game_ids:
            status = self._classify_game(game_id, now)
            game_statuses.append(status)

            if self._game_statuses.get(game_id) != status:
                last_success = (
                    self.transport.cache[game_id].fetched_at
                    if game_id in self.transport.cache
                    else 0.0
                )
                self.health_queue.put_nowait(FeedHealthEvent(
                    status=status,
                    game_id=game_id,
                    last_success=last_success,
                    observed_at=now,
                    message=f"Game {game_id} feed is {status}",
                ))
                self._game_statuses = {**self._game_statuses, game_id: status}

        overall = self._classify_overall(game_statuses)
        if self._overall_status != overall:
            self.health_queue.put_nowait(FeedHealthEvent(
                status=overall,
                game_id=None,
                last_success=now if overall == "HEALTHY" else 0.0,
                observed_at=now,
                message=f"Overall feed is {overall}",
            ))
            self._overall_status = overall
