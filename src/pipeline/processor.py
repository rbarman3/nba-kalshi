"""Lineup diffing and event emission.

Consumes RawSnapshot from transport layer.
Diffs consecutive snapshots to extract substitution events.
Emits LineupChangeEvent downstream.
"""
import asyncio
import logging
from typing import Optional

from .models import RawSnapshot, LineupChangeEvent

logger = logging.getLogger(__name__)


class NBAProcessor:
    """Consumes RawSnapshot from in_queue, emits LineupChangeEvent to out_queue.

    Maintains per-game lineup state. First snapshot per game initializes state
    without emitting. Subsequent snapshots are diffed — events only emitted on change.
    """

    def __init__(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._state: dict[str, dict[str, frozenset[str]]] = {}

    async def run(self) -> None:
        """Consume snapshots forever. Never returns."""
        while True:
            snapshot = await self.in_queue.get()
            await self._process(snapshot)

    async def run_once(self) -> None:
        """Consume one snapshot. Used in tests."""
        snapshot = await self.in_queue.get()
        await self._process(snapshot)

    async def _process(self, snapshot: RawSnapshot) -> None:
        game_id = snapshot.game_id
        curr = extract_lineup(snapshot)
        prev = self._state.get(game_id)

        if prev is None:
            self._state[game_id] = curr
            return

        event = diff_lineups(game_id, prev, curr, snapshot)
        self._state[game_id] = curr

        if event is not None:
            logger.info("Lineup change: game=%s in=%s out=%s", game_id, event.players_in, event.players_out)
            await self.out_queue.put(event)


def extract_lineup(snapshot: RawSnapshot) -> dict[str, frozenset[str]]:
    """Extract on-court player personIds from a raw snapshot.

    Args:
        snapshot: RawSnapshot containing full boxscore payload.

    Returns:
        Dict with 'home' and 'away' keys, each a frozenset of personIds
        currently on court.
    """
    game = snapshot.payload.get("game", {})
    home_players = game.get("homeTeam", {}).get("players", [])
    away_players = game.get("awayTeam", {}).get("players", [])

    home_oncourt = frozenset(
        p["personId"] for p in home_players if p.get("oncourt") == "1"
    )
    away_oncourt = frozenset(
        p["personId"] for p in away_players if p.get("oncourt") == "1"
    )

    return {"home": home_oncourt, "away": away_oncourt}


def diff_lineups(
    game_id: str,
    prev_lineup: dict[str, frozenset[str]],
    curr_lineup: dict[str, frozenset[str]],
    snapshot: RawSnapshot,
) -> Optional[LineupChangeEvent]:
    """Diff consecutive lineups and emit event if players changed.

    Args:
        game_id: Game identifier.
        prev_lineup: Previous lineup state from extract_lineup().
        curr_lineup: Current lineup state from extract_lineup().
        snapshot: Current RawSnapshot (for clock/period/timestamp).

    Returns:
        LineupChangeEvent if lineup changed, None otherwise.
    """
    prev_all = prev_lineup["home"] | prev_lineup["away"]
    curr_all = curr_lineup["home"] | curr_lineup["away"]

    players_in = curr_all - prev_all
    players_out = prev_all - curr_all

    if not players_in and not players_out:
        return None

    game = snapshot.payload.get("game", {})
    period = game.get("period")
    clock = game.get("gameClock", "PT00M00.00S")

    return LineupChangeEvent(
        game_id=game_id,
        period=period,
        clock=clock,
        players_in=players_in,
        players_out=players_out,
        observed_at=snapshot.fetched_at,
    )
