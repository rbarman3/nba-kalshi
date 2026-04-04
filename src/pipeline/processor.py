"""Lineup diffing and score change detection.

Consumes RawSnapshot from transport layer.
Diffs consecutive snapshots to extract substitution and score change events.
Emits LineupChangeEvent and ScoreChangeEvent downstream.
"""
import asyncio
import logging
from typing import Optional

from .models import RawSnapshot, LineupChangeEvent, ScoreChangeEvent

logger = logging.getLogger(__name__)


class NBAProcessor:
    """Consumes RawSnapshot, emits LineupChangeEvent and ScoreChangeEvent to out_queue.

    Maintains per-game lineup and score state. First snapshot per game initializes
    state without emitting. Subsequent snapshots are diffed — events only emitted on change.
    """

    def __init__(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._state: dict[str, dict[str, frozenset[str]]] = {}
        self._score_state: dict[str, dict[str, int]] = {}

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
        is_first = game_id not in self._state

        # Lineup diffing
        curr_lineup = extract_lineup(snapshot)
        prev_lineup = self._state.get(game_id)

        if not is_first:
            lineup_event = diff_lineups(game_id, prev_lineup, curr_lineup, snapshot)
            if lineup_event is not None:
                logger.info("Lineup change: game=%s in=%s out=%s", game_id, lineup_event.players_in, lineup_event.players_out)
                await self.out_queue.put(lineup_event)

        self._state[game_id] = curr_lineup

        # Score diffing
        curr_scores = extract_scores(snapshot)
        prev_scores = self._score_state.get(game_id)

        if not is_first and prev_scores is not None:
            score_event = diff_scores(game_id, prev_scores, curr_scores, snapshot)
            if score_event is not None:
                logger.info("Score change: game=%s home=%d away=%d", game_id, score_event.home_score, score_event.away_score)
                await self.out_queue.put(score_event)

        self._score_state[game_id] = curr_scores


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


def extract_scores(snapshot: RawSnapshot) -> dict[str, int]:
    """Extract current scores from a raw snapshot.

    Args:
        snapshot: RawSnapshot containing boxscore payload.

    Returns:
        Dict with 'home' and 'away' keys, each an int score.
        Defaults to 0 if score fields are missing.
    """
    game = snapshot.payload.get("game", {})
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    return {"home": home_score, "away": away_score}


def diff_scores(
    game_id: str,
    prev_scores: dict[str, int],
    curr_scores: dict[str, int],
    snapshot: RawSnapshot,
) -> Optional[ScoreChangeEvent]:
    """Diff consecutive scores and emit event if either changed.

    Args:
        game_id: Game identifier.
        prev_scores: Previous score state from extract_scores().
        curr_scores: Current score state from extract_scores().
        snapshot: Current RawSnapshot (for period/clock/timestamp).

    Returns:
        ScoreChangeEvent if either score changed, None otherwise.
    """
    if prev_scores == curr_scores:
        return None

    game = snapshot.payload.get("game", {})
    period = game.get("period")
    clock = game.get("gameClock", "PT00M00.00S")

    return ScoreChangeEvent(
        game_id=game_id,
        home_score=curr_scores["home"],
        away_score=curr_scores["away"],
        home_prev=prev_scores["home"],
        away_prev=prev_scores["away"],
        period=period,
        clock=clock,
        observed_at=snapshot.fetched_at,
    )
