"""Lineup diffing, score change detection, and play-by-play action parsing.

Consumes RawSnapshot from transport layer.
Diffs consecutive snapshots to extract substitution and score change events.
Parses game.actions[] to emit fine-grained play events.
Emits LineupChangeEvent, ScoreChangeEvent, and action events downstream.
"""
import asyncio
import logging
from typing import Optional, Any

from .models import (
    RawSnapshot,
    LineupChangeEvent,
    ScoreChangeEvent,
    ScoringPlayEvent,
    FoulEvent,
    TimeoutEvent,
    TurnoverEvent,
    PeriodEvent,
    SubstitutionEvent,
)

logger = logging.getLogger(__name__)

# actionType values that produce ScoringPlayEvent
_SCORING_TYPES = frozenset({"2pt", "3pt", "freethrow"})


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
        self._action_state: dict[str, int] = {}  # game_id → last processed actionNumber

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

        # Action parsing — emit fine-grained play events from game.actions[]
        last_action = self._action_state.get(game_id, -1)
        new_actions = extract_new_actions(snapshot, last_action)

        for action in new_actions:
            event = classify_action(game_id, action, snapshot.fetched_at)
            if event is not None:
                await self.out_queue.put(event)

        if new_actions:
            self._action_state[game_id] = new_actions[-1].get("actionNumber", last_action)


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


def extract_new_actions(snapshot: RawSnapshot, last_action_number: int) -> list[dict]:
    """Extract actions from snapshot with actionNumber > last_action_number.

    Actions are monotonically numbered by the NBA CDN — comparing against the
    highest previously-seen number ensures each action is emitted exactly once
    across consecutive snapshot polls.

    Args:
        snapshot: RawSnapshot containing the full boxscore payload.
        last_action_number: Highest actionNumber already processed (-1 = none).

    Returns:
        List of new action dicts sorted ascending by actionNumber.
    """
    actions = snapshot.payload.get("game", {}).get("actions", [])
    new_actions = [
        a for a in actions
        if a.get("actionNumber", 0) > last_action_number
    ]
    return sorted(new_actions, key=lambda a: a.get("actionNumber", 0))


def classify_action(game_id: str, action: dict, observed_at: float) -> Optional[Any]:
    """Convert a single NBA CDN action dict into a typed event.

    Handles: scoring plays, fouls, timeouts, turnovers, period markers,
    and substitutions. Unknown action types are silently skipped.

    Args:
        game_id: Game identifier.
        action: Action dict from game.actions[].
        observed_at: Unix timestamp when the snapshot was fetched.

    Returns:
        A typed event dataclass, or None if actionType is unrecognized.
    """
    action_type = action.get("actionType", "")
    action_number = action.get("actionNumber", 0)
    period = action.get("period", 0)
    clock = action.get("clock", "PT00M00.00S")
    home_score = int(action.get("scoreHome") or 0)
    away_score = int(action.get("scoreAway") or 0)
    description = action.get("description", "")
    team_id = int(action.get("teamId") or 0)
    player_id = int(action.get("personId") or 0)
    sub_type = action.get("subType", "")

    if action_type in _SCORING_TYPES and action.get("shotResult") == "Made":
        raw_points = action.get("pointsTotal")
        if raw_points is not None:
            score_value = int(raw_points)
        else:
            score_value = {"2pt": 2, "3pt": 3, "freethrow": 1}.get(action_type, 2)
        return ScoringPlayEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            score_value=score_value,
            team_id=team_id,
            player_id=player_id,
            action_type=action_type,
            sub_type=sub_type,
            description=description,
            observed_at=observed_at,
        )

    if action_type == "foul":
        return FoulEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            team_id=team_id,
            player_id=player_id,
            foul_type=sub_type or "personal",
            description=description,
            observed_at=observed_at,
        )

    if action_type == "timeout":
        return TimeoutEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            team_id=team_id,
            timeout_type=sub_type or "full",
            description=description,
            observed_at=observed_at,
        )

    if action_type == "turnover":
        return TurnoverEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            team_id=team_id,
            player_id=player_id,
            turnover_type=sub_type or "bad pass",
            description=description,
            observed_at=observed_at,
        )

    if action_type == "period":
        event_type = "start" if sub_type.lower() == "start" else "end"
        return PeriodEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            event_type=event_type,
            description=description,
            observed_at=observed_at,
        )

    if action_type == "substitution":
        return SubstitutionEvent(
            game_id=game_id,
            action_number=action_number,
            period=period,
            clock=clock,
            home_score=home_score,
            away_score=away_score,
            team_id=team_id,
            player_id=player_id,
            sub_type=sub_type.lower() or "in",
            description=description,
            observed_at=observed_at,
        )

    logger.debug("Skipping unrecognized action type: %s", action_type)
    return None
