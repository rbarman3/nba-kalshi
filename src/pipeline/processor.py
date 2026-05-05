"""Snapshot-diff event detection for the NBA CDN pipeline.

Consumes RawSnapshot from transport layer.
Diffs consecutive snapshots to detect:
  - Lineup changes (substitutions)
  - Score changes
  - Fouls (per-player foulsPersonal increase)
  - Timeouts (per-team timeoutsRemaining decrease)
  - Turnovers (per-player turnovers increase)
  - Period changes (game.period increase)
  - Scoring plays (per-player points increase)
  - Substitutions (per-player in/out with team context)
"""
import asyncio
import dataclasses
import logging
import time
from typing import Optional

from .models import (
    RawSnapshot,
    LineupChangeEvent,
    ScoreChangeEvent,
    ScoringPlayEvent,
    SubstitutionEvent,
    FoulEvent,
    TimeoutEvent,
    TurnoverEvent,
    PeriodEvent,
)

logger = logging.getLogger(__name__)


class NBAProcessor:
    """Consumes RawSnapshot, emits events to out_queue by diffing consecutive snapshots.

    Maintains per-game state for each diff dimension. First snapshot per game
    initializes state without emitting. Subsequent snapshots are diffed.
    """

    def __init__(self, in_queue: asyncio.Queue, out_queue: asyncio.Queue) -> None:
        self.in_queue = in_queue
        self.out_queue = out_queue
        self._lineup_state: dict[str, dict[str, frozenset[str]]] = {}
        self._score_state: dict[str, dict[str, int]] = {}
        self._timeout_state: dict[str, dict[str, dict]] = {}
        self._foul_state: dict[str, dict[str, dict[str, dict]]] = {}
        self._turnover_state: dict[str, dict[str, dict[str, dict]]] = {}
        self._period_state: dict[str, dict] = {}
        self._points_state: dict[str, dict[str, dict[str, dict]]] = {}
        self._roster_state: dict[str, dict[str, dict[str, dict]]] = {}

    async def run(self) -> None:
        """Consume snapshots forever. Never returns."""
        while True:
            snapshot = await self.in_queue.get()
            await self._process(snapshot)

    async def run_once(self) -> None:
        """Consume one snapshot. Used in tests."""
        snapshot = await self.in_queue.get()
        await self._process(snapshot)

    def _stamp(self, event, snapshot: RawSnapshot):
        """Stamp event with cdn_observed_at (passthrough) and emitted_at (now).

        Returns a new immutable event. No-op if event lacks these fields (defensive).
        """
        try:
            return dataclasses.replace(
                event,
                cdn_observed_at=snapshot.cdn_observed_at,
                emitted_at=time.time(),
            )
        except TypeError:
            return event

    async def _emit(self, event, snapshot: RawSnapshot) -> None:
        await self.out_queue.put(self._stamp(event, snapshot))

    async def _process(self, snapshot: RawSnapshot) -> None:
        game_id = snapshot.game_id
        is_first = game_id not in self._lineup_state

        # Lineup diffing
        curr_lineup = extract_lineup(snapshot)
        prev_lineup = self._lineup_state.get(game_id)

        if not is_first and prev_lineup is not None:
            lineup_event = diff_lineups(game_id, prev_lineup, curr_lineup, snapshot)
            if lineup_event is not None:
                logger.info("Lineup change: game=%s in=%s out=%s", game_id, lineup_event.players_in, lineup_event.players_out)
                await self._emit(lineup_event, snapshot)

        self._lineup_state[game_id] = curr_lineup

        # Score diffing
        curr_scores = extract_scores(snapshot)
        prev_scores = self._score_state.get(game_id)

        if not is_first and prev_scores is not None:
            score_event = diff_scores(game_id, prev_scores, curr_scores, snapshot)
            if score_event is not None:
                logger.info("Score change: game=%s home=%d away=%d", game_id, score_event.home_score, score_event.away_score)
                await self._emit(score_event, snapshot)

        self._score_state[game_id] = curr_scores

        # Timeout diffing
        curr_timeouts = extract_timeouts(snapshot)
        prev_timeouts = self._timeout_state.get(game_id)

        if not is_first and prev_timeouts is not None:
            for evt in diff_timeouts(game_id, prev_timeouts, curr_timeouts, snapshot):
                logger.info("Timeout: game=%s team=%s", game_id, evt.team_tricode)
                await self._emit(evt, snapshot)

        self._timeout_state[game_id] = curr_timeouts

        # Foul diffing
        curr_fouls = extract_fouls(snapshot)
        prev_fouls = self._foul_state.get(game_id)

        if not is_first and prev_fouls is not None:
            for evt in diff_fouls(game_id, prev_fouls, curr_fouls, snapshot):
                logger.info("Foul: game=%s player=%s fouls=%d", game_id, evt.player_name, evt.curr_fouls)
                await self._emit(evt, snapshot)

        self._foul_state[game_id] = curr_fouls

        # Turnover diffing
        curr_turnovers = extract_turnovers(snapshot)
        prev_turnovers = self._turnover_state.get(game_id)

        if not is_first and prev_turnovers is not None:
            for evt in diff_turnovers(game_id, prev_turnovers, curr_turnovers, snapshot):
                logger.info("Turnover: game=%s player=%s", game_id, evt.player_name)
                await self._emit(evt, snapshot)

        self._turnover_state[game_id] = curr_turnovers

        # Period diffing
        curr_period = extract_period(snapshot)
        prev_period = self._period_state.get(game_id)

        if not is_first and prev_period is not None:
            period_evt = diff_period(game_id, prev_period, curr_period, snapshot)
            if period_evt is not None:
                logger.info("Period change: game=%s %d→%d", game_id, period_evt.prev_period, period_evt.curr_period)
                await self._emit(period_evt, snapshot)

        self._period_state[game_id] = curr_period

        # Scoring play diffing (per-player points)
        curr_points = extract_player_points(snapshot)
        prev_points = self._points_state.get(game_id)

        if not is_first and prev_points is not None:
            for evt in diff_player_points(game_id, prev_points, curr_points, snapshot):
                logger.info("Scoring play: game=%s player=%s +%d", game_id, evt.player_name, evt.score_delta)
                await self._emit(evt, snapshot)

        self._points_state[game_id] = curr_points

        # Substitution diffing (per-player in/out with team context)
        curr_roster = extract_roster(snapshot)
        prev_roster = self._roster_state.get(game_id)

        if not is_first and prev_roster is not None:
            for evt in diff_substitutions(game_id, prev_roster, curr_roster, prev_lineup, curr_lineup, snapshot):
                logger.info("Substitution: game=%s %s %s %s", game_id, evt.sub_type, evt.player_name, evt.team_tricode)
                await self._emit(evt, snapshot)

        self._roster_state[game_id] = curr_roster


# ---------------------------------------------------------------------------
# Extract functions — pull structured state from a single RawSnapshot
# ---------------------------------------------------------------------------

def extract_lineup(snapshot: RawSnapshot) -> dict[str, frozenset[str]]:
    """Extract on-court player personIds from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each a frozenset of personIds on court.
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


def extract_scores(snapshot: RawSnapshot) -> dict[str, int]:
    """Extract current scores from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each an int score.
    """
    game = snapshot.payload.get("game", {})
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    return {"home": home_score, "away": away_score}


def extract_timeouts(snapshot: RawSnapshot) -> dict[str, dict]:
    """Extract per-team timeout counts from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each containing team_id, tricode, remaining.
    """
    game = snapshot.payload.get("game", {})
    result = {}
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = game.get(key, {})
        result[side] = {
            "team_id": team.get("teamId", 0),
            "tricode": team.get("teamTricode", ""),
            "remaining": team.get("timeoutsRemaining", 0),
        }
    return result


def extract_fouls(snapshot: RawSnapshot) -> dict[str, dict[str, dict]]:
    """Extract per-player foul counts from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each mapping player_id to
        {name, fouls, team_id, tricode}.
    """
    game = snapshot.payload.get("game", {})
    result = {}
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = game.get(key, {})
        team_id = team.get("teamId", 0)
        tricode = team.get("teamTricode", "")
        players = {}
        for p in team.get("players", []):
            pid = str(p.get("personId", ""))
            players[pid] = {
                "name": p.get("name", ""),
                "fouls": p.get("statistics", {}).get("foulsPersonal", 0),
                "team_id": team_id,
                "tricode": tricode,
            }
        result[side] = players
    return result


def extract_turnovers(snapshot: RawSnapshot) -> dict[str, dict[str, dict]]:
    """Extract per-player turnover counts from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each mapping player_id to
        {name, turnovers, team_id, tricode}.
    """
    game = snapshot.payload.get("game", {})
    result = {}
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = game.get(key, {})
        team_id = team.get("teamId", 0)
        tricode = team.get("teamTricode", "")
        players = {}
        for p in team.get("players", []):
            pid = str(p.get("personId", ""))
            players[pid] = {
                "name": p.get("name", ""),
                "turnovers": p.get("statistics", {}).get("turnovers", 0),
                "team_id": team_id,
                "tricode": tricode,
            }
        result[side] = players
    return result


def extract_period(snapshot: RawSnapshot) -> dict:
    """Extract current period and game status from a raw snapshot.

    Returns:
        Dict with 'period' and 'game_status' keys.
    """
    game = snapshot.payload.get("game", {})
    return {
        "period": game.get("period", 0),
        "game_status": game.get("gameStatus", 1),
    }


def extract_player_points(snapshot: RawSnapshot) -> dict[str, dict[str, dict]]:
    """Extract per-player points from a raw snapshot.

    Returns:
        Dict with 'home' and 'away' keys, each mapping player_id to
        {name, points, team_id, tricode}.
    """
    game = snapshot.payload.get("game", {})
    result = {}
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = game.get(key, {})
        team_id = team.get("teamId", 0)
        tricode = team.get("teamTricode", "")
        players = {}
        for p in team.get("players", []):
            pid = str(p.get("personId", ""))
            players[pid] = {
                "name": p.get("name", ""),
                "points": p.get("statistics", {}).get("points", 0),
                "team_id": team_id,
                "tricode": tricode,
            }
        result[side] = players
    return result


def extract_roster(snapshot: RawSnapshot) -> dict[str, dict[str, dict]]:
    """Extract player roster with oncourt status and identity info.

    Returns:
        Dict with 'home' and 'away' keys, each mapping player_id to
        {name, oncourt, team_id, tricode}.
    """
    game = snapshot.payload.get("game", {})
    result = {}
    for side, key in [("home", "homeTeam"), ("away", "awayTeam")]:
        team = game.get(key, {})
        team_id = team.get("teamId", 0)
        tricode = team.get("teamTricode", "")
        players = {}
        for p in team.get("players", []):
            pid = str(p.get("personId", ""))
            players[pid] = {
                "name": p.get("name", ""),
                "oncourt": p.get("oncourt") == "1",
                "team_id": team_id,
                "tricode": tricode,
            }
        result[side] = players
    return result


# ---------------------------------------------------------------------------
# Diff functions — compare consecutive extracted states, emit events
# ---------------------------------------------------------------------------

def diff_lineups(
    game_id: str,
    prev_lineup: dict[str, frozenset[str]],
    curr_lineup: dict[str, frozenset[str]],
    snapshot: RawSnapshot,
) -> Optional[LineupChangeEvent]:
    """Diff consecutive lineups and emit event if players changed."""
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


def diff_scores(
    game_id: str,
    prev_scores: dict[str, int],
    curr_scores: dict[str, int],
    snapshot: RawSnapshot,
) -> Optional[ScoreChangeEvent]:
    """Diff consecutive scores and emit event if either changed."""
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


def diff_timeouts(
    game_id: str,
    prev_timeouts: dict[str, dict],
    curr_timeouts: dict[str, dict],
    snapshot: RawSnapshot,
) -> list[TimeoutEvent]:
    """Diff consecutive timeout counts. Returns event for each team that used a timeout."""
    game = snapshot.payload.get("game", {})
    period = game.get("period", 0)
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    events = []
    for side in ("home", "away"):
        prev = prev_timeouts[side]
        curr = curr_timeouts[side]
        if curr["remaining"] < prev["remaining"]:
            events.append(TimeoutEvent(
                game_id=game_id,
                period=period,
                clock=clock,
                team_id=curr["team_id"],
                team_tricode=curr["tricode"],
                prev_timeouts=prev["remaining"],
                curr_timeouts=curr["remaining"],
                home_score=home_score,
                away_score=away_score,
                observed_at=snapshot.fetched_at,
            ))
    return events


def diff_fouls(
    game_id: str,
    prev_fouls: dict[str, dict[str, dict]],
    curr_fouls: dict[str, dict[str, dict]],
    snapshot: RawSnapshot,
) -> list[FoulEvent]:
    """Diff consecutive per-player foul counts. Returns event for each player who fouled."""
    game = snapshot.payload.get("game", {})
    period = game.get("period", 0)
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    events = []
    for side in ("home", "away"):
        prev_players = prev_fouls[side]
        curr_players = curr_fouls[side]
        for pid, curr_data in curr_players.items():
            if pid not in prev_players:
                continue
            prev_count = prev_players[pid]["fouls"]
            curr_count = curr_data["fouls"]
            if curr_count > prev_count:
                events.append(FoulEvent(
                    game_id=game_id,
                    period=period,
                    clock=clock,
                    team_id=curr_data["team_id"],
                    team_tricode=curr_data["tricode"],
                    player_id=pid,
                    player_name=curr_data["name"],
                    prev_fouls=prev_count,
                    curr_fouls=curr_count,
                    home_score=home_score,
                    away_score=away_score,
                    observed_at=snapshot.fetched_at,
                ))
    return events


def diff_turnovers(
    game_id: str,
    prev_turnovers: dict[str, dict[str, dict]],
    curr_turnovers: dict[str, dict[str, dict]],
    snapshot: RawSnapshot,
) -> list[TurnoverEvent]:
    """Diff consecutive per-player turnover counts. Returns event for each player who turned over."""
    game = snapshot.payload.get("game", {})
    period = game.get("period", 0)
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    events = []
    for side in ("home", "away"):
        prev_players = prev_turnovers[side]
        curr_players = curr_turnovers[side]
        for pid, curr_data in curr_players.items():
            if pid not in prev_players:
                continue
            prev_count = prev_players[pid]["turnovers"]
            curr_count = curr_data["turnovers"]
            if curr_count > prev_count:
                events.append(TurnoverEvent(
                    game_id=game_id,
                    period=period,
                    clock=clock,
                    team_id=curr_data["team_id"],
                    team_tricode=curr_data["tricode"],
                    player_id=pid,
                    player_name=curr_data["name"],
                    prev_turnovers=prev_count,
                    curr_turnovers=curr_count,
                    home_score=home_score,
                    away_score=away_score,
                    observed_at=snapshot.fetched_at,
                ))
    return events


def diff_period(
    game_id: str,
    prev_period: dict,
    curr_period: dict,
    snapshot: RawSnapshot,
) -> Optional[PeriodEvent]:
    """Diff consecutive periods. Returns event if period increased."""
    if curr_period["period"] <= prev_period["period"]:
        return None

    game = snapshot.payload.get("game", {})
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    return PeriodEvent(
        game_id=game_id,
        prev_period=prev_period["period"],
        curr_period=curr_period["period"],
        clock=clock,
        home_score=home_score,
        away_score=away_score,
        game_status=curr_period["game_status"],
        observed_at=snapshot.fetched_at,
    )


def diff_player_points(
    game_id: str,
    prev_points: dict[str, dict[str, dict]],
    curr_points: dict[str, dict[str, dict]],
    snapshot: RawSnapshot,
) -> list[ScoringPlayEvent]:
    """Diff consecutive per-player points. Returns event for each player whose points increased."""
    game = snapshot.payload.get("game", {})
    period = game.get("period", 0)
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    events = []
    for side in ("home", "away"):
        prev_players = prev_points[side]
        curr_players = curr_points[side]
        for pid, curr_data in curr_players.items():
            if pid not in prev_players:
                continue
            prev_count = prev_players[pid]["points"]
            curr_count = curr_data["points"]
            if curr_count > prev_count:
                events.append(ScoringPlayEvent(
                    game_id=game_id,
                    period=period,
                    clock=clock,
                    team_id=curr_data["team_id"],
                    team_tricode=curr_data["tricode"],
                    player_id=pid,
                    player_name=curr_data["name"],
                    prev_points=prev_count,
                    curr_points=curr_count,
                    score_delta=curr_count - prev_count,
                    home_score=home_score,
                    away_score=away_score,
                    observed_at=snapshot.fetched_at,
                ))
    return events


def diff_substitutions(
    game_id: str,
    prev_roster: dict[str, dict[str, dict]],
    curr_roster: dict[str, dict[str, dict]],
    prev_lineup: dict[str, frozenset[str]],
    curr_lineup: dict[str, frozenset[str]],
    snapshot: RawSnapshot,
) -> list[SubstitutionEvent]:
    """Emit per-player SubstitutionEvent for each oncourt change, with team context."""
    prev_all = prev_lineup["home"] | prev_lineup["away"]
    curr_all = curr_lineup["home"] | curr_lineup["away"]

    players_in = curr_all - prev_all
    players_out = prev_all - curr_all

    if not players_in and not players_out:
        return []

    game = snapshot.payload.get("game", {})
    period = game.get("period", 0)
    clock = game.get("gameClock", "PT00M00.00S")
    home_score = game.get("homeTeam", {}).get("score", 0)
    away_score = game.get("awayTeam", {}).get("score", 0)

    # Build a flat lookup from both teams in curr_roster
    all_players = {}
    for side in ("home", "away"):
        all_players.update(curr_roster[side])
    # Also include prev_roster for players going out
    for side in ("home", "away"):
        for pid, data in prev_roster[side].items():
            if pid not in all_players:
                all_players[pid] = data

    events = []
    for pid in sorted(players_in):
        info = all_players.get(str(pid), {})
        events.append(SubstitutionEvent(
            game_id=game_id,
            period=period,
            clock=clock,
            team_id=info.get("team_id", 0),
            team_tricode=info.get("tricode", ""),
            player_id=str(pid),
            player_name=info.get("name", ""),
            sub_type="in",
            home_score=home_score,
            away_score=away_score,
            observed_at=snapshot.fetched_at,
        ))
    for pid in sorted(players_out):
        info = all_players.get(str(pid), {})
        events.append(SubstitutionEvent(
            game_id=game_id,
            period=period,
            clock=clock,
            team_id=info.get("team_id", 0),
            team_tricode=info.get("tricode", ""),
            player_id=str(pid),
            player_name=info.get("name", ""),
            sub_type="out",
            home_score=home_score,
            away_score=away_score,
            observed_at=snapshot.fetched_at,
        ))
    return events
