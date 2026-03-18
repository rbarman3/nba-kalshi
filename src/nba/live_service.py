"""
Live game data services backed by the nba_api live endpoints.

Public API
----------
get_live_scoreboard()       -- today's games with current scores (network)
get_live_lineup(game_id)    -- players currently on court for one game (network)

Mock targets: ``nba.live_service.live_scoreboard.ScoreBoard``, ``nba.live_service.live_boxscore.BoxScore``
"""
from nba_api.live.nba.endpoints import boxscore as live_boxscore
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

from nba.models import GameSummary, PlayerOnCourt


def _game_summary_from_dict(game: dict) -> GameSummary:
    home = game["homeTeam"]
    away = game["awayTeam"]
    return GameSummary(
        game_id=game["gameId"],
        home_team=f"{home['teamCity']} {home['teamName']}",
        away_team=f"{away['teamCity']} {away['teamName']}",
        home_score=home.get("score", 0),
        away_score=away.get("score", 0),
        status=game.get("gameStatusText", "").strip(),
        period=game.get("period", 0),
        clock=game.get("gameClock", ""),
    )


def _on_court_players(players: list[dict]) -> list[PlayerOnCourt]:
    return [
        PlayerOnCourt(
            name=p["name"],
            jersey_num=p.get("jerseyNum", ""),
            position=p.get("position", ""),
            points=p["statistics"].get("points", 0),
            assists=p["statistics"].get("assists", 0),
            rebounds=p["statistics"].get("reboundsTotal", 0),
        )
        for p in players
        if p.get("oncourt") == "1"
    ]


def get_live_lineup(game_id: str) -> tuple[list[PlayerOnCourt], list[PlayerOnCourt]]:
    """Return (home_players, away_players) currently on the court."""
    box = live_boxscore.BoxScore(game_id=game_id)
    home = _on_court_players(box.home_team_player_stats.get_dict())
    away = _on_court_players(box.away_team_player_stats.get_dict())
    return home, away


def get_live_scoreboard() -> list[GameSummary]:
    """Return today's games with live scores from the NBA API."""
    board = live_scoreboard.ScoreBoard()
    games = board.games.get_dict()
    return [_game_summary_from_dict(g) for g in games]
