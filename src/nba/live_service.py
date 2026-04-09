"""
Live game data services backed by the nba_api live endpoints.

Public API
----------
get_live_scoreboard()       -- today's games with current scores (network)

Mock targets: ``nba.live_service.live_scoreboard.ScoreBoard``
"""
from nba_api.live.nba.endpoints import scoreboard as live_scoreboard

from nba.models import GameSummary


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


def get_live_scoreboard() -> list[GameSummary]:
    """Return today's games with live scores from the NBA API."""
    board = live_scoreboard.ScoreBoard()
    games = board.games.get_dict()
    return [_game_summary_from_dict(g) for g in games]
