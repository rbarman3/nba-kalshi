"""FastAPI server exposing NBA service functions as HTTP endpoints."""
from dataclasses import asdict

import fastapi

from nba.live_service import get_live_lineup, get_live_scoreboard
from nba.player_service import find_players_by_name, find_players_by_team

app = fastapi.FastAPI(title="NBA Live Scores API")


@app.get("/scoreboard")
def scoreboard():
    games = get_live_scoreboard()
    return {"games": [asdict(g) for g in games]}


@app.get("/lineup/{game_id}")
def lineup(game_id: str):
    try:
        home, away = get_live_lineup(game_id)
    except Exception as e:
        raise fastapi.HTTPException(status_code=502, detail=str(e))
    return {"home": [asdict(p) for p in home], "away": [asdict(p) for p in away]}


@app.get("/players")
def players_by_name(name: str):
    try:
        results = find_players_by_name(name)
    except ValueError as e:
        raise fastapi.HTTPException(status_code=422, detail=str(e))
    return {"players": [asdict(p) for p in results]}


@app.get("/players/team")
def players_by_team(name: str):
    try:
        results = find_players_by_team(name)
    except ValueError as e:
        raise fastapi.HTTPException(status_code=422, detail=str(e))
    return {"players": [asdict(p) for p in results]}


def main():
    import uvicorn
    uvicorn.run("nba.server:app", host="0.0.0.0", port=8000, reload=False)
