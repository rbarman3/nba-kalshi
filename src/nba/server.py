"""FastAPI server exposing NBA service functions as HTTP endpoints."""
import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

import fastapi

from nba.live_service import get_live_scoreboard
from nba.player_service import find_players_by_name, find_players_by_team

app = fastapi.FastAPI(title="NBA Live Scores API")


@app.get("/scoreboard")
def scoreboard():
    games = get_live_scoreboard()
    return {"games": [asdict(g) for g in games]}


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


SLO_DIR = Path(os.getenv("SLO_DIR", "data/slo"))


def _slo_path() -> Path:
    return SLO_DIR / f"{datetime.now().strftime('%Y-%m-%d')}.jsonl"


def _read_latest_slo() -> dict:
    """Return latest SLO snapshot per game from today's JSONL.

    File format: each line is {"ts": float, "game_id": str, "stats": {...}}.
    Latest line per game wins.
    """
    path = _slo_path()
    if not path.exists():
        return {}
    latest: dict[str, dict] = {}
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            gid = row.get("game_id")
            if not gid:
                continue
            prev = latest.get(gid)
            if prev is None or row.get("ts", 0) >= prev.get("ts", 0):
                latest[gid] = row
    return latest


@app.get("/slo")
def slo_snapshot():
    """Latest per-game SLO percentiles. Sourced from today's data/slo/*.jsonl."""
    latest = _read_latest_slo()
    return {
        "as_of": datetime.now().isoformat(),
        "source": str(_slo_path()),
        "games": latest,
    }


@app.get("/slo/{game_id}")
def slo_for_game(game_id: str):
    latest = _read_latest_slo()
    if game_id not in latest:
        raise fastapi.HTTPException(status_code=404, detail=f"no SLO data for game {game_id}")
    return latest[game_id]


def main():
    import uvicorn
    uvicorn.run("nba.server:app", host="0.0.0.0", port=8000, reload=False)
