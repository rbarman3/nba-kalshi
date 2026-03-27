# CLAUDE.md

This file provides guidance to Claude Code when working with this repository. You are building an NBA Kalshi in-game trading system that discovers arbitrage opportunities in real time and executes trades on Kalshi markets. Do NOT expose Kalshi API keys or private keys.

## Commands

```bash
# Install all dependencies (including dev)
pip install -e ".[dev]"

# Run all tests with coverage
PYTHONPATH=src python3 -m pytest

# Run a specific test file
PYTHONPATH=src python3 -m pytest tests/test_player_service.py -v

# Run only integration tests (real NBA API, no mocking)
PYTHONPATH=src python3 -m pytest tests/test_server_integration.py tests/test_player_to_service_integration.py tests/test_cli_integration.py -v

# Start the HTTP server (data layer only)
nba-server
# or
uvicorn nba.server:app --reload --port 8000

# Use the CLI (scoreboard only)
nba-scores scores

# Run the trading pipeline debug script
PYTHONPATH=src python3 scripts/debug_transport.py
PYTHONPATH=src python3 scripts/debug_transport.py --max-snapshots 20 --max-seconds 60
```

## Architecture

The codebase is split into three concerns:

**Data Layer** (`src/nba/`):
```
models.py          # GameSummary, Player, Team (frozen dataclasses)
live_service.py    # get_live_scoreboard() — scoreboard + schedule
player_service.py  # find_players_by_name(), find_players_by_team()
server.py          # FastAPI: /scoreboard, /players, /players/team endpoints
cli.py             # Typer: scores command only
```

**Trading Pipeline** (`src/pipeline/`):
```
models.py          # RawSnapshot (game_id, payload: dict, fetched_at: float)
transport.py       # Layer 1: NBATransport — async poll + queue-based delivery
```
(Layers 2–4 — processor, strategy, execution — not yet built)

**Exchange Client** (`src/kalshi/`):
```
client.py          # RSA-signed HTTP client (prod/demo environments)
markets.py         # NBA market discovery
orders.py          # Limit order placement
```

**Debug Tools** (`scripts/`):
```
debug_transport.py # Print live snapshots from NBATransport
```

## Data Flow

### Query Path (Data Layer)
```
HTTP client / curl / browser
        │
        ▼
  FastAPI app  (src/nba/server.py)         nba-scores CLI  (src/nba/cli.py)
        │                                          │
        ├─► live_service.get_live_scoreboard()     │
        ├─► player_service.find_players_by_name()  └─► live_service.*
        └─► player_service.find_players_by_team()       player_service.*
        │
        ▼
  nba_api.stats.static  (local dict, no network)
  nba_api.stats.endpoints.CommonTeamRoster  (NBA API — network)
  nba_api.live.nba.endpoints.ScoreBoard     (NBA live API — network)
```

### Trading Pipeline (async)
```
asyncio event loop (periodic polls)
        │
        ├─► NBATransport (per game, concurrent via asyncio.gather)
        │     GET https://cdn.nba.com/static/json/liveData/boxscore/{game_id}.json
        │     Throttle: random.uniform(0.6, 1.2)s per cycle
        │     Jitter: avoid Akamai rate-limit blocks
        │
        ├─► RawSnapshot → asyncio.Queue (per game)
        │     game_id, payload (full JSON), fetched_at (staleness gate)
        │
        ├─► [Layer 2: NBAProcessor — diff snapshots, emit LineupChangeEvent]
        ├─► [Layer 3: LineupStrategy — binary state, buy/sell signals]
        └─► [Layer 4: KalshiExecutor — submit orders, track positions]
```

## HTTP Endpoints

| Method | Path | Description | Returns |
|--------|------|-------------|---------|
| `GET` | `/scoreboard` | Today's games with live scores | `{"games": [...]}` |
| `GET` | `/players?name=` | Search players by name (partial, case-insensitive) | `{"players": [...]}` |
| `GET` | `/players/team?name=` | Roster for a team by full name | `{"players": [...]}` |

OpenAPI docs: `http://localhost:8000/docs`

## Models

**Data Layer** (`src/nba/models.py`):
- `GameSummary` — game_id, home_team, away_team, home_score, away_score, status, period, clock
- `Player` — id, full_name, first_name, last_name, is_active
- `Team` — id, full_name, abbreviation, nickname, city, state, year_founded

**Pipeline** (`src/pipeline/models.py`):
- `RawSnapshot` — game_id, payload: dict, fetched_at: float (raw CDN response + fetch timestamp)

All models are `frozen=True` dataclasses (immutable).

## Test Strategy

**Unit tests** (mocked, fast, no network):
- `test_player_service.py` — player lookup (mocked `nba_api.stats.static`)
- `test_roster_helper.py` — `_get_roster_for_team` (mocked `CommonTeamRoster`)
- `test_live_service.py` — scoreboard parsing (mocked `ScoreBoard`)
- `test_transport.py` — NBATransport (mocked `httpx.AsyncClient`)

Mock targets:
- `nba.player_service.players_static.find_players_by_full_name`
- `nba.player_service.teams_static.find_teams_by_full_name`
- `nba.player_service._get_roster_for_team`
- `nba.player_service.commonteamroster.CommonTeamRoster`
- `nba.live_service.live_scoreboard.ScoreBoard`
- `pipeline.transport.httpx.AsyncClient`

**Integration tests** (real NBA API, network required):
- `test_server_integration.py` — HTTP endpoints via `fastapi.testclient.TestClient`
- `test_player_to_service_integration.py` — service functions directly
- `test_cli_integration.py` — CLI via `typer.testing.CliRunner`
- Tests skip with `pytest.skip()` when no games are live

## Key Design Decisions

- `nba_api.stats.static` (players/teams) is a local dictionary — safe to use in tests without mocking.
- `NBATransport` uses `httpx.AsyncClient` directly (not `nba_api`) for full async control + custom headers.
- Polling throttle is 0.6–1.2s random jitter per cycle; concurrent games use `asyncio.gather()` with independent connections (no shared rate limit).
- `RawSnapshot` lives in `pipeline/models.py` because it is a pipeline concern, not a data layer concern.
- Headers include Chrome 145 User-Agent + Sec-Ch-Ua to pass Akamai CDN fingerprinting on `cdn.nba.com`. Update version numbers when Chrome stable ships.
- Models are immutable (`frozen=True`); never mutate, always return new instances.
- `server.py` wraps `ValueError` as HTTP 422; unexpected errors as HTTP 502.
