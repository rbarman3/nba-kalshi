# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install all dependencies (including dev)
pip install -e ".[dev]"

# Run all tests with coverage
PYTHONPATH=src python3 -m pytest

# Run a specific test file
PYTHONPATH=src python3 -m pytest tests/test_player_service.py -v

# Run a specific test by name
PYTHONPATH=src python3 -m pytest tests/test_player_service.py::TestFindPlayersByName::test_exact_full_name_returns_player -v

# Run only integration tests (real NBA API, no mocking)
PYTHONPATH=src python3 -m pytest tests/test_server_integration.py tests/test_player_to_service_integration.py tests/test_cli_integration.py -v

# Start the HTTP server
nba-server
# or
uvicorn nba.server:app --reload --port 8000

# Use the CLI
nba-scores scores
nba-scores lineup <game_id>
nba-scores watch [game_id] [--interval 30]
```

## Architecture

```
src/nba/
  models.py          # Frozen dataclasses: GameSummary, Player, PlayerOnCourt, Team
  player_service.py  # find_players_by_name(), find_players_by_team()
  live_service.py    # get_live_scoreboard(), get_live_lineup(game_id)
  server.py          # FastAPI app (4 HTTP endpoints) + main() entry point
  cli.py             # Typer CLI: scores, lineup, watch commands
  poller.py          # Background polling thread used by `watch`

tests/
  test_player_service.py            # Unit tests — player lookup (all mocked)
  test_roster_helper.py             # Unit tests — _get_roster_for_team helper (mocked)
  test_live_service.py              # Unit tests — scoreboard/lineup parsing (mocked)
  test_cli.py                       # Unit tests — CLI commands (mocked)
  test_poller.py                    # Unit tests — Poller class
  test_player_to_service_integration.py  # Integration tests — player service vs real API
  test_cli_integration.py           # Integration tests — CLI vs real API
  test_server_integration.py        # Integration tests — HTTP endpoints vs real API
```

## Data Flow

```
HTTP client / curl / browser
        │
        ▼
  FastAPI app  (src/nba/server.py)         nba-scores CLI  (src/nba/cli.py)
        │                                          │
        ├─► live_service.get_live_scoreboard()     │
        ├─► live_service.get_live_lineup()         ├─► live_service.*
        ├─► player_service.find_players_by_name()  └─► player_service.*
        └─► player_service.find_players_by_team()         │
                                                          ▼
                                                   Poller (background thread)
                                                   used by `watch` command
        │
        ▼
  nba_api.stats.static  (local dict, no network)
  nba_api.stats.endpoints.CommonTeamRoster  (NBA stats API — network)
  nba_api.live.nba.endpoints.ScoreBoard     (NBA live API — network)
  nba_api.live.nba.endpoints.BoxScore       (NBA live API — network)
```

## HTTP Endpoints

| Method | Path | Description | Returns |
|--------|------|-------------|---------|
| `GET` | `/scoreboard` | Today's games with live scores | `{"games": [...]}` |
| `GET` | `/lineup/{game_id}` | On-court players for a live game | `{"home": [...], "away": [...]}` |
| `GET` | `/players?name=` | Search players by name (partial, case-insensitive) | `{"players": [...]}` |
| `GET` | `/players/team?name=` | Roster for a team by full name | `{"players": [...]}` |

OpenAPI docs auto-generated at `http://localhost:8000/docs`.

## Models

All models are `frozen=True` dataclasses (immutable). Serialized to JSON via `dataclasses.asdict()`.

- `GameSummary` — game_id, home_team, away_team, home_score, away_score, status, period, clock
- `Player` — id, full_name, first_name, last_name, is_active
- `PlayerOnCourt` — name, jersey_num, position, points, assists, rebounds
- `Team` — id, full_name, abbreviation, nickname, city, state, year_founded

## Test Strategy

**Unit tests** (mocked, fast, no network):
- Mock targets patch at the module level:
  - `nba.player_service.players_static.find_players_by_full_name`
  - `nba.player_service.teams_static.find_teams_by_full_name`
  - `nba.player_service._get_roster_for_team`
  - `nba.player_service.commonteamroster.CommonTeamRoster`
  - `nba.live_service.live_scoreboard.ScoreBoard`
  - `nba.live_service.live_boxscore.BoxScore`

**Integration tests** (real NBA API, network required):
- `test_server_integration.py` — uses `fastapi.testclient.TestClient` (backed by `httpx`)
- `test_player_to_service_integration.py` — calls service functions directly
- `test_cli_integration.py` — invokes CLI via `typer.testing.CliRunner`
- Tests that require live games use `pytest.skip` when no games are scheduled

## Key Design Decisions

- `nba_api.stats.static` (players/teams) is a local dictionary — no network. Safe to call in tests without mocking.
- `commonteamroster.CommonTeamRoster` and all live endpoints hit the NBA API — always mock in unit tests.
- Models are `frozen=True` dataclasses (immutable); never mutate, always return new instances.
- `server.py` wraps `ValueError` from services as HTTP 422; unexpected errors from live endpoints as HTTP 502.
- The `Poller` runs on a daemon thread; errors during fetch are swallowed to keep the thread alive.
