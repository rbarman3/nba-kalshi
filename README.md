# NBA → Kalshi Betting Pipeline

An automated pipeline that polls live NBA game data, analyzes on-court lineups, and places bets via the [Kalshi](https://kalshi.com) prediction market API.

## Overview

```
NBA Live API  ──►  Lineup Polling  ──►  Signal Analysis  ──►  Kalshi Orders
(nba_api)          (src/nba/)           (src/analysis/)        (src/kalshi/)
```

The system runs in two stages:

1. **NBA side (built)** — continuously polls live scoreboard and per-game lineups via `nba_api`, exposes data through a FastAPI HTTP server and a Typer CLI.
2. **Kalshi side (in progress)** — consumes lineup signals, finds matching NBA markets on Kalshi, and places/manages orders.

---

## Repository Layout

```
src/
  nba/              # Data ingestion — live scores, lineups, player lookup
    models.py         # Frozen dataclasses: GameSummary, Player, PlayerOnCourt, Team
    live_service.py   # get_live_scoreboard(), get_live_lineup(game_id)
    player_service.py # find_players_by_name(), find_players_by_team()
    server.py         # FastAPI app (4 HTTP endpoints) + uvicorn entry point
    cli.py            # Typer CLI: scores, lineup, watch
    poller.py         # Background daemon thread used by `watch`

  kalshi/           # Kalshi API integration — SEE CONTRIBUTING BELOW
    models.py         # Kalshi market, contract, order dataclasses
    client.py         # Authenticated HTTP client (REST + WebSocket)
    markets.py        # Discover and filter NBA-related markets
    orders.py         # Place, cancel, and track orders

  analysis/         # Signal layer — bridges NBA data and Kalshi bets
    signals.py        # Lineup → betting signal logic
    filters.py        # Market eligibility filters (spread, volume, timing)

tests/
  test_player_service.py                  # Unit — player lookup (mocked)
  test_roster_helper.py                   # Unit — _get_roster_for_team (mocked)
  test_live_service.py                    # Unit — scoreboard/lineup parsing (mocked)
  test_poller.py                          # Unit — Poller class
  test_cli.py                             # Unit — CLI commands (mocked)
  test_player_to_service_integration.py   # Integration — player service vs real API
  test_cli_integration.py                 # Integration — CLI via CliRunner
  test_server_integration.py             # Integration — HTTP endpoints vs real NBA API
  kalshi/                                 # Kalshi test suite (to be added)
  analysis/                               # Analysis test suite (to be added)
```

---

## NBA Side (Built)

### Install

```bash
pip install -e ".[dev]"
```

### CLI

```bash
nba-scores scores                        # Today's live scoreboard
nba-scores lineup <game_id>             # On-court players for a game
nba-scores watch [game_id] [--interval 30]  # Auto-refresh lineup
```

### HTTP Server

```bash
nba-server
# or
uvicorn nba.server:app --reload --port 8000
```

| Endpoint | Description |
|----------|-------------|
| `GET /scoreboard` | Today's games with live scores |
| `GET /lineup/{game_id}` | On-court players for a live game |
| `GET /players?name=` | Search players by name |
| `GET /players/team?name=` | Roster by team full name |

OpenAPI docs at `http://localhost:8000/docs`.

### Run Tests

```bash
# All tests with coverage
PYTHONPATH=src python3 -m pytest

# Integration tests only (requires network)
PYTHONPATH=src python3 -m pytest tests/test_server_integration.py tests/test_player_to_service_integration.py tests/test_cli_integration.py -v
```

---

## Kalshi Side (In Progress)

### Architecture

```
src/kalshi/
  client.py    ──►  Kalshi REST API  (auth, rate limiting, retries)
  markets.py   ──►  Filter NBA markets by event type, timing, liquidity
  orders.py    ──►  Place YES/NO orders, track fills, cancel stale orders
  models.py    ──►  KalshiMarket, KalshiOrder, OrderResult dataclasses

src/analysis/
  signals.py   ──►  Converts lineup data → BettingSignal(market_ticker, side, size)
  filters.py   ──►  Guards: min spread, min volume, game clock constraints
```

### Contributing — Kalshi Side

See stub files in `src/kalshi/` and `src/analysis/` for interfaces to implement.

**Environment variables required:**

```bash
KALSHI_API_KEY=your_api_key_here
KALSHI_API_KEY_ID=your_key_id_here
KALSHI_BASE_URL=https://trading-api.kalshi.com/trade-api/v2  # or demo URL
```

**Kalshi API docs:** https://trading-api.kalshi.com/trade-api/v2/openapi.json

**Key contracts to implement:**

1. `KalshiClient` — authenticated requests, handle 429 rate limits, surface errors as typed exceptions
2. `find_nba_markets(game_id)` — given an NBA game_id, return matching Kalshi market tickers
3. `BettingSignal` — output of the analysis layer, consumed by `orders.py`
4. `place_order(signal)` — translate a signal into a Kalshi order, return fill confirmation

---

## End-to-End Data Flow

```
Poller (30s interval)
  └─► get_live_lineup(game_id)          # src/nba/live_service.py
        │
        ▼
  generate_signals(home, away)          # src/analysis/signals.py
        │  returns list[BettingSignal]
        ▼
  find_nba_markets(game_id)            # src/kalshi/markets.py
        │  returns matching tickers
        ▼
  place_order(signal, ticker)          # src/kalshi/orders.py
        │
        ▼
  Kalshi order confirmation
```

---

## Mock Targets (for contributors)

When writing unit tests, patch at the **module level** where the name is used:

| Symbol | Mock target |
|--------|-------------|
| `players_static.find_players_by_full_name` | `nba.player_service.players_static.find_players_by_full_name` |
| `teams_static.find_teams_by_full_name` | `nba.player_service.teams_static.find_teams_by_full_name` |
| `_get_roster_for_team` | `nba.player_service._get_roster_for_team` |
| `CommonTeamRoster` | `nba.player_service.commonteamroster.CommonTeamRoster` |
| `ScoreBoard` | `nba.live_service.live_scoreboard.ScoreBoard` |
| `BoxScore` | `nba.live_service.live_boxscore.BoxScore` |
| CLI service calls | `nba.cli.get_live_scoreboard`, `nba.cli.get_live_lineup` |
| Kalshi HTTP calls | `kalshi.client.KalshiClient.get`, `kalshi.client.KalshiClient.post` |
