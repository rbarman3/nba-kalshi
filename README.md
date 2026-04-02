# NBA → Kalshi Betting Pipeline

An automated pipeline that polls live NBA game data, detects player substitutions in real time, and trades Kalshi prediction markets on the mispricing window between a substitution and market repricing.

## Overview

```
get_live_scoreboard()
        │ game_ids
        ▼
NBATransport ──► raw_queue ──► NBAProcessor ──► event_queue ──► (future: strategy)
        │
        └──► SnapshotStore (JSONL → Parquet on game FINAL)
```

The system has four planned layers:

1. **Market data feed (built)** — polls NBA CDN boxscore endpoint, diffs consecutive snapshots, emits `LineupChangeEvent` when player substitutions occur. Persists snapshots to JSONL during live games, compacts to Parquet when game ends.
2. **Strategy (planned)** — consumes lineup change events, maps to Kalshi markets, determines edge and signal.
3. **Execution (planned)** — places and manages Kalshi orders.
4. **Backtesting (planned)** — replays persisted snapshots through strategy layer.

---

## Repository Layout

```
src/
  pipeline/             # Market data feed (Layer 1 + 2)
    transport.py        # NBATransport — async CDN polling, jitter, dedup, game state machine
    processor.py        # NBAProcessor — lineup diff, LineupChangeEvent emission
    store.py            # SnapshotStore — JSONL ingestion + Parquet compaction
    models.py           # RawSnapshot, LineupChangeEvent, FeedHealthEvent

  nba/                  # NBA data service (scores, player lookup, HTTP server, runner)
    live_service.py     # get_live_scoreboard()
    player_service.py   # find_players_by_name(), find_players_by_team()
    server.py           # FastAPI: GET /scoreboard, /players, /players/team
    cli.py              # Typer CLI: scores command
    runner.py           # Pipeline orchestrator — wires transport + processor + store
    models.py           # GameSummary, Player, Team

  kalshi/               # Kalshi integration — stub only, not yet implemented
    __init__.py

tests/                  # Unit + integration tests
scripts/
  debug_transport.py    # Ad-hoc transport debugging
docs/
  DATA_FEED_GAPS.md     # Market data feed gap analysis
```

---

## Install

```bash
pip install -e ".[dev]"
```

## Run Pipeline

```bash
# Run the full market data feed against today's live games
PYTHONPATH=src python3 -m nba.runner

# With snapshot persistence enabled
SNAPSHOT_STORE_ENABLED=true PYTHONPATH=src python3 -m nba.runner
```

## HTTP Server

```bash
nba-server
# or
uvicorn nba.server:app --reload --port 8000
```

| Endpoint | Description |
|----------|-------------|
| `GET /scoreboard` | Today's games with live scores |
| `GET /players?name=` | Search players by name |
| `GET /players/team?name=` | Roster by team full name |

OpenAPI docs at `http://localhost:8000/docs`.

## CLI

```bash
nba-scores scores    # Today's live scoreboard
```

## Run Tests

```bash
# All tests with coverage
PYTHONPATH=src python3 -m pytest

# Single test
PYTHONPATH=src python3 -m pytest tests/test_processor.py::TestDiffLineups::test_simultaneous_in_out -v

# Integration tests only (requires network)
PYTHONPATH=src python3 -m pytest tests/test_server_integration.py tests/test_player_to_service_integration.py -v
```

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SNAPSHOT_STORE_ENABLED` | `"false"` | Enable snapshot persistence to disk |
| `SNAPSHOT_STORE_DIR` | `"data/snapshots"` | Base directory for JSONL/Parquet files |
| `PIPELINE_POLL_MIN` | `"0.6"` | Minimum poll interval (seconds) |
| `PIPELINE_POLL_MAX` | `"1.2"` | Maximum poll interval (seconds) |
| `NBA_CDN_USER_AGENT` | Chrome 145 UA | Override Akamai User-Agent fingerprint |
| `NBA_CDN_SEC_CH_UA` | Chrome 145 | Override Sec-Ch-Ua header |

---

## Pipeline Architecture

### Layer 1 — Transport (`src/pipeline/transport.py`)

`NBATransport` polls the NBA CDN boxscore endpoint with jittered intervals (0.6–1.2s). Emits `RawSnapshot` onto an `asyncio.Queue`.

- Polls `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`
- Game state machine: UNKNOWN → NOT_STARTED (403) → LIVE (gameStatus==2) → FINAL (gameStatus==3)
- Hash-based deduplication — skips unchanged payloads
- Multi-game concurrency via `asyncio.gather`
- On game FINAL: triggers Parquet compaction via `SnapshotStore.compact()`

### Layer 2 — Processor (`src/pipeline/processor.py`)

`NBAProcessor` diffs consecutive `RawSnapshot` objects to detect lineup changes.

- Reads `player.oncourt` ("1" = on court, "0" = bench) from each player in the payload
- First snapshot per game initializes state without emitting
- Subsequent snapshots are diffed — emits `LineupChangeEvent` only when players change

### Persistence — Store (`src/pipeline/store.py`)

`SnapshotStore` persists snapshots for backtesting replay.

- **Live games:** Appends to `data/snapshots/{YYYY-MM-DD}/{game_id}.jsonl`
- **Game over:** Compacts JSONL → Parquet with zstd compression, deletes JSONL
- **Loading:** Prefers Parquet if available, falls back to JSONL

### Runner (`src/nba/runner.py`)

Discovers today's games via `get_live_scoreboard()`, wires all layers, and runs them concurrently.

---

## What's Built vs Planned

| Component | Status | Description |
|-----------|--------|-------------|
| `NBATransport` | Built | Async CDN polling, jitter, dedup, game state machine |
| `NBAProcessor` | Built | Lineup diff engine, emits `LineupChangeEvent` |
| `SnapshotStore` | Built | JSONL ingestion + Parquet compaction |
| Pipeline runner | Built | Discovers games, wires layers, runs concurrently |
| `TradingStrategy` | Planned | Consume `LineupChangeEvent`, generate trade signals |
| `KalshiClient` | Planned | Authenticated HTTP client for Kalshi API |
| `OrderManager` | Planned | Place and manage Kalshi orders |
| `FeedWatchdog` | Planned | Feed health monitoring (model exists, implementation removed) |

---

## Mock Targets

When writing unit tests, patch at the **module level** where the name is used:

| Symbol | Mock target |
|--------|-------------|
| `players_static.find_players_by_full_name` | `nba.player_service.players_static.find_players_by_full_name` |
| `teams_static.find_teams_by_full_name` | `nba.player_service.teams_static.find_teams_by_full_name` |
| `_get_roster_for_team` | `nba.player_service._get_roster_for_team` |
| `CommonTeamRoster` | `nba.player_service.commonteamroster.CommonTeamRoster` |
| `ScoreBoard` | `nba.live_service.live_scoreboard.ScoreBoard` |
| `httpx.AsyncClient` | `pipeline.transport.httpx.AsyncClient` |
