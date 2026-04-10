# NBA Live Data Pipeline

A real-time pipeline that polls NBA CDN boxscore data, detects game events by diffing consecutive snapshots, persists data for backtesting, and will eventually trade prediction markets on detected signals.

## Architecture

### Core Pipeline

The pipeline is a linear 4-stage flow connected by `asyncio.Queue` channels:

```
Transport ──► raw_queue ──► Processor ──► event_queue ──► Strategy (planned) ──► Execution (planned)
```

| Stage | Module | Status | Description |
|-------|--------|--------|-------------|
| **Transport** | `pipeline/transport.py` | Built | Async CDN polling with jitter, hash dedup, game state machine |
| **Processor** | `pipeline/processor.py` | Built | Diffs consecutive snapshots to emit typed events |
| **Strategy** | — | Planned | Consume events, generate trade signals |
| **Execution** | — | Planned | Place and manage prediction market orders |

### Infrastructure

These modules support the core pipeline but are not stages in the data flow:

```
                                    ┌──► SnapshotStore (JSONL → Parquet)
Transport ──► raw_queue ──► Processor ──► event_queue
    ▲                                        │
    │                                        └──► Runner (logs events)
SnapshotReplayer (backtesting)
    └── loads stored snapshots, feeds into Processor
```

| Module | Purpose |
|--------|---------|
| **Store** (`pipeline/store.py`) | Side-channel off transport — persists snapshots to disk during live games, compacts to Parquet on game end |
| **Replayer** (`pipeline/replayer.py`) | Separate entry point (`nba-replay`) — loads stored snapshots and feeds them through processor for backtesting |
| **Runner** (`nba/runner.py`) | Orchestrator — discovers today's games, wires transport + processor, logs events |
| **Watchdog** | Planned — monitors transport health |

---

## Quick Start

```bash
pip install -e ".[dev]"
```

### Run Live Pipeline

```bash
# Poll today's live games
nba-pipeline

# With snapshot persistence
SNAPSHOT_STORE_ENABLED=true nba-pipeline
```

### Replay Stored Data

```bash
# Replay all games from a date (fast mode)
nba-replay 2026-04-09

# Replay a single game with event details
nba-replay 2026-04-09 --game 0022501170 --verbose

# Simulate real-time pacing
nba-replay 2026-04-09 --mode realtime

# Add signal delay to simulate detection latency
nba-replay 2026-04-09 --signal-delay 3.0
```

### Other CLI Tools

```bash
nba-scores scores    # Today's live scoreboard
nba-server           # FastAPI server (GET /scoreboard, /players, /players/team)
```

---

## Event Types

All events are detected by diffing consecutive boxscore snapshots (the NBA CDN boxscore endpoint does not include a play-by-play `actions[]` array).

| Event | Trigger | Key Fields |
|-------|---------|------------|
| `LineupChangeEvent` | Player `oncourt` field changes | `players_in`, `players_out` (frozensets of personIds) |
| `ScoreChangeEvent` | Team `score` changes | `home_score`, `away_score`, `home_prev`, `away_prev` |
| `FoulEvent` | Player `foulsPersonal` increases | `player_name`, `team_tricode`, `prev_fouls`, `curr_fouls` |
| `TimeoutEvent` | Team `timeoutsRemaining` decreases | `team_tricode`, `prev_timeouts`, `curr_timeouts` |
| `TurnoverEvent` | Player `turnovers` stat increases | `player_name`, `team_tricode`, `prev_turnovers`, `curr_turnovers` |
| `PeriodEvent` | `game.period` increases | `prev_period`, `curr_period`, `game_status` |

All events include `game_id`, `period`, `clock`, `home_score`, `away_score`, and `observed_at`.

---

## Repository Layout

```
src/
  pipeline/
    transport.py        # NBATransport — async CDN polling, jitter, dedup, game state machine
    processor.py        # NBAProcessor — snapshot diffing, event emission
    store.py            # SnapshotStore — JSONL ingestion + Parquet compaction
    replayer.py         # SnapshotReplayer — replay engine with fast/realtime modes
    replay_cli.py       # nba-replay CLI tool
    models.py           # All event dataclasses + RawSnapshot

  nba/
    runner.py           # Pipeline orchestrator — discovers games, wires layers
    live_service.py     # get_live_scoreboard() — today's games from NBA API
    player_service.py   # Player/team lookup
    server.py           # FastAPI server
    cli.py              # nba-scores CLI
    models.py           # GameSummary, Player, Team

tests/
  test_processor.py     # Processor extract/diff unit tests
  test_transport.py     # Transport polling tests
  test_store.py         # Persistence tests
  test_replayer.py      # Replay engine tests
  test_server_integration.py
  test_player_service.py
  ...

data/
  snapshots/            # Persisted game data (JSONL + Parquet)
    {season}/{game_type}/{date}/{game_id}.parquet
```

---

## Snapshot Storage

Live snapshots are stored under `data/snapshots/` organized by season, game type, and date:

```
data/snapshots/2025-26/regular/2026-04-09/0022501170.parquet
```

- **Live games**: Appended to `.jsonl` files
- **Game over**: Compacted to `.parquet` with zstd compression, JSONL deleted
- **Loading**: Prefers Parquet, falls back to JSONL

---

## Transport Details

`NBATransport` polls `https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json`

- Game state machine: `UNKNOWN → NOT_STARTED (403) → LIVE (gameStatus==2) → FINAL (gameStatus==3)`
- Hash-based deduplication — only queues snapshots when payload changes
- Jittered poll interval (configurable, default 0.6–1.2s)
- Multi-game concurrency via `asyncio.gather`

---

## Replay Engine

`SnapshotReplayer` loads stored snapshots and feeds them through `NBAProcessor`:

- **Fast mode**: No delays, processes all snapshots immediately
- **Realtime mode**: Paces snapshots by original `fetched_at` timestamps
- **Signal delay**: Offsets `observed_at` on emitted events to simulate detection-to-execution latency
- **Anti-bias**: `stream_game()` async generator yields events one at a time, preventing look-ahead

---

## Run Tests

```bash
PYTHONPATH=src python3 -m pytest                    # All tests
PYTHONPATH=src python3 -m pytest tests/test_processor.py -v  # Processor only
```

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `SNAPSHOT_STORE_ENABLED` | `"false"` | Enable snapshot persistence to disk |
| `SNAPSHOT_STORE_DIR` | `"data/snapshots"` | Base directory for snapshot files |
| `PIPELINE_POLL_MIN` | `"0.6"` | Minimum poll interval (seconds) |
| `PIPELINE_POLL_MAX` | `"1.2"` | Maximum poll interval (seconds) |
| `NBA_CDN_USER_AGENT` | Chrome 145 UA | Override User-Agent header |
| `NBA_CDN_SEC_CH_UA` | Chrome 145 | Override Sec-Ch-Ua header |
