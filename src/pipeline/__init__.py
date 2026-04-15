"""NBA live data pipeline for prediction market trading.

Core pipeline (linear flow via asyncio.Queue):
  1. transport.py  — Poll NBA CDN for raw boxscore snapshots
  2. processor.py  — Diff consecutive snapshots, emit typed events
  3. strategy.py   — Consume events, generate trade signals (planned)
  4. execution.py  — Place and manage prediction market orders (planned)

Infrastructure:
  - store.py       — Persist snapshots to JSONL/Parquet for backtesting
  - replayer.py    — Replay stored snapshots through processor
  - replay_cli.py  — nba-replay CLI tool
  - models.py      — All event dataclasses + RawSnapshot
"""
