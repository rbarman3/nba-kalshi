"""NBA data pipeline for live and historical game analysis.

Core pipeline (NBA CDN):
  1. transport.py — Poll NBA CDN for raw snapshots
  2. processor.py — Diff snapshots, emit lineup/score changes

ESPN scraper (independent tool):
  1. espn_id_map.py — Discover ESPN game IDs via scoreboard API
  2. espn_transport.py — Fetch historical play-by-play data
  3. espn_processor.py — Classify plays into typed events
  4. espn_store.py — Persist raw + processed data as JSONL
  5. espn_cli.py — CLI entry point (nba-espn)

Backtesting & Replay:
  1. espn_replayer.py — Replay ESPN events with optional realtime pacing
  2. replay_cli.py — CLI entry point for NBA CDN replayer (nba-replay)
  3. replayer.py — Replay raw NBA CDN snapshots through processor
"""

from .espn_replayer import ESPNReplayer
from .models import ReplayResult

__all__ = [
    "ESPNReplayer",
    "ReplayResult",
]
