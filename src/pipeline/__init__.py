"""Trading pipeline for Kalshi NBA in-game contracts.

Layers:
  1. transport.py — Poll NBA CDN for raw snapshots
  2. processor.py — Diff snapshots, emit lineup changes (future)
  3. strategy.py — Signal logic: target players in/out (future)
  4. execution.py — Kalshi order placement (future)
"""
