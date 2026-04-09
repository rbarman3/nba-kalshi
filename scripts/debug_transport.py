#!/usr/bin/env python3
"""Debug script: run NBATransport and print live snapshots with poll state.

Usage:
    PYTHONPATH=src python3 scripts/debug_transport.py [--max-snapshots 10] [--max-seconds 30]
"""
import asyncio
import sys
import time
from argparse import ArgumentParser

from nba.live_service import get_live_scoreboard
from pipeline.transport import GameStatus, NBATransport


async def main(max_snapshots: int = 10, max_seconds: int = 30):
    """Fetch live games and run transport, printing snapshots until limit reached."""
    games = get_live_scoreboard()
    game_ids = [g.game_id for g in games]

    if not game_ids:
        print("No games scheduled today.")
        return

    print(f"Found {len(games)} games: {game_ids}\n")

    queue = asyncio.Queue()
    transport = NBATransport(game_ids=game_ids, queue=queue)

    snapshot_count = [0]
    start_time = time.time()

    async def drain():
        """Consume queue and print snapshots until limits are reached."""
        while True:
            try:
                snap = await asyncio.wait_for(queue.get(), timeout=2.0)
            except asyncio.TimeoutError:
                _print_poll_states(transport)
                continue

            elapsed = time.time() - start_time
            age = round(time.time() - snap.fetched_at, 3)

            game = snap.payload.get("game", {})
            oncourt_names = []
            for team_key in ["homeTeam", "awayTeam"]:
                team = game.get(team_key, {})
                team_name = team.get("teamName", "?")
                players = team.get("players", [])
                on_court = [p["name"] for p in players if p.get("oncourt") == "1"]
                if on_court:
                    oncourt_names.append(f"{team_name}: {', '.join(on_court)}")

            print(f"[{snap.game_id}] LIVE  age={age}s  {' | '.join(oncourt_names) if oncourt_names else '(no players on court yet)'}")

            snapshot_count[0] += 1
            if snapshot_count[0] >= max_snapshots or elapsed > max_seconds:
                return

    transport_task = asyncio.create_task(transport.run())
    drain_task = asyncio.create_task(drain())

    await asyncio.wait(
        {transport_task, drain_task},
        timeout=max_seconds,
        return_when=asyncio.FIRST_COMPLETED,
    )

    for task in (transport_task, drain_task):
        if not task.done():
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    elapsed = time.time() - start_time
    _print_summary(transport, snapshot_count[0], elapsed)


def _print_poll_states(transport: NBATransport) -> None:
    for gid, state in transport._poll_states.items():
        if state.status == GameStatus.LIVE:
            snap = transport.cache.get(gid)
            age = round(time.time() - snap.fetched_at, 1) if snap else "?"
            print(f"[{gid}] LIVE         age={age}s")
        else:
            print(f"[{gid}] UNKNOWN")


def _print_summary(transport: NBATransport, count: int, elapsed: float) -> None:
    from collections import Counter
    tally = Counter(s.status for s in transport._poll_states.values())
    print(f"\n--- Summary after {round(elapsed, 1)}s ({count} snapshots) ---")
    for status in GameStatus:
        print(f"  {status.value:<15}: {tally[status]}")


if __name__ == "__main__":
    parser = ArgumentParser(description="Debug script for NBA transport layer")
    parser.add_argument("--max-snapshots", type=int, default=10, help="Stop after N snapshots (default 10)")
    parser.add_argument("--max-seconds", type=int, default=30, help="Stop after N seconds (default 30)")
    args = parser.parse_args()

    try:
        asyncio.run(main(max_snapshots=args.max_snapshots, max_seconds=args.max_seconds))
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        sys.exit(0)
