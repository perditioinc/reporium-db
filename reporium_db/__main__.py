"""CLI entry point: python -m reporium_db sync [--dry-run] | status."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DATA_DIR = Path("data")
SNAPSHOT_DIR = Path("snapshot")
SCHEDULE_PATH = Path("schedule.json")


def _cmd_status() -> None:
    """Print current sync status from index.json and LAST_RUN.md."""
    index_path = DATA_DIR / "index.json"
    if not index_path.exists():
        print("No data yet. Run: python -m reporium_db sync")
        return
    index = json.loads(index_path.read_text())
    meta = index.get("meta", {})
    print(f"Total repos : {meta.get('total', 0):,}")
    print(f"Last updated: {meta.get('last_updated', 'never')}")
    print(f"Categories  : {len(index.get('categories', {}))}")
    print(f"Languages   : {len(index.get('languages', {}))}")

    last_run_path = Path("LAST_RUN.md")
    if last_run_path.exists():
        print("\n--- LAST_RUN.md ---")
        print(last_run_path.read_text())


async def _cmd_sync(dry_run: bool) -> None:
    """Run a full sync: fetch → schedule → diff → partition → generate docs."""
    from .config import load_config
    from .differ import compute_diff
    from .fetcher import fetch_all_repos
    from .generator import generate_last_run, generate_readme
    from .models import ScheduleEntry, SyncRun
    from .partitioner import write_partitioned
    from .scheduler import get_tier, is_due, load_schedule, save_schedule

    config = load_config()
    t0 = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    run = SyncRun(started_at=started_at)

    try:
        logger.info("Starting sync (dry_run=%s)", dry_run)
        repos, fetch_meta = await fetch_all_repos(config)
        run.total_fetched = len(repos)
        run.api_calls_used = fetch_meta["api_calls"]
        run.checkpoint_resumed = fetch_meta["resumed"]
        rate_info = fetch_meta.get("rate_info", {})
        run.rate_limit_remaining = rate_info.get("remaining", 0)

        schedule = load_schedule(SCHEDULE_PATH)

        # Determine which repos are due and update schedule
        due_repos = []
        for repo in repos:
            tier = get_tier(repo.pushedAt, config.nightly_tier_days, config.weekly_tier_days)
            if is_due(repo.nameWithOwner, tier, schedule):
                due_repos.append(repo)
                run.checked += 1
                schedule[repo.nameWithOwner] = ScheduleEntry(
                    repo_name=repo.nameWithOwner,
                    last_checked=datetime.now(timezone.utc).isoformat(),
                    tier=tier,
                    upstream_pushed_at=repo.pushedAt,
                )
            else:
                run.skipped_schedule += 1

        logger.info("Due for processing: %d / %d", len(due_repos), len(repos))

        if dry_run:
            logger.info("Dry-run: skipping writes")
            print(
                f"Would process {len(due_repos)} repos ({run.skipped_schedule} skipped by schedule)"
            )
            return

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        diff = compute_diff(repos, DATA_DIR, SNAPSHOT_DIR)
        run.new_repos = len(diff.new_repos)
        run.updated_repos = len(diff.updated_repos)

        index = write_partitioned(repos, DATA_DIR)
        save_schedule(schedule, SCHEDULE_PATH)

        run.completed_at = datetime.now(timezone.utc).isoformat()
        run.duration_seconds = time.monotonic() - t0

        readme = generate_readme(run, index)
        Path("README.md").write_text(readme)
        Path("LAST_RUN.md").write_text(generate_last_run(run))

        logger.info(
            "Sync complete in %.1fs — %d new, %d updated",
            run.duration_seconds,
            run.new_repos,
            run.updated_repos,
        )

    except Exception as exc:
        run.errors.append(str(exc))
        run.completed_at = datetime.now(timezone.utc).isoformat()
        run.duration_seconds = time.monotonic() - t0
        logger.error("Sync failed: %s", exc, exc_info=True)
        raise


def main() -> None:
    """Parse CLI arguments and dispatch to the correct command."""
    parser = argparse.ArgumentParser(description="reporium-db CLI")
    subparsers = parser.add_subparsers(dest="command")

    sync_parser = subparsers.add_parser("sync", help="Run a full sync")
    sync_parser.add_argument("--dry-run", action="store_true", help="Preview without writing")

    subparsers.add_parser("status", help="Show current data status")

    args = parser.parse_args()

    if args.command == "sync":
        asyncio.run(_cmd_sync(args.dry_run))
    elif args.command == "status":
        _cmd_status()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
