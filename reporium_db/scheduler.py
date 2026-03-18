"""Tier-based scheduling for incremental repo syncs at scale."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from .models import ScheduleEntry

logger = logging.getLogger(__name__)

TIER_NIGHTLY = "nightly"
TIER_WEEKLY = "weekly"
TIER_MONTHLY = "monthly"


def get_tier(pushed_at: Optional[str], nightly_days: int = 30, weekly_days: int = 365) -> str:
    """Assign a schedule tier based on the repo's last push date.

    Args:
        pushed_at: ISO-8601 timestamp of last push, or None for empty repos.
        nightly_days: Repos pushed within this many days are nightly.
        weekly_days: Repos pushed within this many days (but > nightly_days) are weekly.

    Returns:
        One of 'nightly', 'weekly', or 'monthly'.
    """
    if not pushed_at:
        return TIER_MONTHLY

    try:
        pushed = datetime.fromisoformat(pushed_at.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Could not parse pushedAt=%r — defaulting to monthly", pushed_at)
        return TIER_MONTHLY

    now = datetime.now(timezone.utc)
    age_days = (now - pushed).days

    if age_days <= nightly_days:
        return TIER_NIGHTLY
    if age_days <= weekly_days:
        return TIER_WEEKLY
    return TIER_MONTHLY


def is_due(name: str, tier: str, schedule: dict[str, ScheduleEntry]) -> bool:
    """Return True if a repo is due for checking based on its tier and last check time.

    Args:
        name: Repository nameWithOwner.
        tier: Assigned tier (nightly/weekly/monthly).
        schedule: Current schedule mapping nameWithOwner → ScheduleEntry.

    Returns:
        True if the repo should be fetched in this run.
    """
    if name not in schedule:
        return True  # Never seen — always fetch

    entry = schedule[name]
    try:
        last = datetime.fromisoformat(entry.last_checked.replace("Z", "+00:00"))
    except ValueError:
        return True

    now = datetime.now(timezone.utc)
    if tier == TIER_NIGHTLY:
        return now.date() > last.date()
    if tier == TIER_WEEKLY:
        return (now - last) >= timedelta(days=7)
    # monthly
    return (now - last) >= timedelta(days=30)


def load_schedule(path: Path) -> dict[str, ScheduleEntry]:
    """Load the schedule from a JSON file.

    Args:
        path: Path to schedule.json.

    Returns:
        Mapping of nameWithOwner → ScheduleEntry. Empty dict if file missing.
    """
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
        return {
            k: ScheduleEntry(
                repo_name=v["repo_name"],
                last_checked=v["last_checked"],
                tier=v["tier"],
                upstream_pushed_at=v.get("upstream_pushed_at"),
            )
            for k, v in raw.items()
        }
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load schedule from %s: %s", path, exc)
        return {}


def save_schedule(schedule: dict[str, ScheduleEntry], path: Path) -> None:
    """Atomically save the schedule to a JSON file.

    Args:
        schedule: Mapping of nameWithOwner → ScheduleEntry.
        path: Destination path.
    """
    data = {
        k: {
            "repo_name": v.repo_name,
            "last_checked": v.last_checked,
            "tier": v.tier,
            "upstream_pushed_at": v.upstream_pushed_at,
        }
        for k, v in schedule.items()
    }
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2))
    os.replace(tmp, path)
    logger.info("Saved schedule with %d entries to %s", len(schedule), path)
