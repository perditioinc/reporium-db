"""Tier-assignment boundary tests for reporium_db.scheduler.get_tier.

The existing scheduler tests cover the interiors of each tier (10d, 60d, 400d).
These assert the exact tier *boundaries*, which is where off-by-one bugs hide:

    age_days <= nightly_days            -> nightly
    nightly_days < age_days <= weekly   -> weekly
    age_days > weekly_days              -> monthly

get_tier uses datetime.days (floor toward zero) so we add a few hours of slack
to land cleanly on the intended whole-day age and avoid flakiness at midnight.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from reporium_db.scheduler import (
    TIER_MONTHLY,
    TIER_NIGHTLY,
    TIER_WEEKLY,
    get_tier,
)


def _pushed_days_ago(days: int, extra_hours: int = 2) -> str:
    """ISO timestamp `days` whole days ago plus a small slack so .days == days."""
    when = datetime.now(timezone.utc) - timedelta(days=days, hours=extra_hours)
    return when.isoformat()


@pytest.mark.parametrize(
    "age_days,expected",
    [
        (0, TIER_NIGHTLY),    # pushed today
        (29, TIER_NIGHTLY),   # just inside nightly
        (30, TIER_NIGHTLY),   # exact nightly boundary is inclusive
        (31, TIER_WEEKLY),    # first day past nightly -> weekly
        (200, TIER_WEEKLY),   # interior weekly
        (364, TIER_WEEKLY),   # just inside weekly
        (365, TIER_WEEKLY),   # exact weekly boundary is inclusive
        (366, TIER_MONTHLY),  # first day past weekly -> monthly
        (1000, TIER_MONTHLY), # deep monthly
    ],
)
def test_get_tier_default_boundaries(age_days, expected):
    """Default thresholds (30 / 365) classify each boundary day correctly."""
    assert get_tier(_pushed_days_ago(age_days)) == expected


@pytest.mark.parametrize(
    "age_days,expected",
    [
        (7, TIER_NIGHTLY),    # exact custom nightly boundary inclusive
        (8, TIER_WEEKLY),     # first day past custom nightly
        (90, TIER_WEEKLY),    # exact custom weekly boundary inclusive
        (91, TIER_MONTHLY),   # first day past custom weekly
    ],
)
def test_get_tier_custom_thresholds(age_days, expected):
    """Custom nightly/weekly thresholds shift the boundaries accordingly."""
    assert get_tier(_pushed_days_ago(age_days), nightly_days=7, weekly_days=90) == expected


def test_get_tier_boundary_is_inclusive_not_exclusive():
    """Pin the inclusivity invariant: age == nightly_days is nightly, not weekly.

    Guards against a regression to a strict '<' comparison, which would push
    every boundary-day repo down a tier and silently slow its refresh cadence.
    """
    exactly_nightly = _pushed_days_ago(30)
    exactly_weekly = _pushed_days_ago(365)
    assert get_tier(exactly_nightly) == TIER_NIGHTLY
    assert get_tier(exactly_weekly) == TIER_WEEKLY
    # And one day past each must drop exactly one tier.
    assert get_tier(_pushed_days_ago(31)) == TIER_WEEKLY
    assert get_tier(_pushed_days_ago(366)) == TIER_MONTHLY
