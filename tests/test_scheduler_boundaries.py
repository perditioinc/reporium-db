"""Tier-boundary and threshold-edge tests for the scheduler.

The existing tests cover the happy path (clearly nightly / weekly / monthly).
These pin the EXACT boundaries that the get_tier(... <= nightly_days) and
(... <= weekly_days) comparisons hinge on, plus custom thresholds and a few
edge inputs (future push dates, naive timestamps, the Z-suffix path). Getting a
boundary off by one silently mis-tiers thousands of repos at 100K scale, so each
boundary is asserted on both sides.
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


def _pushed_days_ago(days: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


# -- nightly/weekly boundary (default thresholds 30 / 365) ---------------------


def test_tier_exactly_at_nightly_boundary_is_nightly():
    """age_days == nightly_days (30) is INCLUSIVE of nightly (uses <=).

    ``.days`` floors, so pushing slightly MORE than 30 days ago (30d + 1h)
    yields exactly age_days == 30 - the boundary value. With ``<=`` this is
    nightly; flipping to ``<`` would mis-tier it as weekly (the mutation this
    test exists to catch).
    """
    pushed = (datetime.now(timezone.utc) - timedelta(days=30, hours=1)).isoformat()
    assert get_tier(pushed) == TIER_NIGHTLY


def test_tier_just_past_nightly_boundary_is_weekly():
    """age_days == nightly_days + 1 (31) crosses into weekly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=31, hours=1)).isoformat()
    assert get_tier(pushed) == TIER_WEEKLY


def test_tier_exactly_at_weekly_boundary_is_weekly():
    """age_days == weekly_days (365) is INCLUSIVE of weekly (uses <=)."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=365, hours=1)).isoformat()
    assert get_tier(pushed) == TIER_WEEKLY


def test_tier_just_past_weekly_boundary_is_monthly():
    """age_days == weekly_days + 1 (366) crosses into monthly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=366, hours=1)).isoformat()
    assert get_tier(pushed) == TIER_MONTHLY


# -- custom thresholds are honored ---------------------------------------------


def test_tier_custom_nightly_threshold():
    """A repo 10 days old is weekly when nightly_days is tightened to 7."""
    pushed = _pushed_days_ago(10)
    assert get_tier(pushed, nightly_days=7, weekly_days=365) == TIER_WEEKLY


def test_tier_custom_weekly_threshold():
    """A repo 100 days old is monthly when weekly_days is tightened to 90."""
    pushed = _pushed_days_ago(100)
    assert get_tier(pushed, nightly_days=30, weekly_days=90) == TIER_MONTHLY


@pytest.mark.parametrize(
    "age_days,nightly,weekly,expected",
    [
        (0, 30, 365, TIER_NIGHTLY),       # pushed today
        (29, 30, 365, TIER_NIGHTLY),
        (31, 30, 365, TIER_WEEKLY),
        (200, 30, 365, TIER_WEEKLY),
        (400, 30, 365, TIER_MONTHLY),
        (5, 1, 7, TIER_WEEKLY),           # custom tight thresholds
        (10, 1, 7, TIER_MONTHLY),
    ],
)
def test_tier_table(age_days, nightly, weekly, expected):
    """Table-driven coverage across both boundaries and custom thresholds."""
    # Add an hour of slack so integer-day flooring lands deterministically.
    pushed = (datetime.now(timezone.utc) - timedelta(days=age_days, hours=1)).isoformat()
    assert get_tier(pushed, nightly_days=nightly, weekly_days=weekly) == expected


# -- edge inputs ---------------------------------------------------------------


def test_tier_future_push_date_is_nightly():
    """A repo with a future pushedAt (clock skew) has negative age => nightly.

    This documents the current behavior so a future regression that flips skewed
    timestamps to monthly (and starves a freshly pushed repo of nightly checks)
    is caught.
    """
    future = (datetime.now(timezone.utc) + timedelta(days=2)).isoformat()
    assert get_tier(future) == TIER_NIGHTLY


def test_tier_z_suffix_at_boundary():
    """The Z-suffix parse path respects the nightly boundary like the +00:00 path."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert get_tier(pushed) == TIER_NIGHTLY


def test_tier_empty_string_defaults_monthly():
    """An empty pushedAt string is falsy and defaults to monthly (no crash)."""
    assert get_tier("") == TIER_MONTHLY
