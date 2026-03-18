"""Tests for reporium_db.scheduler."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from reporium_db.models import ScheduleEntry
from reporium_db.scheduler import (
    TIER_MONTHLY,
    TIER_NIGHTLY,
    TIER_WEEKLY,
    get_tier,
    is_due,
    load_schedule,
    save_schedule,
)

# ── get_tier ──────────────────────────────────────────────────────────────────


def test_get_tier_nightly():
    """Repos pushed within 30 days are nightly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()
    assert get_tier(pushed) == TIER_NIGHTLY


def test_get_tier_weekly():
    """Repos pushed 60 days ago (>30d but ≤365d) are weekly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    assert get_tier(pushed) == TIER_WEEKLY


def test_get_tier_monthly():
    """Repos pushed over a year ago are monthly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=400)).isoformat()
    assert get_tier(pushed) == TIER_MONTHLY


def test_get_tier_none():
    """None pushed_at defaults to monthly."""
    assert get_tier(None) == TIER_MONTHLY


def test_get_tier_invalid():
    """Unparseable pushed_at defaults to monthly."""
    assert get_tier("not-a-date") == TIER_MONTHLY


def test_get_tier_z_suffix():
    """ISO timestamps ending in Z are parsed correctly."""
    pushed = (datetime.now(timezone.utc) - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert get_tier(pushed) == TIER_NIGHTLY


# ── is_due ────────────────────────────────────────────────────────────────────


def test_is_due_new_repo():
    """A repo not in the schedule is always due."""
    assert is_due("user/new-repo", TIER_NIGHTLY, {}) is True


def test_is_due_nightly_checked_yesterday():
    """Nightly repo checked yesterday is due today."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    schedule = {"user/repo": ScheduleEntry("user/repo", last_checked=yesterday, tier=TIER_NIGHTLY)}
    assert is_due("user/repo", TIER_NIGHTLY, schedule) is True


def test_is_due_nightly_checked_today():
    """Nightly repo checked today is not due."""
    today = datetime.now(timezone.utc).isoformat()
    schedule = {"user/repo": ScheduleEntry("user/repo", last_checked=today, tier=TIER_NIGHTLY)}
    assert is_due("user/repo", TIER_NIGHTLY, schedule) is False


def test_is_due_weekly_checked_8_days_ago():
    """Weekly repo checked 8 days ago is due."""
    checked = (datetime.now(timezone.utc) - timedelta(days=8)).isoformat()
    schedule = {"user/repo": ScheduleEntry("user/repo", last_checked=checked, tier=TIER_WEEKLY)}
    assert is_due("user/repo", TIER_WEEKLY, schedule) is True


def test_is_due_weekly_checked_3_days_ago():
    """Weekly repo checked 3 days ago is not due."""
    checked = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    schedule = {"user/repo": ScheduleEntry("user/repo", last_checked=checked, tier=TIER_WEEKLY)}
    assert is_due("user/repo", TIER_WEEKLY, schedule) is False


def test_is_due_monthly_checked_31_days_ago():
    """Monthly repo checked 31 days ago is due."""
    checked = (datetime.now(timezone.utc) - timedelta(days=31)).isoformat()
    schedule = {"user/repo": ScheduleEntry("user/repo", last_checked=checked, tier=TIER_MONTHLY)}
    assert is_due("user/repo", TIER_MONTHLY, schedule) is True


# ── load/save schedule ────────────────────────────────────────────────────────


def test_save_and_load_schedule(tmp_path):
    """Round-trip: save then load produces identical schedule."""
    path = tmp_path / "schedule.json"
    schedule = {
        "user/repo-a": ScheduleEntry(
            repo_name="user/repo-a",
            last_checked="2026-03-17T05:00:00+00:00",
            tier=TIER_NIGHTLY,
            upstream_pushed_at="2026-03-16T12:00:00Z",
        ),
        "user/repo-b": ScheduleEntry(
            repo_name="user/repo-b",
            last_checked="2026-03-10T05:00:00+00:00",
            tier=TIER_WEEKLY,
        ),
    }
    save_schedule(schedule, path)
    loaded = load_schedule(path)

    assert set(loaded.keys()) == set(schedule.keys())
    assert loaded["user/repo-a"].tier == TIER_NIGHTLY
    assert loaded["user/repo-b"].upstream_pushed_at is None


def test_load_schedule_missing_file(tmp_path):
    """Missing schedule file returns an empty dict."""
    result = load_schedule(tmp_path / "nonexistent.json")
    assert result == {}


def test_load_schedule_corrupt_file(tmp_path):
    """Corrupt schedule file returns an empty dict without raising."""
    path = tmp_path / "schedule.json"
    path.write_text("NOT JSON {{")
    result = load_schedule(path)
    assert result == {}


def test_save_schedule_atomic(tmp_path):
    """save_schedule uses atomic write (no .tmp remains after save)."""
    path = tmp_path / "schedule.json"
    save_schedule({"user/r": ScheduleEntry("user/r", "2026-03-17", TIER_NIGHTLY)}, path)
    assert path.exists()
    assert not path.with_suffix(".tmp").exists()
