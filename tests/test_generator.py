"""Tests for reporium_db.generator."""

from __future__ import annotations

from reporium_db.generator import generate_last_run, generate_readme
from reporium_db.models import SyncRun


def _run(**kwargs) -> SyncRun:
    """Build a SyncRun with sensible defaults."""
    defaults = dict(
        started_at="2026-03-17T05:00:00+00:00",
        completed_at="2026-03-17T05:01:30+00:00",
        duration_seconds=90.0,
        total_fetched=805,
        checked=805,
        skipped_schedule=0,
        new_repos=10,
        updated_repos=5,
        api_calls_used=9,
        rate_limit_remaining=4800,
        errors=[],
        checkpoint_resumed=False,
    )
    defaults.update(kwargs)
    return SyncRun(**defaults)


def _index(total: int = 805, languages: dict | None = None, categories: dict | None = None) -> dict:
    """Build a minimal index dict."""
    return {
        "meta": {
            "total": total,
            "last_updated": "2026-03-17T05:01:30+00:00",
            "version": "1.0.0",
        },
        "languages": languages or {"Python": 400, "TypeScript": 200},
        "categories": categories or {"llm": 300, "rag": 100},
    }


# ── generate_readme ────────────────────────────────────────────────────────────


def test_readme_has_required_sections():
    """README contains all 8 required section headings."""
    readme = generate_readme(_run(), _index())
    required = [
        "## Why This Exists",
        "## Architecture",
        "## Quick Start",
        "## Configuration",
        "## Performance",
        "## Platform Fit",
        "## Contributing",
        "## License",
    ]
    for section in required:
        assert section in readme, f"Missing section: {section}"


def test_readme_contains_total():
    """README prominently shows total repo count."""
    readme = generate_readme(_run(), _index(total=1234))
    assert "1,234" in readme


def test_readme_contains_env_vars():
    """README configuration section lists all env vars."""
    readme = generate_readme(_run(), _index())
    for var in ["GH_TOKEN", "GH_USERNAME", "CONCURRENCY_GRAPHQL", "RATE_LIMIT_THRESHOLD"]:
        assert var in readme


def test_readme_no_errors_note():
    """README omits the errors note when there are no errors."""
    readme = generate_readme(_run(errors=[]), _index())
    assert "Last run errors" not in readme


def test_readme_shows_errors():
    """README includes error list when the run had errors."""
    readme = generate_readme(_run(errors=["timeout", "rate limit"]), _index())
    assert "Last run errors" in readme
    assert "timeout" in readme


# ── generate_last_run ─────────────────────────────────────────────────────────


def test_last_run_has_table():
    """LAST_RUN.md contains a markdown table with key fields."""
    md = generate_last_run(_run())
    assert "| Started |" in md
    assert "| Completed |" in md
    assert "| Duration |" in md
    assert "| Total fetched |" in md


def test_last_run_shows_numbers():
    """LAST_RUN.md shows correct numeric values."""
    md = generate_last_run(_run(total_fetched=805, new_repos=10, api_calls_used=9))
    assert "805" in md
    assert "10" in md
    assert "9" in md


def test_last_run_checkpoint_resumed_yes():
    """Shows 'Yes (checkpoint)' when checkpoint_resumed is True."""
    md = generate_last_run(_run(checkpoint_resumed=True))
    assert "Yes (checkpoint)" in md


def test_last_run_no_errors():
    """Shows '_None_' for errors section when there are no errors."""
    md = generate_last_run(_run(errors=[]))
    assert "_None_" in md


def test_last_run_with_errors():
    """Lists each error when errors exist."""
    md = generate_last_run(_run(errors=["err1", "err2"]))
    assert "err1" in md
    assert "err2" in md
