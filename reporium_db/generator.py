"""Generate README.md and LAST_RUN.md from sync run statistics."""

from __future__ import annotations

from .models import SyncRun


def generate_readme(run: SyncRun, index: dict) -> str:
    """Generate the repository README.md from the latest sync run and index.

    Args:
        run: Statistics from the most recent sync run.
        index: Parsed index.json content.

    Returns:
        Markdown string for README.md.
    """
    meta = index.get("meta", {})
    total = meta.get("total", 0)
    last_updated = meta.get("last_updated", "unknown")

    perf_rows = (
        "| 805 | ~68s fetch | ~9 API calls |\n"
        "| 10,000 | ~14min fetch | ~100 API calls |\n"
        "| 100,000 | ~250 GraphQL calls/night (tiered) | ~250 API calls |"
    )

    errors_note = f"\n> **Last run errors:** {', '.join(run.errors)}" if run.errors else ""

    readme = f"""# reporium-db
> Nightly GitHub metadata sync powering reporium.com — currently tracking **{total:,} repos** across {len(index.get("categories", {}))} categories.

## Why This Exists

reporium-db is the data backbone of the Reporium platform. It fetches GitHub repository metadata
nightly via the GraphQL API, detects changes, and outputs partitioned JSON that the frontend,
API, and AI enrichment pipeline all consume.

Designed from day one for 100K repos using schedule tiers, cursor checkpointing, and partitioned output.

## Architecture

```
GitHub GraphQL API
      │  (cursor pagination, 100 repos/page)
      ▼
  fetcher.py ──► checkpoint (resume if <24h old)
      │
      ▼
  scheduler.py ──► tier assignment (nightly / weekly / monthly)
      │
      ▼
   differ.py ──► snapshot yesterday ──► pending_enrichment.json
      │
      ▼
partitioner.py ──► data/
                       index.json
                       recent.json
                       top_starred.json
                       by_category/
                       by_language/
                       full/repos_NNNN.json
```

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env  # add GH_TOKEN and GH_USERNAME
python -m reporium_db sync
python -m reporium_db status
```

## Configuration

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| GH_TOKEN | yes | — | GitHub PAT (read:repo scope) |
| GH_USERNAME | yes | — | User or org to sync |
| CONCURRENCY_GRAPHQL | no | 20 | Parallel GraphQL fetches |
| RATE_LIMIT_THRESHOLD | no | 0.8 | Throttle at this fraction used |
| CHECKPOINT_INTERVAL | no | 1000 | Save checkpoint every N repos |
| NIGHTLY_TIER_DAYS | no | 30 | Active threshold in days |
| WEEKLY_TIER_DAYS | no | 365 | Moderate threshold in days |

## Performance

| Repos | Runtime | API Calls |
|-------|---------|-----------|
{perf_rows}

## Platform Fit

- **reporium-api** reads `data/` directly for search and filtering
- **reporium-ingestion** reads `pending_enrichment.json` for AI enrichment
- **reporium-dataset** mirrors `index.json` for public dataset access
- **reporium-metrics** reads `data/index.json` for platform performance tracking

## Contributing

PRs welcome. Run `pytest tests/` and `ruff check .` before submitting.

## License

MIT
{errors_note}

---
*Last updated: {last_updated} | {total:,} repos tracked*
"""
    return readme


def generate_last_run(run: SyncRun) -> str:
    """Generate LAST_RUN.md markdown from a SyncRun.

    Args:
        run: The completed sync run.

    Returns:
        Markdown string for LAST_RUN.md.
    """
    duration = f"{run.duration_seconds:.1f}s" if run.duration_seconds is not None else "—"
    errors_section = "\n".join(f"- {e}" for e in run.errors) if run.errors else "_None_"
    resumed = "Yes (checkpoint)" if run.checkpoint_resumed else "No"

    return f"""# Last Sync Run

| Field | Value |
|-------|-------|
| Started | {run.started_at} |
| Completed | {run.completed_at or "—"} |
| Duration | {duration} |
| Total fetched | {run.total_fetched:,} |
| Checked | {run.checked:,} |
| Skipped (schedule) | {run.skipped_schedule:,} |
| New repos | {run.new_repos:,} |
| Updated repos | {run.updated_repos:,} |
| API calls used | {run.api_calls_used:,} |
| Rate limit remaining | {run.rate_limit_remaining:,} |
| Checkpoint resumed | {resumed} |

## Errors

{errors_section}
"""
