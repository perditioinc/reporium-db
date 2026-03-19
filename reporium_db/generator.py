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
    languages = index.get("languages", {})

    duration = f"{run.duration_seconds:.1f}s" if run.duration_seconds is not None else "—"
    errors_note = f"\n> **Last run errors:** {', '.join(run.errors)}" if run.errors else ""

    # Scale estimates based on actual run data
    api_per_100 = (run.api_calls_used / total * 100) if total else 9
    perf_rows = (
        f"| {total:,} | {duration} | {run.api_calls_used} API calls |\n"
        f"| 10,000 | ~{int(api_per_100 * 100 / 60 + 10)}min | ~{int(api_per_100 * 100)} API calls |\n"
        f"| 100,000 | ~{int(api_per_100 * 1000 / 60 + 60)}min (tiered) | ~{int(api_per_100 * 1000)} API calls |"
    )

    last_run_table = f"""\
| Field | Value |
|-------|-------|
| Duration | {duration} |
| Repos fetched | {run.total_fetched:,} |
| New repos | {run.new_repos:,} |
| Updated repos | {run.updated_repos:,} |
| API calls used | {run.api_calls_used:,} |
| Rate limit remaining | {run.rate_limit_remaining:,} |
| Schedule tiers | nightly · weekly · monthly |
| Checkpoint resumed | {"Yes" if run.checkpoint_resumed else "No"} |\
"""

    readme = f"""# reporium-db
> Nightly GitHub metadata sync powering reporium.com — currently tracking **{total:,} repos** across **{len(languages)} languages**.

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

## Last Run

{last_run_table}

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
