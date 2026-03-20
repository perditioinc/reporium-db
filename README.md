# reporium-db

<!-- perditio-badges-start -->
[![Tests](https://github.com/perditioinc/reporium-db/actions/workflows/sync.yml/badge.svg)](https://github.com/perditioinc/reporium-db/actions/workflows/sync.yml)
![Last Commit](https://img.shields.io/github/last-commit/perditioinc/reporium-db)
![License](https://img.shields.io/github/license/perditioinc/reporium-db)
![python](https://img.shields.io/badge/python-3.11%2B-3776ab)
![suite](https://img.shields.io/badge/suite-Reporium-6e40c9)
![repos tracked](https://img.shields.io/badge/repos%20tracked-826-blue)
![languages](https://img.shields.io/badge/languages-29-blue)
![updated](https://img.shields.io/badge/updated-nightly-blue)
<!-- perditio-badges-end -->

> Nightly GitHub metadata sync powering reporium.com — currently tracking **826 repos** across **29 languages**.

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
| 826 | 127.1s | 9 API calls |

_Scale projections will be added as real data is collected._

## Platform Fit

- **reporium-api** reads `data/` directly for search and filtering
- **reporium-ingestion** reads `pending_enrichment.json` for AI enrichment
- **reporium-dataset** mirrors `index.json` for public dataset access
- **reporium-metrics** reads `data/index.json` for platform performance tracking

## Last Run

| Field | Value |
|-------|-------|
| Duration | 127.1s |
| Repos fetched | 826 |
| New repos | 0 |
| Updated repos | 0 |
| API calls used | 9 |
| Rate limit remaining | 4,876 |
| Schedule tiers | nightly · weekly · monthly |
| Checkpoint resumed | No |

## Contributing

PRs welcome. Run `pytest tests/` and `ruff check .` before submitting.

## License

MIT


---
*Last updated: 2026-03-20T07:09:18.337868+00:00 | 826 repos tracked*
