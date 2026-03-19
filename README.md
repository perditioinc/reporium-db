# reporium-db
> Nightly GitHub metadata sync powering reporium.com — currently tracking **818 repos** across 1 categories.

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
| 805 | ~68s fetch | ~9 API calls |
| 10,000 | ~14min fetch | ~100 API calls |
| 100,000 | ~250 GraphQL calls/night (tiered) | ~250 API calls |

## Platform Fit

- **reporium-api** reads `data/` directly for search and filtering
- **reporium-ingestion** reads `pending_enrichment.json` for AI enrichment
- **reporium-dataset** mirrors `index.json` for public dataset access
- **reporium-metrics** reads `data/index.json` for platform performance tracking

## Contributing

PRs welcome. Run `pytest tests/` and `ruff check .` before submitting.

## License

MIT


---
*Last updated: 2026-03-19T02:53:23.553842+00:00 | 818 repos tracked*
