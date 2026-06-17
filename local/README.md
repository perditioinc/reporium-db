# Local OSS dev substrate (`local/`)

A `$0`, OSS-only, fully local way to run the **real** reporium-db pipeline on a
clean checkout, with no GitHub token, no network calls to `api.github.com`, and
no paid services. Additive and local only: it does not modify the application,
the nightly sync workflow, or CI.

## What it does

reporium-db's only "cloud" dependency is the **GitHub GraphQL API** (token
gated, network, rate limited) plus an *optional* `reporium-events` Pub/Sub
publish. This substrate stands both of those in with local equivalents and runs
the genuine `fetch -> schedule -> diff -> partition -> generate` pipeline
against them on a clean, ephemeral scratch tree.

```
local/seed/repos.json
        │  (deterministic fixtures: tiers + recent window + no-language fallback)
        ▼
mock-github  (local/mock_github/server.py, stdlib HTTP)  ── serves GitHub
        │      GraphQL response shape, cursor pagination, rateLimit block
        ▼
runner  (local/runner.py)  ── points reporium_db.fetcher.GRAPHQL_URL at the mock
        │                      at runtime, then runs the REAL CLI: `sync`
        ▼
/work/data/  ── index.json, recent.json, top_starred.json,
                by_language/*, by_category/*, full/repos_NNNN.json,
                pending_enrichment.json, schedule.json, README/LAST_RUN
        ▼
smoke  (local/smoke.py)  ── asserts every output object exists and is well formed
```

## Cloud -> OSS / local mapping

| Production dependency | Local substrate equivalent | Cost |
|-----------------------|----------------------------|------|
| GitHub GraphQL API (`api.github.com/graphql`, PAT-gated) | `mock-github` service serving `local/seed/repos.json` with the same GraphQL shape and cursor pagination | $0 |
| `GH_TOKEN` secret | none needed; a dummy `local-no-secret` value satisfies the config loader and is ignored by the mock | $0 |
| `reporium-events` Pub/Sub publish | not installed in the runner image; the CLI already guards it with `try/except ImportError`, so it is skipped | $0 |
| Nightly committed `data/` (cloud state) | clean ephemeral `/work` scratch volume; source is mounted **read-only** so the checkout is never mutated | $0 |

The terminology lines up with the generic substrate vocabulary:

- **seed**  = load deterministic fixtures into the mock source + reset clean state
- **migrate** = run the real pipeline that materializes the dataset "schema"
  (the partitioned `data/` object layout this repo owns) on a clean DB
- **smoke** = clean state, run pipeline, assert all objects, tear down

> Note: this repo is a Python GitHub-metadata sync pipeline that outputs
> partitioned JSON. It is **not** a Postgres/pgvector/SQL-migrations repo, so
> the substrate is built around the pipeline's real cloud dependency (GitHub
> GraphQL) and its real output objects rather than a SQL database.

## Usage

From the repo root (passthrough) or from `local/`:

```bash
make up       # build images, start the mock GitHub service (waits for healthy)
make seed     # validate the seed dataset, reset the clean scratch tree
make migrate  # run the real pipeline on a clean scratch DB
make smoke    # full clean-state run + object assertions + teardown (PASS/FAIL)
make down     # stop and remove containers, network, and scratch volume
make logs     # tail mock-github logs
```

Everything runs in Docker; the only requirement is Docker + Docker Compose.

## Tuning the mock

- `local/seed/repos.json` - edit fixtures (uses `RECENT` / `WEEKLY` / `MONTHLY`
  pushedAt tokens that resolve to real dates relative to now).
- `MOCK_PAGE_SIZE` (compose env, default 2) - page size to exercise cursor
  pagination across multiple GraphQL pages.
