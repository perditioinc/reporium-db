"""Local entry point that runs the REAL reporium_db pipeline against the local
mock GitHub GraphQL endpoint.

This is additive, local-only scaffolding. It does not modify any application,
production, or CI code. It simply overrides the module-level GRAPHQL_URL
constant in reporium_db.fetcher at runtime (before the CLI runs) so the real
fetch -> schedule -> diff -> partition -> generate pipeline reads from the
local mock instead of api.github.com. No GitHub token and no network egress
are required.

The optional reporium-events Pub/Sub publish in the real CLI is already wrapped
in try/except ImportError, so it is naturally skipped in this OSS-only
substrate when the package is not installed.

Usage:
    python -m local.runner sync
    python -m local.runner status

Env:
    MOCK_GRAPHQL_URL  GraphQL endpoint to target (default http://mock-github:8787/graphql)
"""

from __future__ import annotations

import os
import sys

MOCK_URL = os.getenv("MOCK_GRAPHQL_URL", "http://mock-github:8787/graphql")


def _point_fetcher_at_mock(url: str) -> None:
    """Override the fetcher's hardcoded GraphQL URL for local-only runs."""
    import reporium_db.fetcher as fetcher

    fetcher.GRAPHQL_URL = url
    print(f"[local.runner] reporium_db.fetcher.GRAPHQL_URL -> {url}", file=sys.stderr)


def main() -> None:
    # Provide harmless local defaults so the real config loader is satisfied
    # without any real secret. The mock ignores the token entirely.
    os.environ.setdefault("GH_TOKEN", "local-no-secret")
    os.environ.setdefault("GH_USERNAME", "perditioinc")

    _point_fetcher_at_mock(MOCK_URL)

    from reporium_db.__main__ import main as cli_main

    cli_main()


if __name__ == "__main__":
    main()
