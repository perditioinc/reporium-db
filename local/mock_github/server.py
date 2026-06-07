"""Local OSS stand-in for the GitHub GraphQL API.

This is part of the $0 local-only dev substrate. It serves the exact GraphQL
response shape that reporium_db.fetcher expects, sourced from a deterministic
seed file (local/seed/repos.json) instead of the live GitHub GraphQL API. No
token, no network, no cost.

It is intentionally dependency-free (Python stdlib only) so the substrate has
no third-party install footprint of its own. It does NOT validate the GraphQL
query string; it only needs to honor the `first`/`after` cursor pagination that
the fetcher drives, and return the node fields the fetcher reads.

Run:  python -m local.mock_github.server   (host 0.0.0.0, port 8787 by default)
Env:  MOCK_PORT, MOCK_SEED (path to seed json), MOCK_PAGE_SIZE
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [mock-github] %(message)s")
logger = logging.getLogger("mock_github")

SEED_PATH = Path(os.getenv("MOCK_SEED", "/seed/repos.json"))
PAGE_SIZE = int(os.getenv("MOCK_PAGE_SIZE", "2"))
PORT = int(os.getenv("MOCK_PORT", "8787"))

# Relative-date tokens in the seed get resolved to real ISO timestamps at load
# time so the seed stays deterministic relative to "now" and exercises every
# schedule tier and the recent-window logic regardless of the calendar date.
_NOW = datetime.now(timezone.utc)
_DATE_TOKENS = {
    "RECENT": (_NOW - timedelta(days=1)).isoformat(),
    "WEEKLY": (_NOW - timedelta(days=90)).isoformat(),
    "MONTHLY": (_NOW - timedelta(days=800)).isoformat(),
}


def _resolve_pushed(value):
    if value is None:
        return None
    return _DATE_TOKENS.get(value, value)


def _load_nodes() -> list[dict]:
    """Load the seed file and project each entry into a GraphQL repo node."""
    raw = json.loads(SEED_PATH.read_text())
    nodes = []
    for r in raw.get("repos", []):
        pushed = _resolve_pushed(r.get("pushedAt"))
        lang = r.get("language")
        nodes.append(
            {
                "nameWithOwner": r["nameWithOwner"],
                "name": r["name"],
                "description": r.get("description"),
                "stargazerCount": r.get("stars", 0),
                "forkCount": r.get("forks", 0),
                "primaryLanguage": ({"name": lang} if lang else None),
                "pushedAt": pushed,
                "updatedAt": pushed,
                "createdAt": "2024-01-01T00:00:00Z",
                "isArchived": False,
                "isFork": r.get("isFork", False),
                "isEmpty": pushed is None,
                "isPrivate": r.get("isPrivate", False),
                "parent": None,
                "repositoryTopics": {
                    "nodes": [{"topic": {"name": t}} for t in r.get("topics", [])]
                },
                "licenseInfo": {"name": r.get("license", "MIT")},
                "issues": {"totalCount": r.get("openIssues", 0)},
                "defaultBranchRef": {"name": r.get("defaultBranch", "main")},
            }
        )
    return nodes


_NODES = _load_nodes()
logger.info("Loaded %d seed repos from %s (page size %d)", len(_NODES), SEED_PATH, PAGE_SIZE)


def _page(after: str | None) -> dict:
    """Return one cursor page of nodes plus the rateLimit block.

    Cursors are simple integer offsets encoded as 'cursor:<n>'. The fetcher
    treats them opaquely, so any stable scheme works.
    """
    start = 0
    if after:
        try:
            start = int(after.split(":", 1)[1])
        except (IndexError, ValueError):
            start = 0

    chunk = _NODES[start : start + PAGE_SIZE]
    end = start + len(chunk)
    has_next = end < len(_NODES)
    end_cursor = f"cursor:{end}"

    return {
        "data": {
            "repositoryOwner": {
                "repositories": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": end_cursor},
                    "nodes": chunk,
                }
            },
            "rateLimit": {
                "remaining": 5000,
                "resetAt": (_NOW + timedelta(hours=1)).isoformat(),
                "cost": 1,
            },
        }
    }


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # quieter default logging
        logger.info("%s", fmt % args)

    def _send_json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        # Lightweight health endpoint for compose healthcheck / make wait.
        if self.path in ("/health", "/healthz"):
            self._send_json({"status": "ok", "repos": len(_NODES)})
            return
        self._send_json({"message": "mock github graphql; POST to /graphql"}, status=404)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            req = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            req = {}
        variables = req.get("variables", {}) or {}
        after = variables.get("after")
        self._send_json(_page(after))


def main() -> None:
    server = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    logger.info("Mock GitHub GraphQL listening on 0.0.0.0:%d", PORT)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
