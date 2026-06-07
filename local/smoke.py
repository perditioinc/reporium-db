"""Smoke test for the local OSS substrate.

Runs inside the runner container AFTER `python -m local.runner sync` has
produced output. Asserts that the real pipeline materialized the full set of
output "objects" (the dataset schema this repo is responsible for) on a clean
workspace, sourced entirely from the local mock GitHub endpoint.

For this repo the analogue of "schema / objects exist" is the partitioned
dataset layout: index.json (with meta/categories/languages), recent.json,
top_starred.json, by_language/*, by_category/*, full/repos_NNNN.json, plus the
diff/scheduling side outputs pending_enrichment.json and schedule.json.

Exit code 0 = PASS, non-zero = FAIL.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

DATA = Path("data")
failures: list[str] = []
checks = 0


def check(cond: bool, msg: str) -> None:
    global checks
    checks += 1
    if cond:
        print(f"  PASS  {msg}")
    else:
        print(f"  FAIL  {msg}")
        failures.append(msg)


def load(path: Path):
    return json.loads(path.read_text())


print("=== reporium-db local substrate smoke ===")

# 1. index.json exists with the expected top-level schema.
idx_path = DATA / "index.json"
check(idx_path.exists(), "data/index.json exists")
if idx_path.exists():
    idx = load(idx_path)
    check("meta" in idx, "index.json has meta")
    check("categories" in idx, "index.json has categories")
    check("languages" in idx, "index.json has languages")
    meta = idx.get("meta", {})
    total = meta.get("total", 0)
    check("total" in meta and "last_updated" in meta and "version" in meta,
          "index.meta has total/last_updated/version")
    check(total > 0, f"index.meta.total > 0 (got {total})")
    check(len(idx.get("languages", {})) > 0, "index.languages is non-empty")
    check(len(idx.get("categories", {})) > 0, "index.categories is non-empty")

# 2. recent.json and top_starred.json exist and are JSON lists.
for name in ("recent.json", "top_starred.json"):
    p = DATA / name
    check(p.exists(), f"data/{name} exists")
    if p.exists():
        check(isinstance(load(p), list), f"data/{name} is a JSON array")

# 3. top_starred is actually sorted by stars descending (real pipeline behavior).
ts_path = DATA / "top_starred.json"
if ts_path.exists():
    ts = load(ts_path)
    stars = [r.get("stars", 0) for r in ts]
    check(stars == sorted(stars, reverse=True), "top_starred.json sorted by stars desc")
    check(len(ts) > 0, "top_starred.json non-empty")

# 4. Partitioned subtrees exist and contain at least one file each.
for sub in ("by_language", "by_category", "full"):
    d = DATA / sub
    check(d.is_dir(), f"data/{sub}/ exists")
    if d.is_dir():
        files = list(d.glob("*.json"))
        check(len(files) > 0, f"data/{sub}/ has at least one partition file")

# 5. full/repos_0000.json is the first full partition and is a list.
full0 = DATA / "full" / "repos_0000.json"
check(full0.exists(), "data/full/repos_0000.json exists")
if full0.exists():
    rows = load(full0)
    check(isinstance(rows, list) and len(rows) > 0, "full/repos_0000.json non-empty array")
    if rows:
        row = rows[0]
        required = {"nameWithOwner", "name", "stars", "forks", "primaryLanguage", "topics"}
        check(required.issubset(row.keys()),
              f"repo record carries required fields {sorted(required)}")

# 6. by_language has an 'unknown' bucket (the empty/no-language seed repo proves
#    the real fallback path ran).
check((DATA / "by_language" / "unknown.json").exists(),
      "by_language/unknown.json exists (no-language fallback exercised)")

# 7. Diff + scheduling side outputs.
pe_path = DATA / "pending_enrichment.json"
check(pe_path.exists(), "data/pending_enrichment.json exists")
if pe_path.exists():
    pe = load(pe_path)
    check("generated_at" in pe and "repos" in pe, "pending_enrichment has generated_at/repos")
    # On a clean run every seed repo is 'new', so enrichment list is non-empty.
    check(isinstance(pe.get("repos"), list) and len(pe["repos"]) > 0,
          "pending_enrichment.repos non-empty on clean run (all new)")

sched_path = Path("schedule.json")
check(sched_path.exists(), "schedule.json exists")
if sched_path.exists():
    sched = load(sched_path)
    check(isinstance(sched, dict) and len(sched) > 0, "schedule.json has entries")
    tiers = {v.get("tier") for v in sched.values()}
    check(tiers.issubset({"nightly", "weekly", "monthly"}) and len(tiers) >= 1,
          f"schedule tiers are valid (got {sorted(tiers)})")

# 8. Generated docs from the real generator.
check(Path("LAST_RUN.md").exists(), "LAST_RUN.md regenerated")
check(Path("README.md").exists(), "README.md regenerated")

print(f"\n=== {checks - len(failures)}/{checks} checks passed ===")
if failures:
    print("SMOKE: FAIL")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)

print("SMOKE: PASS")
sys.exit(0)
