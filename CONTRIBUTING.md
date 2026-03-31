# Contributing to reporium-db

## Branch model

| Branch | Purpose |
|--------|---------|
| `main` | Stable, deployable. Every commit should be production-ready. |
| `dev`  | Integration branch. All feature/fix work merges here first. |
| `feature/<name>` or `fix/<name>` | Short-lived branches cut from `dev`. |

### Workflow

```
dev → feature/my-fix → PR to dev → merge → PR dev→main → merge → release tag
```

1. Cut a branch from `dev`:
   ```bash
   git checkout dev && git pull
   git checkout -b fix/my-issue-name
   ```

2. Make changes, write tests, update CHANGELOG under an `[Unreleased]` section.

3. Open a PR targeting `dev`. Title format: `fix: short description (#issue)` or `feat: ...`

4. Once `dev` is stable and tested, open a PR from `dev → main`.

5. After merging to `main`, create a GitHub release with a semver tag (`v1.x.x`).

## Versioning

Follows [Semantic Versioning](https://semver.org/):

- `PATCH` — bug fixes, no API changes (`v1.0.x`)
- `MINOR` — new features, backwards compatible (`v1.x.0`)
- `MAJOR` — breaking API changes (`vX.0.0`)

## CHANGELOG

Every PR must update `CHANGELOG.md` (if the repo has one):
- Add entries under `## [Unreleased]` during development
- Rename the section to `## [X.Y.Z] - YYYY-MM-DD` when tagging a release

## Tests

All new features and bug fixes must include tests. Tests must pass before merging to `dev`.

## GitHub Issues

- Use issues to track all bugs, features, and security concerns
- Link PRs to issues: `Closes #123` in the PR body
- Label issues: `bug`, `feature`, `security`, `docs`, `ci`
