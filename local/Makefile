# Local OSS dev substrate for reporium-db.
#
# $0 / OSS / local only. Brings up a deterministic mock of this repo's single
# cloud dependency (the GitHub GraphQL API) and runs the REAL pipeline against
# it on a clean, ephemeral scratch tree. Never touches the committed checkout
# (source is mounted read-only), never reaches api.github.com, uses no token.
#
# Targets:
#   up      - build images and start the mock GitHub service (waits for healthy)
#   seed    - validate the seed dataset and reset the scratch tree to clean state
#   migrate - run the real pipeline (fetch->schedule->diff->partition->generate)
#             on a clean scratch DB; this materializes the full dataset "schema"
#   smoke   - up + seed + migrate + assert all output objects + report PASS/FAIL
#   down    - stop and remove containers, network, and the scratch volume (-v)
#   logs    - tail mock-github logs
#
# Usage:  make -C local smoke     (or, from repo root: make smoke)

COMPOSE := docker compose -f docker-compose.yml
RUN_RUNNER := $(COMPOSE) run --rm -T runner

.PHONY: up seed migrate smoke down logs build

build:
	$(COMPOSE) build

up:
	$(COMPOSE) up -d --build --wait mock-github
	@echo "mock-github is up and healthy."

seed:
	@echo "Validating seed dataset and resetting clean scratch state..."
	$(RUN_RUNNER) /src/local/validate_seed.py
	$(RUN_RUNNER) /src/local/bootstrap_work.py
	@echo "Seed validated; clean /work prepared."

migrate:
	@echo "Running real pipeline on clean scratch DB (fetch->...->partition)..."
	$(RUN_RUNNER) /src/local/bootstrap_work.py
	$(RUN_RUNNER) -m local.runner sync
	@echo "Pipeline complete; dataset objects materialized in /work/data."

smoke: up seed
	@echo "=== SMOKE: clean-state pipeline run + object assertions ==="
	$(RUN_RUNNER) -m local.runner sync
	$(RUN_RUNNER) /src/local/smoke.py
	@echo "=== SMOKE: tearing down ==="
	$(MAKE) down
	@echo "=== SMOKE: PASS ==="

down:
	$(COMPOSE) down -v --remove-orphans

logs:
	$(COMPOSE) logs -f mock-github
