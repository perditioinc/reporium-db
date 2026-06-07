# Root passthrough to the local OSS dev substrate in local/.
#
# This is additive, local-only convenience. It does not change how the app, the
# nightly sync workflow, or CI run. All real work lives in local/Makefile.
#
#   make up       - start the local mock GitHub service
#   make seed     - validate seed + reset clean scratch state
#   make migrate  - run the real pipeline on a clean scratch DB
#   make smoke    - full clean-state run + assertions + teardown (PASS/FAIL)
#   make down     - tear everything down (containers, network, scratch volume)
#   make logs     - tail mock-github logs

.PHONY: up seed migrate smoke down logs build

up seed migrate smoke down logs build:
	$(MAKE) -C local $@
