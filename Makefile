SCRIPT_DIR := $(shell dirname $(realpath $(lastword $(MAKEFILE_LIST))))

SHELL = /bin/bash

# highlight the Makefile targets
# @grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: help
.DEFAULT_GOAL=help
help:  ## help for this Makefile
	@grep -E '^[a-zA-Z0-9_\-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "%-30s %s\n", $$1, $$2}'

.PHONY: tmux
tmux:  ## run tmux
	tmuxp load .tmuxp.yaml

.PHONY: devenv-pre-commit
devenv-pre-commit:  ## run devenv pre-commit
	devenv test

.PHONY: black
black:  ## run black to format code
	poetry run black -l79 src

.PHONY: isort
isort:  ## run isort to order imports
	poetry run isort src

.PHONY: ray-head-start
ray-head-start:  ## start ray head
	poetry run ray start --head --port=6379 --disable-usage-stats

.PHONY: ray-head-stop
ray-head-stop:  ## stop ray head
	poetry run ray stop

.PHONY: env-up
env-up:
	poetry install

.PHONY: env-rm
env-rm:
	poetry env remove $$(poetry env info -e)

.PHONY: duckdb-tutorial
duckdb-tutorial: env-up  ## duckdb tutorial
	poetry run python src/duckdb-tutorial.py

.PHONY: duckdb-billion
duckdb-billion: env-up  ## duckdb with a billion rows
	poetry run python src/duckdb-billion.py

.PHONY: polars-tutorial
polars-tutorial: ## polars tutorial
	poetry run python src/$@.py

.PHONY: polars-billion
polars-billion:  ## polars with a billion rows
	poetry run python src/$@.py

.PHONY: polars-sample-slow
polars-sample-slow:  ## polars sample timing
	poetry run python src/$@.py

.PHONY: ray-task
ray-task:  ## ray core: task example
	poetry run python src/ray-core.py $@

.PHONY: ray-actor
ray-actor:  ## ray core: actor example
	poetry run python src/ray-core.py $@

.PHONY: ray-tips
ray-tips:  ## ray tips examples
	poetry run python src/$@.py

.PHONY: ray-env
ray-env:  ## setup ray environment
	poetry run python src/$@.py

.PHONY: ray-data-load
ray-data-load:  ## load data with ray
	poetry run python src/ray-data.py $@

.PHONY: ray-data-inspect
ray-data-inspect:  ## inspect data with ray
	poetry run python src/ray-data.py $@

.PHONY: ray-data-transform
ray-data-transform:  ## transform data with ray
	poetry run python src/ray-data.py $@

.PHONY: clean
clean: env-rm  ## remove temporary files
	rm -f poetry.lock
	find . -name '.pytest_cache' -type d -exec rm -rf '{}' +
	find . -name '__pycache__' -type d -exec rm -rf '{}' +
