PY ?= python
.PHONY: setup sample test lint run-sample real clean
setup:
	$(PY) -m pip install -r requirements.txt && $(PY) -m pip install -e .
sample:
	$(PY) -m revint.sample --out data/sample/raw --reviews 15000
run-sample: sample
	$(PY) -m revint --config config/sample.toml run --publish
	$(PY) -m revint --config config/sample.toml eval --category All_Beauty --seeds 11 --campaigns-per-type 4 --work-dir data/sample/eval
	$(PY) -m revint --config config/sample.toml sql
	$(PY) -m revint --config config/sample.toml dashboard
test:
	$(PY) -m pytest
lint:
	ruff check src tests
real:
	scripts/run_real.sh
clean:
	rm -rf data/sample data/lake data/state data/eval spark-warehouse
