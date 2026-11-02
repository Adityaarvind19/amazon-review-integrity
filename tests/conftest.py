import os
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ["TZ"] = "UTC"  # test datetimes are naive; make them UTC regardless of the laptop's zone
time.tzset()


@pytest.fixture(scope="session")
def make_cfg(tmp_path_factory):
    from revint.config import load_config

    def _make(name: str):
        base = tmp_path_factory.mktemp(name)
        cfg = load_config(ROOT / "config" / "sample.toml", raw_dir=base / "raw", lake_dir=base / "lake",
                          state_dir=base / "state", results_dir=base / "results")
        return cfg
    return _make


@pytest.fixture(scope="session")
def spark(make_cfg):
    from revint.spark import get_spark
    s = get_spark(make_cfg("spark_base"))
    yield s
    s.stop()
