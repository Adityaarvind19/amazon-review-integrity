from __future__ import annotations

import copy
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.toml"


def _merge(base: dict, over: dict) -> dict:
    for k, v in over.items():
        if isinstance(v, dict) and isinstance(base.get(k), dict):
            _merge(base[k], v)
        else:
            base[k] = v
    return base


def load_config(path: str | Path | None = None, **path_overrides) -> dict:
    """Load config/pipeline.toml, then deep-merge an optional overlay file (e.g. config/sample.toml)."""
    with open(DEFAULT_CONFIG, "rb") as f:
        cfg = tomllib.load(f)
    if path and Path(path).resolve() != DEFAULT_CONFIG.resolve():
        with open(path, "rb") as f:
            cfg = _merge(cfg, tomllib.load(f))
    cfg = copy.deepcopy(cfg)
    for key, val in path_overrides.items():
        if val is not None:
            cfg["paths"][key] = str(val)
    for key, val in cfg["paths"].items():
        p = Path(val)
        cfg["paths"][key] = str(p if p.is_absolute() else (REPO_ROOT / p))
    return cfg


def lake(cfg: dict, *parts: str) -> str:
    return str(Path(cfg["paths"]["lake_dir"], *parts))
