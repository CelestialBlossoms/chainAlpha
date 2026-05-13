from __future__ import annotations

import os
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent


def load_env_file(path: str | Path, *, override: bool = False) -> None:
    env_path = Path(path)
    if not env_path.is_absolute():
        env_path = ROOT_DIR / env_path
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and (override or key not in os.environ):
            os.environ[key] = value


def load_project_env() -> None:
    load_env_file(".env")
    load_env_file(os.getenv("GMGN_CLI_ENV_FILE", "gmgn_account_2/.env"))
