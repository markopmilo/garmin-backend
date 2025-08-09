"""
Paths, default config, and a couple tiny helpers.
"""
from __future__ import annotations
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

APP_ROOT = Path(__file__).resolve().parent
UPDATE_LOG = APP_ROOT / "update.log"

HOME = Path.home()
DATA_ROOT = HOME / "HealthData"
DB_PATH = DATA_ROOT / "DBs" / "garmin.db"

REQUIRED_PATHS: List[Path] = [
    DATA_ROOT / "DBs",
    DATA_ROOT / "FitFiles" / "Activities",
    DATA_ROOT / "FitFiles" / "Monitoring",
    DATA_ROOT / "Plugins",
    DATA_ROOT / "Sleep",
]

CFG_PATH = HOME / ".GarminDb" / "GarminConnectConfig.json"
DEFAULT_CFG: Dict[str, Any] = {
    "db": {"type": "sqlite"},
    "garmin": {"domain": "garmin.com"},
    "credentials": {
        "user": "",
        "secure_password": False,
        "password": "",
        "password_file": None,
    },
    "data": {
        "weight_start_date": "",
        "sleep_start_date": "",
        "rhr_start_date": "",
        "monitoring_start_date": "",
        "download_latest_activities": 25,
        "download_all_activities": 1000,
    },
}

# path to the installed garmindb cli (next to interpreter)
CLI = str(Path(sys.executable).parent / "garmindb_cli.py")


def ensure_healthdata_tree() -> list[str]:
    """Create expected ~/HealthData tree; return created path strings."""
    created: list[str] = []
    DATA_ROOT.mkdir(parents=True, exist_ok=True)
    for p in REQUIRED_PATHS:
        if not p.exists():
            p.mkdir(parents=True, exist_ok=True)
            created.append(str(p))
    return created


def read_cfg() -> Dict[str, Any]:
    """Read ~/.GarminDb/GarminConnectConfig.json, writing default if missing."""
    CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not CFG_PATH.exists():
        write_cfg(DEFAULT_CFG.copy())
        return DEFAULT_CFG.copy()
    return json.loads(CFG_PATH.read_text(encoding="utf-8"))


def write_cfg(cfg: Dict[str, Any]) -> None:
    """Write config and try to chmod 0600."""
    CFG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CFG_PATH.write_text(json.dumps(cfg, indent=4), encoding="utf-8")
    try:
        os.chmod(CFG_PATH, 0o600)
    except Exception:
        pass


def create_dirs_if_needed() -> None:
    """Idempotent: make tree + config if missing."""
    ensure_healthdata_tree()
    if not CFG_PATH.exists():
        write_cfg(DEFAULT_CFG.copy())
