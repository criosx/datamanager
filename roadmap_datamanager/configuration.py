import json
import os

from datetime import datetime, timezone
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, Callable

try:
    from platformdirs import user_config_dir
except ImportError:
    user_config_dir = None


@dataclass
class DataManagerConfig:
    # Required identity
    user_name: str
    user_email: str

    # Optional identity/context
    user_id: Optional[str] = None
    organization: Optional[str] = None
    lab_group: Optional[str] = None

    # Defaults
    default_project: Optional[str] = None
    default_campaign: Optional[str] = None

    # DataLad behavior
    datalad_profile: Optional[str] = None

    # MetaLad envelope defaults
    extractor_name: str = "datamanager_v1"
    extractor_version: str = "1.0"

    # Runtime knobs
    verbose: bool = True
    env: Dict[str, str] = field(default_factory=dict)
    register_existing: bool = True

    # Clock (for tests)
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc)

    # GIN repository
    GIN_url: Optional[str] = None
    GIN_repo: Optional[str] = None
    GIN_user: Optional[str] = None

    # Datamanager root directory
    dm_root: Optional[str] = None


def default_config_path() -> Path:
    # env override
    override = os.getenv("ROADMAP_DM_CONFIG")
    if override:
        return Path(override).expanduser()
    if user_config_dir:
        return Path(user_config_dir("roadmap-datamanager", "roadmap")) / "config.json"
    # fallback
    return Path.home() / ".roadmap_datamanager" / "config.json"


def load_persistent_cfg() -> dict:
    cfg_path = default_config_path()
    if not cfg_path.exists():
        return {}
    try:
        return json.loads(cfg_path.read_text())
    except NotADirectoryError:
        return {}


def save_persistent_cfg(data: dict) -> None:
    cfg_path = default_config_path()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)
    cfg_path.write_text(json.dumps(data, indent=2))
