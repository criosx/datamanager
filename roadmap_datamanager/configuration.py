from __future__ import annotations

import json
import os

from datetime import datetime
from dataclasses import dataclass, field
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Optional, Dict

from roadmap_datamanager import metadata as md
from roadmap_datamanager import datalad_gin_api as dgapi

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
    project: Optional[str] = None
    campaign: Optional[str] = None
    experiment: Optional[str] = None

    # MetaLad envelope defaults
    extractor_name: str = "datamanager_v1"
    extractor_version: str = "1.0"

    # Runtime knobs
    verbose: bool = True
    env: Dict[str, str] = field(default_factory=dict)

    # GIN repository
    GIN_url: Optional[str] = None
    GIN_repo: Optional[str] = None
    GIN_user: Optional[str] = None

    # Datamanager root directory
    dm_root: Optional[str] = None

def bootstrap_config(path, cfg):
    """
    Obtains config parameters from a path by walking up and identifying datasets. Requires a datamanager-compatible
    config dataclass with fields: root, user_name, user_email, project, campaign, and experiment.
    :param path: The starting path (ideally below-experiment)
    :param cfg: the configuation dataclass
    :return: the modifie configuation dataclass
    """

    bp = Path(str(path)).expanduser().resolve()
    while True:
        node_type, ds_path = dgapi.get_dataset_nodetype(bp)
        meta = md.Metadata(ds_path)
        metadata = meta.get()
        if node_type == 'root':
            cfg.dm_root = str(ds_path)
            cfg.user_name = metadata['user_name']
            cfg.user_email = metadata['user_email']
            break
        elif node_type == 'project':
            cfg.project = metadata['name']
        elif node_type == 'campaign':
            cfg.campaign = metadata['name']
        elif node_type == 'experiment':
            cfg.experiment = metadata['name']
        elif node_type == 'below-experiment':
            # any below-experiment content will still return the lowest hierarchy dataset, i.e, the experiment
            cfg.experiment = metadata['name']
        else:
            raise RuntimeError(f"Encountered unknown dataset type {node_type}. Cannot bootstrap datamanager.")
        bp = bp.parent
    return cfg


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

def save_persistent_cfg(data: dict | DataManagerConfig) -> None:

    def _make_json_safe(obj):
        # Ensure all values are JSON-safe
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _make_json_safe(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_make_json_safe(v) for v in obj]
        return obj

    cfg_path = default_config_path()
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    # Normalize input
    if is_dataclass(data):
        data = asdict(data)

    safe_data = _make_json_safe(data)
    cfg_path.write_text(json.dumps(safe_data, indent=2))
