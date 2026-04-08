from __future__ import annotations

import json
import os

from datetime import datetime
from dataclasses import dataclass, field, fields, MISSING
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Optional, Dict, ClassVar, Type, TypeVar

from roadmap_datamanager import metadata as md
from roadmap_datamanager import datalad_gin_api as dgapi

try:
    from platformdirs import user_config_dir
except ImportError:
    user_config_dir = None


T = TypeVar("T")


class ConfigError(Exception):
    pass


@dataclass
class BaseConfig:
    CONFIG_ENV_VAR: ClassVar[str] = "DEFAULT_CONFIG"
    CONFIG_APP_NAME: ClassVar[str] = "default"
    CONFIG_APP_AUTHOR: ClassVar[str] = "pyside"
    CONFIG_FILENAME: ClassVar[str] = "config.json"

    # Required identity
    user_name: str = 'default'
    user_email: str = ''

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

    # DataLad behavior
    use_datalad: bool = True

    # GIN repository
    use_GIN: bool = False
    GIN_url: str = 'gin.g-node.org'
    GIN_repo: str = 'datamanager'
    GIN_user: str = 'fhein'
    SSH_host_alias: str = 'gin.g-node.org'

    # Datamanager root directory
    dm_root: str = None


def _filter_to_dataclass_fields(data: dict[str, Any], config_cls: Type[T]) -> dict[str, Any]:
    result: dict[str, Any] = {}

    for f in fields(config_cls):
        if f.name in data:
            result[f.name] = data[f.name]
        else:
            # Use default value if available
            if f.default is not MISSING:  # type: ignore
                result[f.name] = f.default
            elif f.default_factory is not MISSING:  # type: ignore
                result[f.name] = f.default_factory()  # type: ignore
            # else: no default, leave it out so dataclass constructor can raise if needed

    return result


def _make_json_safe(obj):
    """
    Ensures all objectects are JSON safe.
    :param obj: an object to be serialized
    :return: the JSON safe object
    """
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _make_json_safe(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_json_safe(v) for v in obj]
    return obj


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


def default_config_path(
    *,
    env_var: str | None = None,
    app_name: str,
    app_author: str,
    filename: str = "config.json",
    fallback_dirname: str | None = None
) -> Path:
    """
    Returns the default config file path, checking environment variables, and platformdirs. Has a fallback.
    :param env_var: ENV variable
    :param app_name: the name of the app that saves the configuration
    :param app_author: the app author (i.e. streamlit, pyside)
    :param filename: the filename of the config file
    :param fallback_dirname: the fallback directory name under the user home directory
    :return: path to the default config file
    """
    if env_var:
        override = os.getenv(env_var)
        if override:
            return Path(override).expanduser()

    if user_config_dir:
        return Path(user_config_dir(app_name, app_author)) / filename

    fallback = fallback_dirname or f".{app_name}"
    return Path.home() / fallback / filename


def load_config(
    config_cls: Type[T],
    *,
    env_var: str | None = None,
    app_name: str,
    app_author: str,
    filename: str = "config.json",
    fallback_dirname: str | None = None,
) -> T:
    """
    Load config from disk and return a DataConfig instance. If no config file exists (or it cannot be parsed), returns
    a default DataConfig. Unknown keys in the JSON are ignored to allow schema evolution.
    :param config_cls: The type of the config class
    :param env_var: ENV variable
    :param app_name: the name of the app that saves the configuration
    :param app_author: the app author (i.e. streamlit, pyside)
    :param filename: the filename of the config file
    :param fallback_dirname: the fallback directory name under the user home directory
    :return: The loaded config dataclass
    """
    cfg_path = default_config_path(
        env_var=env_var,
        app_name=app_name,
        app_author=app_author,
        filename=filename,
        fallback_dirname=fallback_dirname,
    )
    if not cfg_path.exists():
        return config_cls()

    try:
        raw = json.loads(cfg_path.read_text())
    except (json.JSONDecodeError, OSError, NotADirectoryError):
        return config_cls()

    if not isinstance(raw, dict):
        return config_cls()

    filtered = _filter_to_dataclass_fields(raw, config_cls)
    return config_cls(**filtered)


def save_config(
    data: Any,
    *,
    env_var: str | None = None,
    app_name: str,
    app_author: str,
    filename: str = "config.json",
    fallback_dirname: str | None = None,
) -> Path:
    """
    Saves dataclass to a persistent config file.
    :param data: The dataclass to save
    :param env_var: ENV variable
    :param app_name: the name of the app that saves the configuration
    :param app_author: the app author (i.e. streamlit, pyside)
    :param filename: the filename of the config file
    :param fallback_dirname: the fallback directory name under the user home directory
    :return: the path to the saved config file
    """

    cfg_path = default_config_path(
        env_var=env_var,
        app_name=app_name,
        app_author=app_author,
        filename=filename,
        fallback_dirname=fallback_dirname,
    )
    cfg_path.parent.mkdir(parents=True, exist_ok=True)

    # Normalize input
    if is_dataclass(data):
        data = asdict(data)

    safe_data = _make_json_safe(data)
    cfg_path.write_text(json.dumps(safe_data, indent=2))
    return cfg_path


# Datmanager specific implementation, might be moved to different file in future

@dataclass
class DataManagerConfig(BaseConfig):
    CONFIG_ENV_VAR: ClassVar[str] = "ROADMAP_DM_CONFIG"
    CONFIG_APP_NAME: ClassVar[str] = "roadmap-datamanager"
    CONFIG_APP_AUTHOR: ClassVar[str] = "pyside"
    CONFIG_FILENAME: ClassVar[str] = "config.json"

def load_persistent_cfg() -> DataManagerConfig:
    config_cls = DataManagerConfig
    return load_config(
        config_cls,
        env_var=getattr(config_cls, "CONFIG_ENV_VAR", None),
        app_name=config_cls.CONFIG_APP_NAME,
        app_author=config_cls.CONFIG_APP_AUTHOR,
        filename=getattr(config_cls, "CONFIG_FILENAME", "config.json"),
    )

def save_persistent_cfg(data: Any) -> Path:
    cls = type(data)
    return save_config(
        data,
        env_var=getattr(cls, "CONFIG_ENV_VAR", None),
        app_name=cls.CONFIG_APP_NAME,
        app_author=cls.CONFIG_APP_AUTHOR,
        filename=getattr(cls, "CONFIG_FILENAME", "config.json"),
    )