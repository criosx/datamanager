# datamanager.py
from __future__ import annotations

import json
import os
import subprocess
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Callable, Tuple

# DataLad Python API
from datalad import api as dl
from datalad.distribution.dataset import Dataset


# ---------------------------- Config container ---------------------------- #

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
    datalad_profile: Optional[str] = "text2git"  # None to disable profile on create

    # MetaLad envelope defaults
    extractor_name: str = "scidata_node_v1"
    extractor_version: str = "1.0"

    # Runtime knobs
    verbose: bool = True
    env: Dict[str, str] = field(default_factory=dict)
    register_existing: bool = True

    # Clock (for tests)
    now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc)


# -------------------------------- Manager -------------------------------- #

class DataManager:
    """
    Create (user)/(project)/(campaign) as nested DataLad datasets and attach JSON-LD
    using MetaLad. Prefers DataLad's Python API; falls back to CLI for meta-add.
    """

    def __init__(
        self,
        root: os.PathLike | str,
        user_name: str,
        user_email: str,
        *,
        user_id: Optional[str] = None,
        organization: Optional[str] = None,
        lab_group: Optional[str] = None,
        default_project: Optional[str] = None,
        default_campaign: Optional[str] = None,
        datalad_profile: Optional[str] = "text2git",
        extractor_name: str = "scidata_node_v1",
        extractor_version: str = "1.0",
        verbose: bool = True,
        env: Optional[Dict[str, str]] = None,
        register_existing: bool = True,
        now_fn: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.root: Path = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

        self.cfg = DataManagerConfig(
            user_name=user_name, user_email=user_email,
            user_id=user_id, organization=organization, lab_group=lab_group,
            default_project=default_project, default_campaign=default_campaign,
            datalad_profile=datalad_profile,
            extractor_name=extractor_name, extractor_version=extractor_version,
            verbose=verbose, env=env or {},
            register_existing=register_existing, now_fn=now_fn,
        )

        if self.cfg.verbose:
            print(f"[DataManager] root={self.root}")
            print(f"[DataManager] user={self.cfg.user_name} <{self.cfg.user_email}>")
            if self.cfg.organization or self.cfg.lab_group:
                print(f"[DataManager] org={self.cfg.organization or '-'} lab={self.cfg.lab_group or '-'}")
            if self.cfg.default_project or self.cfg.default_campaign:
                print(f"[DataManager] defaults: project={self.cfg.default_project} "
                      f"campaign={self.cfg.default_campaign}")
            print(f"[DataManager] profile={self.cfg.datalad_profile or '(none)'} ")

    # ---------------------------- Core helpers ---------------------------- #

    def _ensure_dataset(self, path: Path, node_type, name, superds: Optional[Path]) -> None:
        """
        If dataset at `path` exists, (optionally) ensure it's registered in `superds`.
        Otherwise, create it (registered when superds is given).
        """
        path = Path(path).resolve()
        ds = Dataset(str(path))

        if ds.is_installed():
            if superds and self.cfg.register_existing:
                self._register_existing(Dataset(str(superds)), ds)
            return

        # Create (and register if superds is provided)
        if superds is None:
            # top-level dataset
            dl.create(path=str(path), cfg_proc=self.cfg.datalad_profile)
        else:
            # create and register as subdataset of superds in one API call
            dl.create(path=str(path), dataset=str(superds), cfg_proc=self.cfg.datalad_profile)
        self._save_meta(path, node_type=node_type, name=name)

    def _register_existing(self, superds: Dataset, child: Dataset) -> None:
        """Register an already-instantiated child dataset in its superdataset."""
        # ensure child is actually inside superds
        self._ensure_is_subpath(Path(child.pathobj), Path(superds.pathobj))

        # If already registered, this is a no-op (status=notneeded)
        # TODO: That might not be true -> check.
        dl.subdatasets(dataset=str(superds.path),
                       path=[str(Path(child.path).relative_to(superds.path))],
                       on_failure="ignore",
                       return_type="list")
        dl.save(dataset=str(superds.path),
                message=f"Register existing subdataset {os.path.relpath(child.path, superds.path)}")

    # --------------------------- Metadata helpers ------------------------- #
    def _save_meta(self, ds_path: Path, *, node_type: str, name: str) -> None:
        """Attach JSON-LD at dataset level using MetaLad (CLI)."""

        ds = Dataset(str(ds_path))
        if not ds.is_installed():
            raise RuntimeError(f"Dataset not installed at {ds_path}")

        dataset_id = self._get_dataset_id(ds)
        dataset_version = self._get_dataset_version(ds)
        extraction_time = self.cfg.now_fn().replace(microsecond=0).isoformat()

        payload = {
            "type": "dataset",
            "extractor_name": self.cfg.extractor_name,
            "extractor_version": self.cfg.extractor_version,
            "extraction_parameter": {"node_type": node_type, "name": name},
            "extraction_time": extraction_time,
            "agent_name": self.cfg.user_name,
            "agent_email": self.cfg.user_email,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "extracted_metadata": {
                "@context": {"@vocab": "http://schema.org/", "scidata": "https://example.org/scidata#"},
                "@type": "Dataset",
                "name": name,
                "scidata:nodeType": node_type,
            },
        }

        p = subprocess.Popen(
            ["datalad", "meta-add", "-d", str(ds_path), "-"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=self._proc_env()
        )
        out, err = p.communicate(json.dumps(payload))
        if p.returncode != 0:
            raise RuntimeError(f"meta-add failed for {ds_path} ({node_type}={name}): {err.strip()}")

        # Commit metadata in this dataset (even if not yet registered by parent)
        dl.save(dataset=str(ds_path), message=f"scidata: metadata for {node_type}={name}")

    # ------------------------------ Utilities ---------------------------- #

    def _get_dataset_id(self, ds: Dataset) -> str:
        # Prefer DataLad property; fallback to reading .datalad/config via Git if needed
        if getattr(ds, "id", None):
            return ds.id
        # Rare fallback
        p = subprocess.run(
            ["git", "-C", ds.path, "config", "-f", str(Path(ds.path) / ".datalad/config"),
             "datalad.dataset.id"], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self._proc_env()
        )
        if p.returncode == 0 and p.stdout.strip():
            return p.stdout.strip()
        raise RuntimeError(f"Could not read dataset id in {ds.path}")

    def _get_dataset_version(self, ds: Dataset) -> str:
        try:
            return ds.repo.get_hexsha()
        except Exception:
            # Ensure there is at least one commit to reference
            dl.save(dataset=str(ds.path), message="Initial commit (auto)")
            return ds.repo.get_hexsha()

    @staticmethod
    def _ensure_is_subpath(child: Path, parent: Path) -> None:
        child = child.resolve()
        parent = parent.resolve()
        try:
            child.relative_to(parent)
        except ValueError:
            raise RuntimeError(f"{child} is not inside super dataset {parent}")

    def _proc_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(self.cfg.env)
        # Example: enforce non-interactive Git
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    # ----------------------------- Public API ----------------------------- #

    def init_tree(self, *, project: Optional[str] = None, campaign: Optional[str] = None,
                  experiment: Optional[str] = None) -> None:
        """
        Ensure the (user)/(project)/(campaign) dataset tree exists and is registered.
        Attach minimal JSON-LD at each level. Idempotent.
        """

        up = self.root
        pp = up / project if project else None
        cp = pp / campaign if (pp and campaign) else None
        ep = cp / experiment if (cp and experiment) else None

        # Ensure/create datasets
        self._ensure_dataset(up, superds=None, node_type="user", name=self.cfg.user_name)

        if pp:
            self._ensure_dataset(pp, superds=up, node_type="project", name=project)
        if cp:
            self._ensure_dataset(cp, superds=pp, node_type="campaign", name=campaign)
        if ep:
            self._ensure_dataset(ep, superds=cp, node_type="experiment", name=experiment)

        # Record state at the top-level, recursing into registered subs
        dl.save(dataset=str(up), recursive=True,
                message=f"scidata: initialized tree for {self.cfg.user_name}/{project or ''}/{campaign or ''}")
        if self.cfg.verbose:
            print(f"[scidata] initialized/verified tree at {up} for "
                  f"{self.cfg.user_name}/" + "/".join(x for x in (project, campaign) if x))

