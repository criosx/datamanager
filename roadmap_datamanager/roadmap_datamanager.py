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

# --------------------------- Install policy --------------------------- #


ALLOWED_CATEGORIES = [
    "raw", "reduced", "measurement", "analysis",
    "template", "experimental_optimization", "model",
]


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

    def _ensure_experiment_category(
        self, project: Optional[str], campaign: Optional[str], experiment: str, category: str
    ) -> Tuple[Path, Path]:
        """
        Ensure experiment dataset exists and a category dataset directly under it.
        Returns (experiment_path, category_dataset_path).
        """
        up = self.root
        pp = up / project if project else None
        cp = pp / campaign if (pp and campaign) else None
        ep = cp / experiment if (cp and experiment) else None
        if ep is None:
            raise ValueError("experiment requires project and campaign to be set")

        # Make sure these are datasets and registered
        self._ensure_dataset(up, superds=None, node_type="user", name=self.cfg.user_name)
        if pp: self._ensure_dataset(pp, superds=up, node_type="project", name=pp.name)
        if cp: self._ensure_dataset(cp, superds=pp, node_type="campaign", name=cp.name)
        self._ensure_dataset(ep, superds=cp, node_type="experiment", name=ep.name)

        cat = ep / category
        self._ensure_dataset(cat, superds=ep, node_type="category", name=category)
        return ep, cat

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

    def _install_file_into_dataset(self, src: Path, ds_path: Path, *, move: bool) -> Path:
        """
        Place a single file into the given dataset and save it.
        """
        ds = Dataset(str(ds_path))
        if not ds.is_installed():
            raise RuntimeError(f"target dataset not installed at {ds_path}")

        dst = ds_path / src.name
        dst.parent.mkdir(parents=True, exist_ok=True)
        if move:
            __import__("shutil").move(str(src), str(dst))
        else:
            __import__("shutil").copy2(str(src), str(dst))

        dl.save(dataset=str(ds_path), path=[str(dst)], message=f"Add file {dst.name}")
        return dst

    def _install_folder_as_datasets(self, src_dir: Path, cat_or_target_ds_path: Path, *, name: Optional[str],
                                     move: bool) -> Path:
        """
        Recursively create a dataset hierarchy mirroring src_dir under cat_or_target_ds_path.
        Each directory becomes a subdataset; files go into their directory’s dataset.
        Returns the path of the top dataset created for the folder.
        """
        top_name = name or src_dir.name
        top_path = cat_or_target_ds_path / top_name

        self._ensure_dataset(top_path, superds=cat_or_target_ds_path, node_type="dataset", name=top_name)

        for root, dirs, files in os.walk(src_dir):
            rel = Path(root).relative_to(src_dir)
            parent_ds_path = (top_path / rel).resolve()

            if rel != Path("."):
                self._ensure_dataset(parent_ds_path, superds=parent_ds_path.parent, node_type="dataset", name=rel.name)

            if files:
                for fn in files:
                    s = Path(root) / fn
                    d = parent_ds_path / fn
                    d.parent.mkdir(parents=True, exist_ok=True)
                    if move:
                        __import__("shutil").move(str(s), str(d))
                    else:
                        __import__("shutil").copy2(str(s), str(d))
                dl.save(dataset=str(parent_ds_path), message=f"Add files in {rel or top_name}")

            for dname in list(dirs):
                sub_path = parent_ds_path / dname
                self._ensure_dataset(sub_path, superds=parent_ds_path, node_type="dataset", name=dname)

        dl.save(dataset=str(cat_or_target_ds_path), recursive=True,
                message=f"Register imported folder {top_name} as dataset hierarchy")
        return top_path

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

    def _resolve_existing_target_below_category(self, *, project: Optional[str], campaign: Optional[str],
                                                experiment: str, category: str, dest_rel: Path) -> (
                                                Tuple)[Path, Path, Path]:
        """
        Returns (experiment_path, category_ds_path, target_ds_path) and verifies:
          - experiment, category are *installed* datasets
          - target_ds_path = category/dest_rel is an *installed* dataset
        No dataset creation is performed here.
        """
        up = self.root
        pp = up / project if project else None
        cp = pp / campaign if (pp and campaign) else None
        ep = cp / experiment if (cp and experiment) else None
        if ep is None:
            raise ValueError("experiment requires project and campaign to be set")

        cat = (ep / category).resolve()

        # Only *check* (no creation) for existence/installed state
        if not Dataset(str(ep)).is_installed():
            raise RuntimeError(f"experiment dataset not installed at {ep}")
        if not Dataset(str(cat)).is_installed():
            raise RuntimeError(f"category dataset not installed at {cat}")

        target = (cat / dest_rel).resolve()
        self._ensure_is_subpath(target, cat)
        if not Dataset(str(target)).is_installed():
            raise RuntimeError(f"target dataset not installed at {target} (dest_rel='{dest_rel}')")

        return ep, cat, target

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

    def install_into_tree(self, source: os.PathLike | str, *, project: Optional[str], campaign: Optional[str],
                          experiment: str, category: str, dest_rel: Optional[os.PathLike | str] = None,
                          name: Optional[str] = None, move: bool = False, metadata: Optional[Dict[str, Any]]
                          = None) -> Path:
        """
        Install a file or folder into {root}/{project}/{campaign}/{experiment}/{category}
        or into an *existing* dataset below the category when dest_rel is given.

        Rules:
          - Never install directly under the experiment root but into a predefined category.
          - For files: add to the chosen dataset and save.
          - For folders: create subdatasets recursively under the chosen dataset.
          - Attach dataset-level metadata (includes file/folder name in .name field).
        :param source: (str or path) source director of the file or folder to install.
        :param project: (str) project identifier for target destination
        :param campaign: (str) campaign identifier for target destination
        :param experiment: (str) experiment identifier for target destination
        :param category: (str) category for target destination
        :param dest_rel: (str or path) relative path to destination folder from category
        :param name: (str) name under which the file or folder will be installed.
        :param move: (bool) move or copy file or folder
        :param metadata: (json) additional metadata to add to file or folder (dataset).
        :return:
        """

        src = Path(source).expanduser().resolve()
        if category not in ALLOWED_CATEGORIES:
            raise ValueError(f"category must be one of {ALLOWED_CATEGORIES}, got {category!r}")
        if not src.exists():
            raise FileNotFoundError(src)

        # If dest_rel is provided: require that the target dataset already exists.
        if dest_rel is not None:
            dest_rel = Path(dest_rel)
            ep, cat_ds_path, target_ds_path = self._resolve_existing_target_below_category(
                project=project, campaign=campaign, experiment=experiment, category=category, dest_rel=dest_rel
            )
            # Install into target_ds_path (no creation of the target or parents here)
            if src.is_file():
                out = self._install_file_into_dataset(src, target_ds_path, move=move)
                # dataset-level metadata at the *target* dataset
                self._save_meta(
                    target_ds_path,
                    node_type="dataset",
                    name=f"{target_ds_path.name} ({out.name})",
                )
                return target_ds_path
            else:
                top_created = self._install_folder_as_datasets(src, target_ds_path, name=name, move=move)
                self._save_meta(top_created, node_type="dataset", name=(name or src.name))
                return top_created

        # No dest_rel: fall back to the category root (ensure tree exists/created)
        ep, cat_ds_path = self._ensure_experiment_category(project, campaign, experiment, category)
        if src.is_file():
            out = self._install_file_into_dataset(src, cat_ds_path, move=move)
            # metadata at category level for single-file install
            self._save_meta(cat_ds_path, node_type="category", name=f"{category} ({out.name})")
            return cat_ds_path
        else:
            top_created = self._install_folder_as_datasets(src, cat_ds_path, name=name, move=move)
            self._save_meta(top_created, node_type="dataset", name=(name or src.name))
            return top_created

