# datamanager.py
from __future__ import annotations

import os
import shutil
import subprocess

from dataclasses import dataclass, field
from datalad.support.exceptions import IncompleteResultsError
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Optional, Dict, Any, Callable

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
        extractor_name: str = "datamanager_v1",
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

    def _get_dataset_version(self, ds: Dataset) -> str:
        try:
            return ds.repo.get_hexsha()
        except IncompleteResultsError:
            # ensure at least one commit by touching .gitignore and saving
            (Path(ds.path) / ".gitignore").touch(exist_ok=True)
            dl.save(dataset=str(ds.path), path=[str(Path(ds.path) / ".gitignore")], message="Initial commit (auto)")
            return ds.repo.get_hexsha()

    def _proc_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(self.cfg.env)
        # Example: enforce non-interactive Git
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    def _ensure_dataset(self, path: Path, node_type, name, superds: Optional[Path]) -> None:
        """
        If dataset at `path` exists, (optionally) ensure it's registered in `superds`.
        Otherwise, create it (registered when superds is given).
        :param path: Path pointing to the dataset.
        :param node_type: Node type (user, project, category, experiment).
        :param name: Name of the dataset.
        :param superds: Path pointing to the parent dataset.
        :return: None
        """
        path = Path(path).resolve()
        ds = Dataset(str(path))

        if ds.is_installed():
            if superds and self.cfg.register_existing:
                # If already registered, this is a no-op (status=notneeded)
                dl.save(
                    dataset=str(superds),
                    path=[str(path)],
                    message=f"Register existing subdataset {path} with parent dataset {str(superds)})"
                )
            return

        # Create (and register if superds is provided)
        if superds is None:
            # top-level dataset
            dl.create(path=str(path), cfg_proc=self.cfg.datalad_profile)
        else:
            # create and register as subdataset of superds in one API call
            dl.create(path=str(path), dataset=str(superds), cfg_proc=self.cfg.datalad_profile)
        self._save_meta(path, name=name, node_type=node_type)

    def _save_meta(self, ds_path: Path, *, rel_path: Optional[Path] = Path(), name: Optional[str] = None,
                   extra: Optional[Dict[str, Any]] = None, node_type: Optional[str] = 'experiment') -> None:
        """
        Attach JSON-LD at dataset level using the  MetaLad Python API.
        :param ds_path: Path to the dataset
        :param rel_path: Relative path to the file or folder whose meta-data should be attached serving as and
                         identifier
        :param name: human-readable name for the file or folder whose meta-data will be saved.
        :return: None
        """
        ds = Dataset(str(ds_path))
        if not ds.is_installed():
            raise RuntimeError(f"Dataset not installed at {ds_path}")

        # Ensure rel_path is relative and normalized to POSIX for stable identifiers
        if rel_path is None:
            rel_path = Path()
        if rel_path.is_absolute():
            raise ValueError(f"rel_path must be relative to {ds_path}, got absolute: {rel_path}")

        # POSIX-normalized relative path string, '' for dataset itself
        relposix = '.' if rel_path == Path() else str(PurePosixPath(*rel_path.parts))

        # Working tree probe (only if materialized); use dataset root when rel_path is empty
        node_path = ds_path if relposix == '.' else (ds_path / rel_path)

        dataset_id = self._get_dataset_id(ds)
        dataset_version = self._get_dataset_version(ds)
        extraction_time = self.cfg.now_fn().replace(microsecond=0).isoformat()

        # Choose a Schema.org type
        if relposix == '.':
            type_str = "dataset"
        elif node_path.exists() and node_path.is_dir():
            type_str = "Collection"
        else:
            type_str = "CreativeWork"

        # Empty relpath identifies the dataset itself
        if relposix != '.':
            node_id = f"datalad:{node_type}{dataset_id}:{relposix}"
            toplevel_type = 'file'
        else:
            node_id = f"datalad:{node_type}{dataset_id}"
            toplevel_type = 'dataset'

        # Interpret this JSON object as a Schema.org entity, so that name, description, etc., have their
        # standardized meanings.
        extracted: Dict[str, Any] = {
            "@context": {
                "@vocab": "https://schema.org/",
                "dm": "https://your-vocab.example/terms/"
            },
            "@type": type_str,
            "@id": node_id,
            "identifier": relposix,  # machine ID (relative path)
        }
        # Only include a human-facing name if you have one
        if name:
            extracted["name"] = name

        if extra:
            extracted.update(extra)

        # The extractor is the current script, as metadata is manually provided when installing a file or folder
        # This is the toplevel metadata record envelope, which contains the extracted metadata as a nested subfield
        # All according to the JSON-LD schema
        payload = {
            "type": toplevel_type,
            "extractor_name": self.cfg.extractor_name,
            "extractor_version": self.cfg.extractor_version,
            "extraction_parameter": {
                "path": relposix,
                "node_type": node_type
            },
            "extraction_time": extraction_time,
            "agent_name": self.cfg.user_name,
            "agent_email": self.cfg.user_email,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "path": relposix,
            "extracted_metadata": extracted
        }

        try:
            print(payload)
            _ = dl.meta_add(metadata=payload, dataset=str(ds_path), allow_id_mismatch=False, json_lines=False,
                            batch_mode=False, on_failure='stop', return_type='list')
        except IncompleteResultsError as e:
            where = f"{ds_path / rel_path}" if rel_path is not None else str(ds_path)
            raise RuntimeError(f"meta-add failed for {where}: {e}") from e

        # Commit metadata; scope to metadata dir to keep the commit tight
        meta_dir = Path(ds_path) / ".datalad" / "metadata"
        dl.save(
            dataset=str(ds_path),
            path=str(meta_dir) if meta_dir.exists() else None,
            message=f"scidata: metadata for {ds_path / rel_path if rel_path != Path() else ds_path}",
        )

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

    def init_tree(self, *, project: Optional[str] = None, campaign: Optional[str] = None,
                  experiment: Optional[str] = None) -> Path:
        """
        Ensure the (user)/(project)/(campaign)/(experiment) dataset tree exists and is registered.
        Attach minimal JSON-LD at each level. Idempotent.
        :param project:
        :param campaign:
        :param experiment:
        :return: (Path) to experiment dataset if argument provided, otherwise None
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
                  f"{self.cfg.user_name}/" + "/".join(x for x in (project, campaign, experiment) if x))

        return ep

    def install_into_tree(self, source: os.PathLike | str, *, project: Optional[str], campaign: Optional[str],
                          experiment: str, category: str, dest_rel: Optional[os.PathLike | str] = None,
                          rename: Optional[str] = None, move: bool = False, metadata: Optional[Dict[str, Any]]
                          = None) -> Path:
        """
        Install a file or folder into {root}/{project}/{campaign}/{experiment}/{category}
        or into an *existing* dataset below the category when dest_rel is given.

        Rules:
          - Never install directly under the experiment root but into a predefined category.
          - For files: add to the chosen category / relative path in dataset and save.
          - Attach dataset-level metadata (includes file/folder name in .name field).

        :param source: (str or path) source director of the file or folder to install.
        :param project: (str) project identifier for target destination
        :param campaign: (str) campaign identifier for target destination
        :param experiment: (str) experiment identifier for target destination
        :param category: (str) category for target destination
        :param dest_rel: (str or path) relative path to destination folder from category
        :param rename: (str) name under which the file or folder will be installed.
        :param move: (bool) move or copy file or folder
        :param metadata: (json) additional metadata to add to file or folder (dataset).
        :return: path to destination of file or dataset
        """

        src = Path(source).expanduser().resolve()
        if category not in ALLOWED_CATEGORIES:
            raise ValueError(f"category must be one of {ALLOWED_CATEGORIES}, got {category!r}")
        if not src.exists():
            raise FileNotFoundError(src)

        # make sure the nested dataset structure exists for project/campaign/experiment
        ep = self.init_tree(project=project, campaign=campaign, experiment=experiment)

        # make sure that the category subfolder exists
        cat_path = ep / category
        if not cat_path.exists():
            cat_path.mkdir(parents=True)

        # make sure any relative path from category exists, if given
        if dest_rel:
            dest_path = cat_path / dest_rel
            if not dest_path.exists():
                dest_path.mkdir(parents=True)
        else:
            dest_path = cat_path

        # add optional renaming to path and check if it exists
        if rename:
            dest_path = dest_path / rename
            if dest_path.exists():
                raise FileExistsError(dest_path)

        # copy file or folder to destination, register metadata, save dataset
        if src.is_file():
            if move:
                shutil.move(str(src), str(dest_path))
            else:
                shutil.copy2(str(src), str(dest_path))
        elif src.is_dir():
            if move:
                shutil.move(str(src), str(dest_path))
            else:
                shutil.copytree(str(src), str(dest_path))

        if not rename:
            # with rename the file or foldername is already part of the destination path
            dest_path = dest_path / src.name

        self._save_meta(ep, rel_path=dest_path.relative_to(ep), extra=metadata)
        dl.save(dataset=str(ep), recursive=True, message=f"Installed {rename or src.name} in {self.cfg.user_name}/"
                                                         f"{project or ''}/{campaign or ''}/{experiment or ''}")

        return dest_path

    def publish_gin_sibling(self, *, sibling_name: str = "gin", repo_name: str = "datamanager", dataset=None,
                            access_protocol: str = "https-ssh", credential: Optional[str] = None, private: bool = False,
                            recursive: bool = False) -> None:
        """
        Pushes a gin sibling dataset to {repo_name}.

        :param sibling_name: sibling name to publish
        :param repo_name: name of the GIN repository
        :param dataset: (str or Path) path to dataset to be published, default: root
        :param access_protocol: (str) access protocol for GIN, default "https-ssh"
        :param credential: (str) credential to be used for GIN, default None
        :param private: (bool) privacy of the published dataset, default False
        :param recursive: (bool) whether to step recursivly into nested subdatasets, default False
        :return: no return value
        """

        if dataset is None:
            dataset = str(self.root)
        ds = Dataset(str(dataset))

        # make sure all changes are saved before publishing to GIN
        if recursive:
            ds.save(recursive=True, message='Recursive save for GIN publishing')

        # Create/reconfigure GIN sibling with content hosting
        ds.create_sibling_gin(
            repo_name,
            name=sibling_name,
            recursive=recursive,
            existing="skip",
            access_protocol=access_protocol,
            credential=credential,
            private=private
        )

        ds.push(to=sibling_name, recursive=recursive, data='auto')

        if self.cfg.verbose:
            print(
                f"[Datamanager] Reset sibling '{sibling_name}' at GIN repo '{repo_name}' and pushed "
                f"(recursive={recursive})."
            )
