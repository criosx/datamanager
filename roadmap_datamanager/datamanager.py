# datamanager.py
from __future__ import annotations

import os
import shutil

from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

# DataLad Python API
from datalad import api as dl
from datalad.distribution.dataset import Dataset

# ROADMAP datamanager modules
from roadmap_datamanager import configuration as dmc
from roadmap_datamanager.helpers import ssh_to_https, ensure_paths, find_dataset_root_and_rel
from roadmap_datamanager import metadata as md


#  Install policy
ALLOWED_CATEGORIES = [
    "raw", "reduced", "measurement", "analysis",
    "template", "experimental_optimization", "model",
]


class DataManager:
    """
    ROADMAP Data Manager class.
    """
    def __init__(
        self,
        root: os.PathLike | str | None = None,
        user_name: str | None = None,
        user_email: str | None = None,
        *,
        default_project: Optional[str] = None,
        default_campaign: Optional[str] = None,
        datalad_profile: Optional[str] = "text2git",
        extractor_name: str = "datamanager_v1",
        extractor_version: str = "1.0",
        verbose: bool = True,
        env: Optional[Dict[str, str]] = None,
        GIN_url: Optional[str] = None,
        GIN_repo: Optional[str] = None,
        GIN_user: Optional[str] = None
    ) -> None:

        # load persistent configuration
        persisted = dmc.load_persistent_cfg()

        # compute effective values (= persisted ⟵ kwargs)
        eff_root = root or persisted.get("dm_root", ".")
        eff_user_name = user_name or persisted.get("user_name")
        eff_user_email = user_email or persisted.get("user_email")
        eff_default_project = default_project or persisted.get("default_project")
        eff_default_campaign = default_campaign or persisted.get("default_campaign")
        eff_GIN_url = GIN_url or persisted.get("GIN_url")
        eff_GIN_repo = GIN_repo or persisted.get("GIN_repo")
        eff_GIN_user = GIN_user or persisted.get("GIN_user")

        if eff_user_name is None or eff_user_email is None:
            raise RuntimeError("DataManager requires user_name and user_email (none persisted yet).")

        # build config
        root_path = Path(eff_root).expanduser().resolve()
        root_path.mkdir(parents=True, exist_ok=True)
        self.cfg = dmc.DataManagerConfig(
            dm_root=str(root_path),
            user_name=eff_user_name,
            user_email=eff_user_email,
            default_project=eff_default_project,
            default_campaign=eff_default_campaign,
            datalad_profile=datalad_profile,
            extractor_name=extractor_name,
            extractor_version=extractor_version,
            verbose=verbose,
            env=env or {},
            GIN_url=eff_GIN_url,
            GIN_repo=eff_GIN_repo,
            GIN_user=eff_GIN_user
        )

        dmc.save_persistent_cfg({
            "dm_root": str(root_path),
            "user_name": self.cfg.user_name,
            "user_email": self.cfg.user_email,
            "default_project": self.cfg.default_project,
            "default_campaign": self.cfg.default_campaign,
            "GIN_url": self.cfg.GIN_url,
            "GIN_repo": self.cfg.GIN_repo,
            "GIN_user": self.cfg.GIN_user
        })

        if self.cfg.verbose:
            print(f"[DataManager] root={root_path}")
            print(f"[DataManager] user={self.cfg.user_name} <{self.cfg.user_email}>")
            if self.cfg.default_project or self.cfg.default_campaign:
                print(f"[DataManager] defaults: project={self.cfg.default_project} "
                      f"campaign={self.cfg.default_campaign}")
            print(f"[DataManager] profile={self.cfg.datalad_profile or '(none)'} ")

    @classmethod
    def from_persisted(cls):
        """
        Return a DataManager initialized from the last saved configuration.
        """
        persisted = dmc.load_persistent_cfg()
        if not persisted:
            raise FileNotFoundError("No persistent configuration found — initialize once first.")

        return cls(
            root=persisted.get("dm_root"),
            user_name=persisted.get("user_name"),
            user_email=persisted.get("user_email"),
            default_project=persisted.get("default_project"),
            default_campaign=persisted.get("default_campaign"),
            GIN_url=persisted.get("GIN_url"),
            GIN_repo=persisted.get("GIN_repo"),
            GIN_user=persisted.get("GIN_user")
        )

    def _ensure_dataset(self,
                        path: Path,
                        name,
                        superds: Optional[Path],
                        register_installed: bool = False,
                        force: bool = False,
                        do_not_save:bool = False) -> None:
        """
        If dataset at `path` exists, (optionally) ensure it's registered in `superds`.
        Otherwise, create it (registered when superds is given).
        :param path: Path pointing to the dataset.
        :param name: Name of the dataset.
        :param superds: Path pointing to the parent dataset.
        :param register_installed: (bool) Whether to register the dataset with its parent if already installed.
        :param do_not_save: (bool) Whether not to save the dataset.
        :return: None
        """
        path = Path(path).resolve()
        ds = Dataset(str(path))

        if ds.is_installed():
            if superds is not None and register_installed:
                # If already registered, this is a no-op (status=notneeded)
                dl.save(
                    dataset=str(superds),
                    path=[str(path)],
                    message=f"Register existing subdataset {path} with parent dataset {str(superds)}."
                )
            return

        # Create (and register if superds is provided)
        if superds is None:
            # top-level dataset
            # merge any remote changes into superdataset before committing local changes
            dl.create(path=str(path), cfg_proc=self.cfg.datalad_profile, force=force)
        else:
            # create and register as subdataset of superds in one API call
            # merge any remote changes into superdataset before committing local changes
            dl.update(dataset=superds, recursive=False, how='merge')
            dl.create(path=str(path), dataset=str(superds), cfg_proc=self.cfg.datalad_profile, force=force)
            dl.save(dataset=superds, recursive=False)

        # dataset save here is not necessary, as it is saved in save_meta
        # dl.save(dataset=str(path), recursive=recursive_save, message=f"Initialized dataset.")
        self.save_meta(path, name=name, do_not_save=do_not_save)

    @staticmethod
    def _parse_iso(ts: str) -> datetime:
        """
        Lenient ISO parser: accept 'YYYY-MM-DDTHH:MM:SS' (no micros, no tz)
        and '...Z' variants, but don't break if format is slightly different
        :param ts: (str) time
        :return: reformatted ISO datetime
        """

        try:
            # Common case in your save_meta: .isoformat() without microseconds
            return datetime.fromisoformat(ts.replace('Z', '+00:00'))
        except ValueError:
            # Fallback: be robust but keep ordering stable if unparsable
            return datetime.min

    def _proc_env(self) -> Dict[str, str]:
        env = os.environ.copy()
        env.update(self.cfg.env)
        # Example: enforce non-interactive Git
        env.setdefault("GIT_TERMINAL_PROMPT", "0")
        return env

    def clone_from_gin(self, dest: str | os.PathLike, source_url_root: str = None, user: str = None,
                       repo: str = None) -> Path:
        """
        Clone a superdataset from GIN into dest; install subdatasets (no data).
        :param dest: (str, os.Pathlike) destination path to clone the GIN dataset into
        :param source_url_root: (str) URL root of the GIN dataset to clone, defaults to None
        :param user: (str) GIN unser name for the repository, defaults to None
        :param repo: (str) repo name of the repository, defaults to None
        :return: the path to the cloned GIN dataset
        """
        dest = Path(dest).expanduser().resolve()
        dest.mkdir(parents=True, exist_ok=True)
        if any(dest.iterdir()):
            raise RuntimeError(f"Destination path {dest} must be empty.")

        if source_url_root is None:
            # testcomment
            source_url_root = f"git@gin.g-node.org:/"

        if user is None:
            user = getattr(self.cfg, "GIN_user", None)
            if user is None:
                raise RuntimeError(f"No username provided.")

        if repo is None:
            repo = getattr(self.cfg, "GIN_repo", None)
            if repo is None:
                raise RuntimeError(f"No repository name provided.")

        source_url = source_url_root + user + '/' + repo + '.git'

        dl.clone(source=source_url, path=str(dest))
        self.pull_from_remotes(dataset=str(dest), recursive=True)             # installs subdatasets
        return dest

    @staticmethod
    def drop_local(dataset: str | os.PathLike, path: str | os.PathLike = None, recursive: bool = False) -> None:
        """
        Drop local annexed content after confirming availability elsewhere.
        :param dataset: (str, os.Pathlike) path to the dataset for which to drop local content
        :param path: (str, os.Pathlike) relative path to the dataset component for which to drop local content,
                      defaults to None which will drop all components of the dataset
        :param recursive: whether to recursively step into subdatasets
        :return: no return value
        """
        if path is not None:
            path = str(path)
        content_path = Path(dataset) / Path(path)
        dl.drop(dataset=str(dataset), path=content_path, recursive=recursive, what='filecontent')

    @staticmethod
    def get_data(dataset: str | os.PathLike, path: str | os.PathLike | list[str | os.PathLike] | None = None,
                 recursive: bool = False) -> None:
        """
        Retrieve annexed file content (bytes).
        :param dataset: (str, os.Pathlike) path to the dataset to update from GIN
        :param path: (str, os.Pathlike) path to the dataset component to retrieve content for, defaults to
                     None which will obtain all components of the dataset
        :param recursive: whether to recursively step into subdatasets
        :return: no return value
        """
        if path is None:
            targets = None
        elif isinstance(path, (str, os.PathLike, Path)):
            targets = [path]
        else:
            targets = list(path)

        if targets is None:
            dl.get(dataset=str(dataset), recursive=recursive)
        else:
            for p in targets:
                dl.get(dataset=str(dataset), path=str(p) if path else None, recursive=recursive)

    def get_status(self,
                   dataset: str | os.PathLike = None,
                   recursive: bool = False):
        """
        Retrieves the DataLad status of a dataset.
        :param dataset: path to the dataset, defaults to None which will retrieve the status of the entire repository.
        :param recursive: whether to recursively step into subdatasets
        :return: (dict) status
        """

        if dataset is None:
            dataset = self.cfg.dm_root
        dataset = str(dataset)

        status = dl.status(dataset=dataset, recursive=recursive)

        return status


    def init_tree(self, *,
                  project: Optional[str] = None,
                  campaign: Optional[str] = None,
                  experiment: Optional[str] = None,
                  force = False) -> Path:
        """
        Ensure the (user)/(project)/(campaign)/(experiment) dataset tree exists and is registered.
        Attach minimal JSON-LD at each level. Idempotent.
        :param project: project name
        :param campaign: campaign name
        :param experiment: experiment name
        :param force: Force create new datasets even if directory is not empty. This option will trigger a delayed save
                      until the entire tree has been initialized. Otherwise, subdatasets will not be properly created.
        :return: (Path) to experiment dataset if argument provided, otherwise None
        """

        up = Path(self.cfg.dm_root)
        pp = up / project if project else None
        cp = pp / campaign if (pp and campaign) else None
        ep = cp / experiment if (cp and experiment) else None

        # Ensure/create datasets
        self._ensure_dataset(up, superds=None, name=self.cfg.user_name, force=force, do_not_save=force)

        if pp:
            self._ensure_dataset(pp, superds=up, name=project, force=force, do_not_save=force)
        if cp:
            self._ensure_dataset(cp, superds=pp, name=campaign, force=force, do_not_save=force)
        if ep:
            self._ensure_dataset(ep, superds=cp, name=experiment, force=force, do_not_save=force)

        if force:
            dl.save(dataset=up, recursive=True)

        if self.cfg.verbose:
            print(f"Initialized/verified tree at {up} for "
                  f"{self.cfg.user_name}/" + "/".join(x for x in (project, campaign, experiment) if x))
        return ep

    def install_into_tree(self, source: os.PathLike | str, *, project: Optional[str], campaign: Optional[str],
                          experiment: str, category: str, dest_rel: Optional[os.PathLike | str] = None,
                          rename: Optional[str] = None, move: bool = False, metadata: Optional[Dict[str, Any]]
                          = None, overwrite: bool = False) -> Path:
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
        :param overwrite: (bool) whether to overwrite existing target or not
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
        else:
            dest_path = cat_path

        # after computing the destination folder dest_path, create it if not already exists
        dest_path.mkdir(parents=True, exist_ok=True)
        # decide the final target path for file/dir
        final_target = (dest_path / (rename or src.name))

        if final_target.exists():
            if not overwrite:
                raise FileExistsError(final_target)

        # copy file or folder to destination, register metadata, save dataset
        if src.is_file():
            # for files, move and copy2 will replace existing files of the same name by default
            if move:
                shutil.move(str(src), str(final_target))
            else:
                shutil.copy2(str(src), str(final_target))
        elif src.is_dir():
            shutil.copytree(str(src), str(final_target), dirs_exist_ok=overwrite)
            if move:
                # the move command does not have a dirs_exist_ok option and would potentially
                # place a source dir into an existing dest dir of the same name instead of
                # replacing
                src.rmdir()
        else:
            raise FileNotFoundError(src)

        self.save_meta(ep, path=final_target.relative_to(ep), extra=metadata)
        # not necessary, as save_meta alreaddy saved the experiment, recursive=True below should also
        # be redundant
        # dl.save(dataset=str(ep), recursive=True, message=f"Installed {rename or src.name}")
        return final_target

    def load_meta(self, ds_path: str | Path, *, path: str | Path | None = None, mode: str = 'meta') -> Dict[str, Any]:
        """
        Return the metadata for `path` in `ds_path`.
        :param ds_path: (str, os.PathLike) path to the dataset to iterate over
        :param path: (str, os.PathLike) relative path to the dataset component to iterate over
        :param mode: (str) 'envelope' to obtain entire recore, 'meta' to obtain only the actual payload
        :return: metadata dict
        """
        ds_path, path, absolute_path, relposix = ensure_paths(ds_path, path)
        meta = md.Metadata(ds_root=ds_path, path=path, dm_root=self.cfg.dm_root)
        record = meta.get(mode=mode)
        return record

    def publish_lazy_to_remote(self, *, sibling_name: str = "gin", repo_name: str = "datamanager", dataset=None,
                               access_protocol: str = "ssh", credential: Optional[str] = None,
                               private: bool = False, message: str | None = None, existing: str = 'skip') -> None:
        """
        Publish the minimal ancestor needed to expose `dataset` on the remote, then push.
        Strategy: climb to the nearest ancestor that already has `sibling_name`,
        else fall back to `self.cfg.dm_root`. From there, run a single recursive publish/push.
        """
        # normalize inputs
        start_path = Path(dataset or self.cfg.dm_root).expanduser().resolve()
        root_path = Path(self.cfg.dm_root).resolve()

        if not Dataset(str(start_path)).is_installed():
            raise RuntimeError(f"Not a DataLad dataset: {start_path}")

        # climb until you find an ancestor with the target sibling, or root
        ds_path = start_path
        while True:
            ds = Dataset(str(ds_path))
            if not ds.is_installed():
                raise RuntimeError(f"Ancestor not installed as dataset: {ds_path}")

            # Has the target sibling already? Note: cloned trees often have a sibling name 'origin' independent of
            # the initial designation.
            sibs = ds.siblings(action="query", return_type="list")
            has_target_sibling_name = any(s.get("name") == sibling_name for s in sibs)
            has_target_origin = any(s.get("name") == 'origin' for s in sibs)

            if has_target_sibling_name:
                sibling_name_arg = sibling_name
                chosen = ds_path
                break
            elif has_target_origin:
                sibling_name_arg = 'origin'
                chosen = ds_path
                break

            if ds_path == root_path:
                sibling_name_arg = sibling_name
                chosen = ds_path
                break

            # guard: if we’re no longer moving up, bail (dataset not under self.cfg.dm_root)
            parent = ds_path.parent.resolve()
            if parent == ds_path:
                raise RuntimeError(
                    f"{start_path} is not within managed root {root_path}; refusing to climb past filesystem root."
                )
            ds_path = parent

        # One recursive publish from the chosen ancestor creates/configures siblings and fixes .gitmodules URLs.
        self.publish_gin_sibling(
            sibling_name=sibling_name_arg,
            repo_name=repo_name,
            dataset=str(chosen),
            access_protocol=access_protocol,
            credential=credential,
            private=private,
            recursive=True,
            existing=existing
        )

        # Save narrowly (only where needed) before the push, but OK to be simple here
        Dataset(str(chosen)).save(recursive=True, message=message or "Publish subtree")

        # This should be unnecessary, as pushing is done by publish_gin_sibling()
        """
        # Push once, recursively, to the chosen remote name
        self.push_to_remotes(
            dataset=str(chosen),
            recursive=True,
            message=message,
            sibling_name=sibling_name_arg
        )
        """

    def publish_gin_sibling(self, *, sibling_name: str = "gin", repo_name: str = None, dataset=None,
                            access_protocol: str = "ssh", credential: Optional[str] = None, private: bool = False,
                            recursive: bool = False, existing: str = 'skip') -> None:
        """
        Creates and pushes a gin sibling dataset to {repo_name}.
        Important Note: The operation must be started from root or a dataset that has already a gin sibling. This is
        to ensure consistent registration of gin siblings throughout the entire tree. If that becomes undesirable,
        we have to implement explicit registration of the reference data set to its parent, if it exists (and
        possibly register the parent to its parent, and so on).

        :param sibling_name: sibling name to publish
        :param repo_name: name of the GIN repository
        :param dataset: (str or Path) path to dataset to be published, default: root
        :param access_protocol: (str) access protocol for GIN, default "https-ssh"
        :param credential: (str) credential to be used for GIN, default None
        :param private: (bool) privacy of the published dataset, default False
        :param recursive: (bool) whether to step recursivly into nested subdatasets, default False
        :param existing: (str) how to deal with existing siblings (see datalad manual), default 'skip'
        :return: no return value
        """

        # init reference dataset
        if dataset is None:
            dataset = str(self.cfg.dm_root)
        ds = Dataset(str(dataset))

        # compute repo name
        root_path, relpath, ds_path, relposix = ensure_paths(ds_path=self.cfg.dm_root, path=dataset)
        if repo_name is None:
            repo_name = self.cfg.GIN_repo
        if str(relposix) != '.':
            repo_name = repo_name + '-' + '-'.join(relpath.parts)
            ds_parent = Dataset(ds_path.parent)
        else:
            ds_parent = None

        # Create/reconfigure GIN sibling with content hosting
        ds.create_sibling_gin(
            repo_name,
            name=sibling_name,
            recursive=recursive,
            existing=existing,
            access_protocol=access_protocol,
            credential=credential,
            private=private
        )

        siblist = ds.siblings(
            'query',
            name=sibling_name,
            recursive=recursive
        )

        # register GIN URLs in .gitmodules of the parents as the above command placed them only in the
        # .git/ record of the sibling itself
        for entry in siblist:
            root_path, relpath, ds_path, relposix = ensure_paths(ds_path=self.cfg.dm_root, path=Path(entry['path']))
            if relposix == '.':
                # exclude root
                continue
            parent = ds_path.parent

            # Prefer HTTPS browser URL (no .git). If we only have SSH, convert it.
            url = entry.get('url') or ''
            if url.startswith('http'):
                https_url = url[:-4] if url.endswith('.git') else url
            else:
                https_url = ssh_to_https(url)

            dl.subdatasets(
                dataset=str(parent),
                path=str(ds_path),
                set_property=[
                    ('url', https_url),
                    ('datalad-url', url)
                ]
            )

        # make sure all changes are saved before publishing to GIN
        ds.save(recursive=recursive, message='GIN publishing')
        ds.push(to=sibling_name, recursive=recursive, data='anything')

        if ds_parent is not None:
            ds_parent.save(recursive=False, message='GIN publishing')
            ds_parent.push(to=sibling_name, recursive=False, data='nothing')

        if self.cfg.verbose:
            print(
                f"[Datamanager] Reset sibling '{sibling_name}' at GIN repo '{repo_name}' and pushed "
                f"(recursive={recursive})."
            )

    @staticmethod
    def pull_from_remotes(dataset: str | os.PathLike, recursive: bool = True, sibling_name: str = None) -> None:
        """
        Pull latest history from GIN and merge.
        :param dataset: (str) path to the dataset to update from remotes
        :param recursive: whether recursively pull from remotes, default True
        :param sibling_name: (str) name of the sibling datasets to pull from (such as 'gin' for GIN publishing),
                             default: None (recommended), which self-determines the target to pull from
        :return: no return value
        """
        ds = Dataset(str(dataset))
        # ds.save(recursive=recursive, message='save before update from remote')
        ds.update(recursive=recursive, how='merge', sibling=sibling_name)
        # ds.get(recursive=recursive, get_data=False)
        ds.save(recursive=recursive, message='updated from remote')

    @staticmethod
    def push_to_remotes(dataset: str | os.PathLike, recursive: bool = True, message: str | None = None, sibling_name:
                        str = None) -> None:
        """
        Save and push commits + annexed content to GIN.
        :param dataset: (str, os.Pathlike) path to the dataset to push to GIN
        :param recursive: (bool) whether to recursively push subdatasets
        :param sibling_name: (str) name of the sibling datasets to push to GIN
        :param message: (str) optional commit message to push to GIN
        :return: no return value
        """
        ds = Dataset(str(dataset))
        if message:
            ds.save(recursive=recursive, message=message)
        else:
            ds.save(recursive=recursive)

        try:
            ds.push(to=sibling_name, recursive=recursive, data='anything')
        except Exception as e:
            # Fallback: pick a sane sibling name
            sibs = ds.siblings(action="query", return_type="list")
            names = {s["name"] for s in sibs if s.get("name")}
            fallback = "gin" if "gin" in names else ("origin" if "origin" in names else None)
            if not fallback:
                raise RuntimeError("No publication target configured and no 'gin'/'origin' sibling found.") from e
            ds.push(to=fallback, recursive=recursive, data="anything")

    @staticmethod
    def remove_siblings(path: str | os.PathLike, name: str = 'gin', recursive: bool = False) -> None:
        """
        Removes all sibling datasets from tree.
        :param path: (str or Path) root path to remove sibling datasets from
        :param name: (str) sibling name to match
        :param recursive: (bool) whether to recursively step into subdatasets
        :return: no return value
        """
        path = str(Path(path).resolve())
        dl.siblings(action='remove', dataset=path, name=name, recursive=recursive)

    def save(self, path: str | os.PathLike, recursive: bool = True, message: str = None) -> None:
        """
        Saves the current dataset to disk
        :param path: (str or Path) path to the dataset or content in dataset
        :param recursive: (bool) step recursively into subdatasets
        :param message: (str) optional commit message
        :return: no return value
        """
        path = Path(path).resolve()
        ds_root, rel = find_dataset_root_and_rel(path, dm_root=self.cfg.dm_root)

        if str(rel) == '.':
            # save dataset
            dl.save(dataset=str(ds_root), recursive=recursive, message=message)
        else:
            # just save content, if path is not related to a subdataset
            dl.save(path=str(path), recursive=False, message=message)

    def save_current_dm_configuration(self):
        """
        Save the current data manager configuration to disk.
        """
        dmc.save_persistent_cfg({
            "dm_root": self.cfg.dm_root,
            "user_name": self.cfg.user_name,
            "user_email": self.cfg.user_email,
            "default_project": self.cfg.default_project,
            "default_campaign": self.cfg.default_campaign,
            "GIN_url": self.cfg.GIN_url,
            "GIN_repo": self.cfg.GIN_repo,
            "GIN_user": self.cfg.GIN_user,
        })

    def save_meta(self,
                  ds_path: str | Path, *,
                  path: str | Path | None = None,
                  name: Optional[str] = None,
                  extra: Optional[Dict[str, Any]] = None,
                  do_not_save = False) -> None:
        """
        Attach JSON-LD at dataset level to any file, folder, or the dataset itself using the MetaLad Python API.
        :param ds_path: (str, Path) path to the dataset
        :param path: (str, Path) Relative path to the file or folder whose meta-data should be attached serving as and
                     identifier
        :param name: (str) human-readable name for the file or folder whose meta-data will be saved.
        :param extra: (Dict[str, Any]) optional extra metadata to be attached beyond default fields
        :param do_not_save: (bool) whether to save recursively or not
        :return: None
        """

        meta = md.Metadata(ds_root=ds_path, path=path, dm_root=self.cfg.dm_root)
        meta.add(
            payload=extra,
            mode='overwrite',
            name=name,
            user_email=self.cfg.user_email,
            user_name=self.cfg.user_name,
            extractor_name=self.cfg.extractor_name,
            extractor_version=self.cfg.extractor_version,
        )
        meta.save()
        targetstr = str(path) if path is not None else str(ds_path)

        # Commit metadata
        if not do_not_save:
            dl.save(
                dataset=str(ds_path),
                message=f"Metadata for {targetstr}",
                recursive=False
            )
        if self.cfg.verbose:
            print(f"Added metadata to dataset {targetstr}")
            print(f"Payload:")
            print(extra)

    @staticmethod
    def remove_from_tree(dataset: str | os.PathLike, path: str | os.PathLike = None, recursive: bool = False,
                         reckless: str = None) -> None:
        """
        Remove content from filesystem after confirming availability elsewhere.
        :param dataset: (str, os.Pathlike) path to the dataset for which to drop local content
        :param path: (str, os.Pathlike) relative path to the dataset component for which to drop local content,
                      defaults to None which will drop all components of the dataset
        :param recursive: whether to recursively step into subdatasets
        :param reckless: disable safety measures for removing local content (see datalad api for remove), default None
        :return: no return value
        """
        if path is not None:
            path = str(path)
        content_path = Path(dataset) / Path(path)
        dl.remove(dataset=str(dataset), path=content_path, recursive=recursive, reckless=reckless)
