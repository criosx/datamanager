# datamanager.py
from __future__ import annotations

import os
import shutil

from pathlib import Path
from typing import Optional, Dict, Any

# DataLad Python API
from datalad import api as dl
from datalad.distribution.dataset import Dataset

# ROADMAP datamanager modules
from roadmap_datamanager import configuration as dmc
from roadmap_datamanager import metadata as md
from roadmap_datamanager import datalad_gin_api as dgapi
from roadmap_datamanager import datalad_utils

#  Install policy
ALLOWED_CATEGORIES = [
    "autocontrol", "raw", "reduced", "measurement", "analysis",
    "template", "experimental_optimization", "model",
]

#
GITIGNORE = """
autocontrol/
"""

class DataManager:
    """
    ROADMAP Data Manager class.
    """
    def __init__(
        self,
        root: os.PathLike | str | None = None,
        user_name: str | None = None,
        user_email: str | None = None,
        default_project: Optional[str] = None,
        default_campaign: Optional[str] = None,
        default_experiment: Optional[str] = None,
        env: Optional[Dict[str, str]] = None,
        GIN_url: Optional[str] = None,
        GIN_repo: Optional[str] = None,
        GIN_user: Optional[str] = None,
        datalad_profile: Optional[str] = "text2git",
        extractor_name: str = "datamanager_v1",
        extractor_version: str = "1.0",
        verbose: bool = True,
    ) -> None:
        """
        Initializes the DataManager class. Provided keyword arguments root too GIN_user will overwrite those from the
        persistent configuration. If the bootstrap path is given, the datamanager will be initialized from that
        directory, walking up the file tree, identifying root (username), project, campaign, and experiment as much
        as possible from the stored metadata. This information will overwrite any other keyword arguments given.

        :param root: (str | PathLike | None) The root of the repository to initialize.
        :param user_name: (str | None) the username .
        :param user_email:  (str | None) user email
        :param default_project: (str | None) default project (for GUI and such)
        :param default_campaign: (str | None) default campaign
        :param default_experiment: (str | None) default experiment
        :param datalad_profile: (str | None) default profile
        :param extractor_name: (str | None) The name of the metadata extractor.
        :param extractor_version: (str | None) The version of the metadata extractor.
        :param verbose: (bool) output verosity
        :param env: (dict) environment variables
        :param GIN_url: (str | None) GIN URL
        :param GIN_repo: (str | None) GIN repository name
        :param GIN_user: (str | None) GIN username
        :param bootstrap_path: (str | PathLike | None) bootstrap path
        """

        persisted = dmc.load_persistent_cfg()

        eff_root = root or persisted.dm_root or "."
        eff_user_name = user_name or persisted.user_name or "default"
        eff_user_email = user_email or persisted.user_email or ""
        eff_default_project = default_project or persisted.project
        eff_default_campaign = default_campaign or persisted.campaign
        eff_default_experiment = default_experiment or persisted.experiment
        eff_GIN_url = GIN_url or persisted.GIN_url
        eff_GIN_repo = GIN_repo or persisted.GIN_repo
        eff_GIN_user = GIN_user or persisted.GIN_user

        # build config
        root_path = Path(eff_root).expanduser().resolve()
        root_path.mkdir(parents=True, exist_ok=True)
        self.cfg = dmc.DataManagerConfig(
            dm_root=str(root_path),
            user_name=eff_user_name,
            user_email=eff_user_email,
            project=eff_default_project,
            campaign=eff_default_campaign,
            experiment=eff_default_experiment,
            extractor_name=extractor_name,
            extractor_version=extractor_version,
            verbose=verbose,
            env=env or {},
            GIN_url=eff_GIN_url,
            GIN_repo=eff_GIN_repo,
            GIN_user=eff_GIN_user
        )
        self.save_current_dm_configuration()

        if self.cfg.verbose:
            print(f"[DataManager] root={root_path}")
            print(f"[DataManager] user={self.cfg.user_name} <{self.cfg.user_email}>")
            if self.cfg.project or self.cfg.campaign:
                print(f"[DataManager] defaults: project={self.cfg.project} "
                      f"campaign={self.cfg.campaign}")

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
                        dataset_type: str = 'below-experiment',
                        register_installed: bool = False,
                        force: bool = False,
                        do_not_save:bool = False) -> None:
        """
        If dataset at `path` exists, (optionally) ensure it's registered in `superds`.
        Otherwise, create_dataset it (registered when superds is given).

        :param path: Path pointing to the dataset.
        :param name: Name of the dataset.
        :param superds: Path pointing to the parent dataset.
        :param register_installed: (bool) Whether to register the dataset with its parent if already installed.
        :param do_not_save: (bool) Whether not to save_dataset the dataset.
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
            dl.create(path=str(path), cfg_proc="text2git", force=force)
        else:
            # create_dataset and register as subdataset of superds in one API call
            # merge any remote changes into superdataset before committing local changes
            dl.update(dataset=superds, recursive=False, how='merge')
            dl.create(path=str(path), dataset=str(superds), cfg_proc="text2git", force=force)
            dl.save(dataset=superds, recursive=False)

        if dataset_type == 'experiment' and not (path / ".gitignore").is_file():
            (path / ".gitignore").write_text(GITIGNORE.strip() + "\n", encoding="utf-8")

        # dataset save_dataset here is not necessary, as it is saved in save_meta
        # dl.save_dataset(dataset=str(path), recursive=recursive_save, message=f"Initialized dataset.")
        self.save_meta(path, name=name, dataset_type=dataset_type, do_not_save=do_not_save)


    def clone_from_remote(self,
                          dest: str | os.PathLike,
                          source_url: str = None,
                          source_url_root: str = None,
                          user: str = None,
                          repo_name: str = None):
        """
        Clone a superdataset from GIN into dest, install subdatasets (no data),
        normalize all GIN remotes to the sibling name 'gin', and remove 'origin'.

        The destination may already exist, but it must be empty.

        :param dest: (str, os.Pathlike) destination path to clone the GIN dataset into
        :param source_url: (str, optional) source GIN repo URL. If not provided, source_url_root, user, and repo need to
                           be provided separately
        :param source_url_root: (str) URL root of the GIN dataset to clone, defaults to None
        :param user: (str) GIN unser name for the repository, defaults to None
        :param repo_name: (str) repo name of the repository, defaults to None
        :return: the path to the cloned GIN dataset
        """

        if source_url is None:
            if user is None:
                user = getattr(self.cfg, "GIN_user", None)
                if user is None:
                    raise RuntimeError(f"No username provided.")
            if repo_name is None:
                repo_name = getattr(self.cfg, "GIN_repo", None)
                if repo_name is None:
                    raise RuntimeError(f"No repository name provided.")

        dgapi.clone_from_remote(dest=dest, source_url=source_url, source_url_root=source_url_root, user_name=user, repo_name=repo_name)

        return

    def get_status(self, *,
                   dataset: str | os.PathLike = None,
                   recursive: bool = False):
        """
        Retrieves the DataLad status of a dataset.
        :param dataset: path to the dataset, defaults to None which will retrieve the status of the entire repository.
        :param recursive: whether to recursively step into subdatasets
        :return: (tuple): (bool) dir exists, (bool) dataset is installed, (dict) status
        """

        if dataset is None:
            dataset = self.cfg.dm_root
        dataset = Path(str(dataset)).expanduser().resolve()
        if not dataset.is_dir():
            return False, False, None
        ds = Dataset(str(dataset))
        if not ds.is_installed():
            return True, False, None
        status = ds.status(recursive=recursive)
        return True, True, status

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
        :param force: Force create_dataset new datasets even if directory is not empty. This option will trigger a delayed save_dataset
                      until the entire tree has been initialized. Otherwise, subdatasets will not be properly created.
        :return: (Path) to experiment dataset if argument provided, otherwise None
        """

        up = Path(self.cfg.dm_root)
        pp = up / project if project else None
        cp = pp / campaign if (pp and campaign) else None
        ep = cp / experiment if (cp and experiment) else None

        # Ensure/create_dataset datasets
        self._ensure_dataset(up, superds=None, name=self.cfg.user_name, dataset_type='root', force=force,
                             do_not_save=force)

        if pp:
            self._ensure_dataset(pp, superds=up, name=project, dataset_type='project', force=force, do_not_save=force)
        if cp:
            self._ensure_dataset(cp, superds=pp, name=campaign, dataset_type='campaign', force=force, do_not_save=force)
        if ep:
            self._ensure_dataset(ep, superds=cp, name=experiment, dataset_type='experiment', force=force,
                                 do_not_save=force)

        if force:
            dgapi.save_dataset(path=up, recursive=True)

        if self.cfg.verbose:
            print(f"Initialized/verified tree at {up} for "
                  f"{self.cfg.user_name}/" + "/".join(x for x in (project, campaign, experiment) if x))
        return ep

    def install_into_tree(self,
                          source: os.PathLike | str,
                          *,
                          project: Optional[str],
                          campaign: Optional[str],
                          experiment: str,
                          category: str,
                          dest_rel: Optional[os.PathLike | str] = None,
                          rename: Optional[str] = None,
                          move: bool = False,
                          metadata: Optional[Dict[str, Any]] = None,
                          overwrite: bool = False) -> Path:
        """
        Install a file or folder into {root}/{project}/{campaign}/{experiment}/{category}
        or into an *existing* dataset below the category when dest_rel is given.

        Rules:
          - Never install directly under the experiment root but into a predefined category.
          - For files: add to the chosen category / relative path in dataset and save_dataset.
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

        # after computing the destination folder dest_path, create_dataset it if not already exists
        dest_path.mkdir(parents=True, exist_ok=True)
        # decide the final target path for file/dir
        final_target = (dest_path / (rename or src.name))

        if final_target.exists():
            if not overwrite:
                raise FileExistsError(final_target)

        # copy file or folder to destination, register metadata, save_dataset dataset
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
        # self.save_dataset(dataset=str(ep), recursive=True, message=f"Installed {rename or src.name}")
        return final_target

    @staticmethod
    def load_meta(ds_path: str | Path, *, path: str | Path | None = None, mode: str = 'meta') -> Dict[str, Any]:
        """
        Return the metadata for `path` in `ds_path`.
        :param ds_path: (str, os.PathLike) path to the dataset to iterate over
        :param path: (str, os.PathLike) relative path to the dataset component to iterate over
        :param mode: (str) 'envelope' to obtain entire recore, 'meta' to obtain only the actual payload
        :return: metadata dict
        """
        ds_path, path, absolute_path, relposix = datalad_utils.ensure_paths(ds_path, path)
        meta = md.Metadata(ds_root=ds_path, path=path)
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

    def publish_gin_sibling(self, *,
                            sibling_name: str = "gin",
                            repo_name: str = None,
                            dataset=None,
                            access_protocol: str = "ssh",
                            credential: Optional[str] = None,
                            private: bool = False,
                            recursive: bool = False,
                            existing: str = 'skip',
                            push_annex_data = True) -> None:
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
        :param push_annex_data: (bool) whether to push annex data to the remote
        :return: no return value
        """

        # init reference dataset
        if dataset is None:
            dataset = str(self.cfg.dm_root)
        ds = Dataset(str(dataset))

        # compute repo name
        root_path, relpath, ds_path, relposix = datalad_utils.ensure_paths(ds_path=self.cfg.dm_root, path=dataset)
        if repo_name is None:
            repo_name = self.cfg.GIN_repo
        if str(relposix) != '.':
            repo_name = repo_name + '-' + '-'.join(relpath.parts)
            ds_parent_path = ds_path.parent
            ds_parent = Dataset(ds_parent_path)
        else:
            ds_parent = None
            ds_parent_path = None

        # Create/reconfigure GIN sibling with content hosting
        siblist = ds.create_sibling_gin(
            repo_name,
            name=sibling_name,
            recursive=recursive,
            existing=existing,
            access_protocol=access_protocol,
            credential=credential,
            private=private
        )

        # Minimal stabilization step: ensure each (sub)dataset has a defined upstream branch on the target remote.
        # This avoids the "no upstream branch" condition that can destabilize subsequent push/annex operations.
        published_ds_paths: set[Path] = set()
        for entry in siblist:
            if entry.get('action') != 'configure-sibling':
                continue
            try:
                _root_path, _relpath, entry_ds_path, _relposix = datalad_utils.ensure_paths(
                    ds_path=self.cfg.dm_root,
                    path=Path(entry['path'])
                )
            except Exception:
                # If ensure_paths fails for any reason, skip upstream setup for this entry.
                continue
            published_ds_paths.add(Path(entry_ds_path).resolve())

        # register GIN URLs in .gitmodules of the parents as the above command placed them only in the
        # .git/ record of the sibling itself
        for entry in siblist:
            # siblist contains entries for all actions performed during sibling creation. Since we only need the path,
            # take it from the 'configure-sibling' action
            if entry['action'] != 'configure-sibling':
                continue
            root_path, relpath, ds_path, relposix = datalad_utils.ensure_paths(ds_path=self.cfg.dm_root,
                                                                               path=Path(entry['path']))

            if relposix == '.':
                # exclude root for parent registration
                continue
            parent = ds_path.parent

            # Prefer HTTPS browser URL (no .git). If we only have SSH, convert it.
            url = entry.get('url') or ''
            if url.startswith('http'):
                https_url = url[:-4] if url.endswith('.git') else url
            else:
                https_url = dgapi.ssh_to_https(url)

            dl.subdatasets(
                dataset=str(parent),
                path=str(ds_path),
                set_property=[
                    ('url', https_url),
                    ('datalad-url', url)
                ]
            )

        # save_dataset and push to remotes
        dgapi.push_to_remotes(dataset=str(dataset), recursive=recursive, message='GIN publishing',
                             sibling_name=sibling_name, push_annex_data=push_annex_data)

        if ds_parent_path is not None:
            dgapi.push_to_remotes(dataset=str(ds_parent_path), recursive=False, message='GIN publishing',
                                 sibling_name=sibling_name, push_annex_data=False)

        if self.cfg.verbose:
            print(
                f"[Datamanager] Reset sibling '{sibling_name}' at GIN repo '{repo_name}' and pushed "
                f"(recursive={recursive})."
            )

    def save_current_dm_configuration(self):
        """
        Save the current data manager configuration to disk.
        """
        dmc.save_persistent_cfg({
            "dm_root": str(self.cfg.dm_root),
            "user_name": self.cfg.user_name,
            "user_email": self.cfg.user_email,
            "project": self.cfg.project,
            "campaign": self.cfg.campaign,
            "experiment": self.cfg.experiment,
            "GIN_url": self.cfg.GIN_url,
            "GIN_repo": self.cfg.GIN_repo,
            "GIN_user": self.cfg.GIN_user,
        })

    def save_meta(self,
                  ds_path: str | Path, *,
                  path: str | Path | None = None,
                  name: Optional[str] = None,
                  dataset_type: str = 'below-experiment',
                  extra: Optional[Dict[str, Any]] = None,
                  do_not_save = False) -> None:
        """
        Attach JSON-LD at dataset level to any file, folder, or the dataset itself using the MetaLad Python API.
        :param ds_path: (str, Path) path to the dataset
        :param path: (str, Path) Relative path to the file or folder whose meta-data should be attached serving as and
                     identifier
        :param name: (str) human-readable name for the file or folder whose meta-data will be saved.
        :param dataset_type: (str) Designates the type of dataset. Options: 'root', 'project', 'campaign', 'experiment',
                                   or 'below experiment'. Content 'below experiment' is not a dataset, but files and
                                   folders that belong to an experiment dataset
        :param extra: (Dict[str, Any]) optional extra metadata to be attached beyond default fields
        :param do_not_save: (bool) whether to save_dataset recursively or not
        :return: None
        """

        meta = md.Metadata(ds_root=ds_path, path=path)
        meta.add(
            payload=extra,
            mode='overwrite',
            name=name,
            dataset_type=dataset_type,
            user_email=self.cfg.user_email,
            user_name=self.cfg.user_name,
            extractor_name=self.cfg.extractor_name,
            extractor_version=self.cfg.extractor_version,
        )
        meta.save()
        targetstr = str(path) if path is not None else str(ds_path)

        # Commit metadata
        if not do_not_save:
            dgapi.save_dataset(
                path=str(ds_path),
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
