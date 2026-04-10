from __future__ import annotations

from datalad import api as dl
from datalad.api import Dataset

from pathlib import Path
import shutil
import subprocess
import shlex
from typing import Dict, Any
import os

from roadmap_datamanager import metadata as md


def _run_git(args: list[str], *, cwd: Path) -> subprocess.CompletedProcess:
    """
    Run a git command in `cwd` without changing the process working directory.
    """
    env = os.environ.copy()
    # We previously had an option to modify the environment via the config -> reintroduce if ever needed
    # env.update(self.cfg.env)
    env.setdefault("GIT_TERMINAL_PROMPT", "0")

    return subprocess.run(
        ["git", *args],
        cwd=str(cwd),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


def clone_from_remote(dest: str | os.PathLike,
                      source_url: str = None,
                      source_url_root: str = None,
                      user_name: str = None,
                      repo_name: str = None):
    """
    Clone a superdataset from GIN into dest, install subdatasets (no data),
    normalize all GIN remotes to the sibling name 'gin', and remove 'origin'.

    The destination may already exist, but it must be empty.

    :param dest: (str, os.Pathlike) destination path to clone the GIN dataset into
    :param source_url: (str, optional) source GIN repo URL. If not provided, source_url_root, user, and repo need to
                       be provided separately
    :param source_url_root: (str) URL root of the GIN dataset to clone, defaults to None
    :param user_name: (str) GIN unser name for the repository, defaults to None
    :param repo_name: (str) repo name of the repository, defaults to None
    :return: no return value
    """

    dest = Path(dest).expanduser().resolve()
    dest.mkdir(parents=True, exist_ok=True)
    if any(dest.iterdir()):
        raise RuntimeError(f"Destination path {dest} must be empty.")

    if source_url is None:
        if source_url_root is None:
            source_url_root = f"git@gin.g-node.org:"

        if user_name is None:
                raise RuntimeError(f"No username provided.")

        if repo_name is None:
                raise RuntimeError(f"No repository name provided.")

        source_url = f"{source_url_root}/{user_name}/{repo_name}.git"

    dl.clone(source=source_url, path=str(dest))

    # installs subdatasets
    pull_from_remotes(dataset=str(dest), recursive=True)

    # Normalize sibling naming: create_dataset/reconfigure 'gin', then remove 'origin'.
    ds = Dataset(str(dest))
    _ = ds.create_sibling_gin(
        reponame=repo_name,
        name='gin',
        recursive=True,
        existing='reconfigure',
        access_protocol='ssh',
        credential=None,
        private=False
    )
    _ = ds.siblings(
        name='origin',
        recursive=True,
        action='remove'
    )
    return


def create_dataset(dataset, path=None):
    """
    Invoke Datalad's create_dataset command
    :param dataset: (str or Path) the dataset parameter of Datalad's create_dataset command
    :param path: (str or Path) the path parameter of Datalad's create_dataset command
    :return: no return value
    """
    dataset = Path(dataset).expanduser().resolve()
    if path is not None:
        path = Path(path).expanduser().resolve()
    dl.create(dataset=dataset, path=path, cfg_proc="text2git")


def drop_content(dataset: str | os.PathLike, path: str | os.PathLike = None, recursive: bool = False) -> None:
    """
    Drop local annexed content after confirming availability elsewhere.
    :param dataset: (str, os.Pathlike) path to the dataset for which to drop local content
    :param path: (str, os.Pathlike) relative or absolute path to the dataset component for which to drop local
                 content, defaults to None which will drop all components of the dataset
    :param recursive: Whether to recursively step into subdatasets.
    :return: no return value
    """
    if path is not None and recursive:
        raise ValueError("Providing file paths and recursive=True is incompatible.")
    dataset = Path(dataset).expanduser().resolve()
    if path is not None:
        path = Path(path)
        if not path.is_absolute():
            path = dataset / path
        path = path.expanduser()
        # do not resolve symlink of potential annexed file
        path = path.parent.resolve() / path.name
        # Sanity check: ensure path lies within dataset
        try:
            path.relative_to(dataset)
        except ValueError:
            raise ValueError(f"{path} is not inside root {dataset}")

    # dl.drop showed connection issues reaching the GIN server -> replace with git annex version
    # dl.drop(dataset=str(dataset), path=path, recursive=recursive, what='filecontent')
    if recursive:
        _ = _run_git(["annex", "drop", "--all"], cwd=Path(str(dataset)))
        sibs = dl.siblings(dataset=str(dataset), action="query", return_type="list", recursive=recursive)
        for sibling in sibs:
            # run annex copy manually, since Datalad implementation proved to be brittle
            _ = _run_git(["annex", "drop", "--all"], cwd=Path(sibling["path"]))
    else:
        _ = _run_git(["annex", "drop", str(path.name)], cwd=Path(str(dataset)))


def find_dataset_root_and_rel(path: str | Path) -> tuple[Path | None, Path | None]:
    """
    Walk up from `path` until we find a directory containing a dataset.
    Returns (ds_root, relpath_within_dataset) or (None, None) if not found.
    If `path` is the dataset root, relpath is Path('.').

    :param path: (Path or str) item path to start walking up from
    """
    path = Path(path).resolve()
    ds_root = None
    rel = None
    if path.exists() or path.is_symlink():
        # if it's a file/symlink, start from parent when searching dataset root
        p = path if path.is_dir() else path.parent

        while True:
            ds = Dataset(p)
            if ds.is_installed():
                ds_root = p
                # rel path is relative to ds_root; for the dataset itself, use '.'
                rel = path.relative_to(ds_root)
                break
            if p.parent == p:
                break
            p = p.parent
    return ds_root, rel


def get_content(dataset: str | os.PathLike, path: str | os.PathLike | list[str | os.PathLike] | None = None,
                recursive: bool = False) -> None:
    """
    Retrieve annexed file content (bytes).
    :param dataset: (str, os.Pathlike) path to the dataset to update from GIN
    :param path: (str, os.Pathlike) path to the dataset component to retrieve content for, defaults to
                 None which will obtain all components of the dataset
    :param recursive: whether to recursively step into subdatasets
    :return: no return value
    """
    dataset = Path(dataset).expanduser().resolve()
    if path is not None and recursive:
        raise ValueError("Providing file paths and recursive=True is incompatible.")
    if path is None:
        targets = []
    elif isinstance(path, (str, os.PathLike, Path)):
        targets = [path]
    else:
        targets = list(path)

    for i, p in enumerate(targets):
        p = Path(p)
        if not p.is_absolute():
            p = dataset / p
        p = p.expanduser()
        # do not resolve symlink of potential annexed file
        p = p.parent.resolve() / p.name
        targets[i] = p
        # Sanity check: ensure path lies within dataset
        try:
            p.relative_to(dataset)
        except ValueError:
            raise ValueError(f"{p} is not inside root {dataset}")

    if not targets:
        # datlad showed issues in git annex transfers -> use git annex directly
        # dl.get(dataset=str(dataset), recursive=recursive)
        _ = _run_git(["annex", "get", "--all"], cwd=Path(str(dataset)))
        if recursive:
            sibs = dl.siblings(dataset=str(dataset), action="query", return_type="list", recursive=recursive)
            for sibling in sibs:
                # run annex copy manually, since Datalad implementation proved to be brittle
                _ = _run_git(["annex", "get", "--all"], cwd=Path(sibling["path"]))
    else:
        for p in targets:
            # dl.get(dataset=str(dataset), path=str(p) if path else None, recursive=recursive)
            directory = p if p.is_dir() else p.parent
            name = '--all' if p.is_dir() else str(p.name)
            _ = _run_git(["annex", "get", name], cwd=directory)


def get_dataset_nodetype(ds_path: str | Path):
    """
    Determine the nodetype of a dataset based on its position to the datamanager root directory. Node types are
    'root', 'project', 'campaign', and 'experiment'. The nodetype 'below-experiment' is given for folders below
    the experiment level, which are not a dataset.
    :param ds_path: (str or Path) dataset path
    :return: (str, str | Path) node type, path to dataset
    """
    ds_path = Path(str(ds_path)).expanduser().resolve()
    stepup_counter = 0

    if ds_path.is_file() or ds_path.is_symlink():
        ds_path = ds_path.parent
    ds = Dataset(ds_path)

    while not ds.is_installed():
        has_parent = ds_path.parent != ds_path
        if has_parent:
            ds_path = ds_path.parent
            ds = Dataset(ds_path)
            stepup_counter += 1
        else:
            return 'outside datalad', None

    meta = md.Metadata(ds_path)
    metadata = meta.get()
    if 'dataset_type' not in metadata:
        return 'not a datamanager repository', ds_path

    node_type = metadata["dataset_type"]
    if node_type == 'experiment' and stepup_counter == 0:
        node_type = 'below-experiment'
    return node_type, ds_path


def get_dataset_status(
               dataset: str | os.PathLike = None,
               recursive: bool = False):
    """
    Retrieves the DataLad status of a dataset.
    :param dataset: path to the dataset, defaults to None which will retrieve the status of the entire repository.
    :param recursive: whether to recursively step into subdatasets
    :return: (tuple): (bool) dir exists, (bool) dataset is installed, (dict) status
    """
    dataset_path = Path(str(dataset)).expanduser().resolve()
    if not dataset_path.is_dir():
        return False, False, None
    ds = Dataset(str(dataset_path))
    if not ds.is_installed():
        return True, False, None
    status = ds.status(recursive=recursive)
    return True, True, status


def get_dirty_items(dataset_path: Path, status = None, return_top_level=True) -> set[str]:
    """
    Use DataLad's dataset status records to obtain a set of relative item paths in the dataset that have not been
    saved, yet.
    :param dataset_path: (str | Path) dataset path
    :param status: (str) [Optional] the dataset status (if not provided), the status will be obtained in this function
    :param return_top_level: (bool) [Optional] to return all ancestor prefixes of each dirty item relative to the
                              dataset path. This is useful for viusalization of directories, where one wants to mark
                              the entire branch as dirty not just the item (leaf).
    :return: (set[str]) set of relative item paths for dirty items
    """
    dataset_path = Path(str(dataset_path)).expanduser().resolve()
    if status is None:
        _, _, status = get_dataset_status(dataset=dataset_path, recursive=False)

    dirty_names: set[str] = set()
    if not status:
        return dirty_names

    for entry in status:
        if entry.get("state") == "clean":
            continue
        entry_path = entry.get("path")
        if not entry_path:
            continue
        try:
            entry_path = Path(str(entry_path)).expanduser().resolve()
        except ValueError:
            continue
        # Skip the dataset root itself, list only children
        if entry_path == dataset_path:
            continue
        try:
            rel = entry_path.relative_to(dataset_path)
        except ValueError:
            continue
        if not rel.parts:
            continue
        if return_top_level:
            relpath = Path(rel.parts[0])
            dirty_names.add(relpath.as_posix())
            for part in rel.parts[1:]:
                relpath = relpath / part
                dirty_names.add(relpath.as_posix())
        else:
            dirty_names.add(rel.as_posix())

    return dirty_names


def get_git_sync_status(
        dataset: str | os.PathLike,
        sibling_name: str = "gin",
        branch: str | None = None,
        fetch: bool = True,
        from_parent: bool = False
) -> Dict[str, Any]:
    """
    Determine whether a local Git/DataLad dataset branch is up to date with, ahead of,
    behind, or diverged from its remote tracking branch.

    Notes:
      - This compares Git commit history only. It does not verify git-annex content
        availability on the remote.
      - If `branch` is not provided, the current checked-out branch is used.
      - If an upstream tracking branch is configured for the selected branch, that
        upstream is preferred. Otherwise, the method falls back to `{remote_name}/{branch}`
        if such a remote branch exists.

    :param dataset: Path to the Git/DataLad repository.
    :param sibling_name: Preferred remote name, e.g. 'gin' or 'origin'.
    :param branch: Optional local branch name. Defaults to the current branch.
    :param fetch: Whether to run `git fetch <remote_name>` before comparison.
    :param from_parent: whether to apply this function to the parent of the dataset instead
    :return: Dictionary with status information.
    """
    dataset = Path(dataset).expanduser().resolve()
    if from_parent:
        child = Dataset(str(dataset))
        if child.is_installed():
            ds = child.get_superdataset()
        else:
            # It's o.k. that this is not the parent. ds is not installed and will be rejected a few lines below.
            ds = child
    else:
        ds = Dataset(str(dataset))

    if ds is not None and not ds.is_installed():
        return {
            "ok": False,
            "state": "not_dataset",
            "message": f"Not an installed dataset: {dataset}",
            "repo_path": str(dataset),
        }

    has_remote, remote_info = has_sibling(dataset=dataset, sib_name=sibling_name)
    if not has_remote:
        return {
            "ok": True,
            "state": "no_remote",
            "message": f"No sibling named '{sibling_name}' is configured.",
            "repo_path": str(dataset),
            "remote_name": sibling_name,
            "remote": remote_info,
        }

    if fetch:
        fetch_res = _run_git(["fetch", sibling_name], cwd=dataset)
        if fetch_res.returncode != 0:
            return {
                "ok": False,
                "state": "fetch_failed",
                "message": fetch_res.stderr.strip() or fetch_res.stdout.strip() or "git fetch failed.",
                "repo_path": str(dataset),
                "remote_name": sibling_name,
            }

    if branch is None:
        branch_res = _run_git(["branch", "--show-current"], cwd=dataset)
        if branch_res.returncode != 0:
            return {
                "ok": False,
                "state": "branch_failed",
                "message": branch_res.stderr.strip() or branch_res.stdout.strip() or
                           "Could not determine current branch.",
                "repo_path": str(dataset),
                "remote_name": sibling_name,
            }
        branch = branch_res.stdout.strip()

    if not branch:
        return {
            "ok": True,
            "state": "detached_head",
            "message": "Repository is in detached HEAD state.",
            "repo_path": str(dataset),
            "remote_name": sibling_name,
        }

    upstream_res = _run_git(
        ["rev-parse", "--abbrev-ref", "--symbolic-full-name", f"{branch}@{{upstream}}"],
        cwd=dataset,
    )
    if upstream_res.returncode == 0:
        upstream = upstream_res.stdout.strip()
    else:
        candidate_upstream = f"{sibling_name}/{branch}"
        verify_res = _run_git(["rev-parse", "--verify", candidate_upstream], cwd=dataset)
        if verify_res.returncode != 0:
            return {
                "ok": True,
                "state": "no_upstream",
                "message": (
                    f"No upstream tracking branch is configured for local branch '{branch}', "
                    f"and remote branch '{candidate_upstream}' was not found."
                ),
                "repo_path": str(dataset),
                "remote_name": sibling_name,
                "branch": branch,
            }
        upstream = candidate_upstream

    counts_res = _run_git(
        ["rev-list", "--left-right", "--count", f"{branch}...{upstream}"],
        cwd=dataset,
    )
    if counts_res.returncode != 0:
        return {
            "ok": False,
            "state": "compare_failed",
            "message": counts_res.stderr.strip() or counts_res.stdout.strip() or "Branch comparison failed.",
            "repo_path": str(dataset),
            "remote_name": sibling_name,
            "branch": branch,
            "upstream": upstream,
        }

    counts_text = counts_res.stdout.strip()
    try:
        local_only_str, remote_only_str = counts_text.split()
        local_only = int(local_only_str)
        remote_only = int(remote_only_str)
    except ValueError:
        return {
            "ok": False,
            "state": "parse_failed",
            "message": f"Unexpected rev-list output: {counts_text!r}",
            "repo_path": str(dataset),
            "remote_name": sibling_name,
            "branch": branch,
            "upstream": upstream,
        }

    if local_only == 0 and remote_only == 0:
        state = "up_to_date"
    elif local_only > 0 and remote_only == 0:
        state = "ahead"
    elif local_only == 0 and remote_only > 0:
        state = "behind"
    else:
        state = "diverged"

    return {
        "ok": True,
        "state": state,
        "message": None,
        "repo_path": str(dataset),
        "remote_name": sibling_name,
        "branch": branch,
        "upstream": upstream,
        "local_only_commits": local_only,
        "remote_only_commits": remote_only,
        "remote": remote_info,
    }

def is_gitignored(dataset_path: Path, child_path: Path) -> bool:
    """
    Return True if a direct child of the current dataset matches a simple dataset-local
    .gitignore entry.
    :param dataset_path: path to parent dataset
    :param child_path: path to child item
    :return: True if a direct child of the current dataset is listed in .gitignore
    """
    patterns = read_gitignore(dataset_path)
    try:
        rel = child_path.relative_to(dataset_path)
    except ValueError:
        return False

    rel_str = rel.as_posix()
    rel_dir_str = rel_str + "/"

    for pattern in patterns:
        pattern = pattern.strip()
        if not pattern or pattern.startswith("#"):
            continue

        if pattern == rel_str:
            return True
        if child_path.is_dir() and pattern == rel_dir_str:
            return True

    return False


def has_content(dataset: str | os.PathLike, path: str | os.PathLike) -> bool:
    """
    Checks if content under path is installed (locally available) in dataset, as opposed to being remote
    :param dataset: path to dataset
    :param path: absolute or relative path to content file
    :return: (bool) whether the conten is locally available
    """
    dataset = Path(dataset).expanduser().resolve()
    path = Path(path)
    if not path.is_absolute():
        path = dataset / path
    path = path.expanduser()
    # do not resolve symlink of potential annexed file
    path = path.parent.resolve() / path.name
    relative_path = path.relative_to(dataset)

    return dl.Dataset(str(dataset)).repo.file_has_content(str(relative_path))


def has_sibling(dataset: str | os.PathLike, sib_name: str | None = None):
    """
    Checks is sibling under path is installed in dataset
    :param dataset: path to dataset
    :param sib_name: (str) sibling name (i.e. GIN)
    :return: (bool) Whether a sibling exists in dataset, (dict) sibling query result
    """

    dataset = Path(dataset).expanduser().resolve()
    ds = Dataset(str(dataset))

    if not ds.is_installed():
        return False, None

    sibs = dl.siblings(dataset=str(dataset), action="query", return_type="list", recursive=False)
    if sibs and sib_name is None:
        # any sibling is acceptable
        return True, sibs[0]

    dicts = [s for s in sibs if s.get("name") and s["name"] == sib_name]
    if dicts:
        return True, dicts[0]
    else:
        return False, None


def pull_from_remotes(dataset: str | os.PathLike,
                      recursive: bool = True,
                      sibling_name: str = None) -> None:
    """
    Pull latest history from GIN and merge.
    :param dataset: (str) path to the dataset to update from remotes
    :param recursive: whether recursively pull from remotes, default True
    :param sibling_name: (str) name of the sibling datasets to pull from (such as 'gin' for GIN publishing),
                         default: None (recommended), which self-determines the target to pull from
    :return: no return value
    """
    ds = Dataset(str(dataset))
    # ds.save_dataset(recursive=recursive, message='save_dataset before update from remote')
    ds.update(recursive=recursive, how='merge', sibling=sibling_name)
    ds.get(recursive=recursive, get_data=False)
    ds.save(recursive=recursive, message='updated from remote')


def push_to_remotes(dataset: str | os.PathLike, recursive: bool = True, message: str | None = None,
                    sibling_name: str = None, push_annex_data: bool = True) -> None:
    """
    Save and push commits + annexed content to GIN.
    :param dataset: (str, os.Pathlike) path to the dataset to push to GIN
    :param recursive: (bool) whether to recursively push subdatasets
    :param sibling_name: (str) name of the sibling datasets to push to GIN
    :param message: (str) optional commit message to push to GIN.
    :param push_annex_data: (bool) whether to push annexed content to GIN
    :return: no return value
    """
    ds = Dataset(str(dataset))
    if message:
        ds.save(recursive=recursive, message=message)
    else:
        ds.save(recursive=recursive)

    sibs = ds.siblings(action="query", return_type="list", recursive=recursive)
    if sibling_name is None:
        names = {s["name"] for s in sibs if s.get("name")}
        sibling_name = "gin" if "gin" in names else ("origin" if "origin" in names else None)
    if not sibling_name:
        raise RuntimeError("No publication target configured and no 'gin'/'origin' sibling found.")

    ds.push(to=sibling_name, recursive=recursive, data="nothing")
    if push_annex_data:
        for sibling in sibs:
            if sibling["name"] != sibling_name:
                continue
            # run annex copy manually, since Datalad implementation proved to be brittle
            _ = _run_git(["config", f"remote.{sibling_name}.annex-ignore", "false"],
                              cwd=Path(sibling["path"]))
            _ = _run_git(["annex", "copy", "--to", sibling_name, "--all"], cwd=Path(sibling["path"]))

def read_gitignore(dataset_path: Path) -> list[str]:
    """
    Read simple dataset-local .gitignore patterns.
    Currently, supports the common folder/file patterns used in this app.
    :param dataset_path: (Path) path to dataset
    :return: (list[str]) list of paths in .gitignore
    """
    gitignore_path = dataset_path / ".gitignore"
    if not gitignore_path.is_file():
        return []

    patterns: list[str] = []
    try:
        for line in gitignore_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            patterns.append(line)
    except IOError:
        return []

    return patterns


def remove_siblings(dataset: str | os.PathLike, sibling_name: str = 'gin', recursive: bool = False) -> None:
    """
    Removes all sibling datasets from tree.
    :param dataset: (str or Path) root path to remove sibling datasets from
    :param sibling_name: (str) sibling name to match
    :param recursive: (bool) whether to recursively step into subdatasets
    :return: no return value
    """
    ds_path = Path(dataset).expanduser().resolve()
    ds = Dataset(str(ds_path))

    if sibling_name is None:
        sibs = ds.siblings(action="query", return_type="list", recursive=recursive)
        names = {s["name"] for s in sibs if s.get("name")}
        sibling_name = "gin" if "gin" in names else ("origin" if "origin" in names else None)
    if not sibling_name:
        raise RuntimeError("No remote target configured and no 'gin'/'origin' sibling found.")

    dl.siblings(action='remove', dataset=ds_path, name=sibling_name, recursive=recursive)

def save_branch(path: str | Path, recursive: bool = True, message: str = None) -> None:
    """
    Saves an entire datalad branch, walking from the given dataset path up to root. The path given can point to
    content below the dataset.

    :param path: (str | Path) path to dataset or content nested within
    :param recursive: (bool) whether to recursively step into the lowest-hierarchy subdatasets
    :param message: (str) optional commit message to add only to the lowest-hierarchy dataset
    :return: No return value
    """
    path = Path(path).expanduser().resolve()
    ds_root = save_dataset(path=path, recursive=recursive, message=message)
    while True:
        ds_root = ds_root.parent
        ds = Dataset(str(ds_root))
        if ds.is_installed():
            save_dataset(path=ds_root, recursive=False)
        else:
            break


def save_dataset(path: str | os.PathLike,
                 recursive: bool = True,
                 message: str = None) -> Path:
    """
    Saves the current dataset to disk. Path can point to nested item in the dataset. The function will walk up
    the file tree until it finds a dataset.

    :param path: (str or Path) path to the dataset or content in dataset
    :param recursive: (bool) step recursively into subdatasets
    :param message: (str) optional commit message
    :return: (Path) the identified root directory of the dataset
    """
    path = Path(path).resolve().absolute()
    ds_root, rel = find_dataset_root_and_rel(path)

    if str(rel) == '.':
        # save dataset
        dl.save(dataset=str(ds_root), recursive=recursive, message=message)
    else:
        # just save content, if path is not that of a subdataset
        dl.save(dataset=str(ds_root), path=str(path), recursive=False, message=message)

    return ds_root


def siblings(dataset, *, recursive=False, action='query'):
    """
    Invokes Datalad's siblings function
    :param dataset: (str or Path) parent dataset
    :param recursive: (bool) recursively step into subdatasets
    :param action: (str) action to perform
    :return: Datalad's return formatted as a list
    """
    dataset = Path(dataset).expanduser().resolve()
    sibs = dl.siblings(dataset=str(dataset), action=action, recursive=recursive, return_type="list")
    return sibs if sibs else []


def set_git_annex_path():
    def which_in_zsh(exe="git-annex"):
        shell = os.environ.get("SHELL", "/bin/zsh")
        # -i => interactive so .zshrc is sourced; use the shell builtin `command -v`
        cmd = f"command -v {shlex.quote(exe)} || true"
        p = subprocess.run([shell, "-i", "-c", cmd],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        path = p.stdout.strip().splitlines()[-1] if p.stdout else ""
        return path or None

    if shutil.which("git-annex") is not None:
        return True

    path = which_in_zsh("git-annex")
    if path:
        os.environ["PATH"] = f"{os.path.dirname(path)}:{os.environ.get('PATH', '')}"
        return True
    else:
        return False


def ssh_to_https(u: str) -> str:
    # git@gin.g-node.org:/owner/repo(.git) -> https://gin.g-node.org/owner/repo
    if u.startswith('git@'):
        host = u.split('@', 1)[1].split(':', 1)[0]
        path = u.split(':', 1)[1]
        if path.endswith('.git'):
            path = path[:-4]
        return f"https://{host}{path}"
    return u
