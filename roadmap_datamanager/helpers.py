from __future__ import annotations
from dataclasses import dataclass

from datalad.support.exceptions import IncompleteResultsError
from datalad import api as dl
from datalad.api import Dataset

from pathlib import Path, PurePosixPath
import shutil
import subprocess
import shlex
from typing import Optional
import os


@dataclass(frozen=True)
class TreeCtx:
    root: Path
    user: str
    project: Optional[str] = None
    campaign: Optional[str] = None
    experiment: Optional[str] = None


def ensure_paths(ds_path, path):
    """
    Consolidates and standardizes paths for class methods
    :param ds_path: (str or Path) dataset path
    :param path: (str or Path) content item path
    :return: (Path, Path, Path, str) datataset, relative content item path,
              absolute content item paths, relative path in posix format
    """
    ds_path = Path(ds_path).resolve()
    ds = Dataset(ds_path)
    if not ds.is_installed():
        raise RuntimeError(f"Dataset not installed at {ds_path}")

    # Ensure rel_path is relative and normalized to POSIX for stable identifiers
    if path is None:
        path = Path()
    else:
        path = Path(path)
    if path.is_absolute():
        try:
            path = path.relative_to(ds_path)
        except NotADirectoryError:
            raise ValueError(f"rel_path must be relative to {ds_path}, or valid absolute path. I got: {path}")

    # POSIX-normalized relative path string, '' for dataset itself
    relposix = '.' if path == Path() else str(PurePosixPath(*path.parts))

    # Working tree probe (only if materialized); use dataset root when rel_path is empty
    absolute_path = ds_path if relposix == '.' else (ds_path / path)

    return ds_path, path, absolute_path, relposix


def find_dataset_root_and_rel(path: str | Path, dm_root: str | Path) -> tuple[Path | None, Path | None]:
    """
    Walk up from `path` until we find a directory containing a dataset.
    Returns (ds_root, relpath_within_dataset) or (None, None) if not found.
    If `path` is the dataset root, relpath is Path('.').
    :param path: (Path or str) item path to start walking up from
    :param dm_root: (Path or str) datamanager root directory
    """
    path = Path(path).resolve()
    dm_root = Path(dm_root).resolve()

    if not (path.exists() or path.is_symlink()):
        return None, None

    # if it's a file/symlink, start from parent when searching dataset root
    search_from = path if path.is_dir() else path.parent

    # climb up until DM root
    dm_root = Path(dm_root)
    p = search_from
    while True:
        if p.is_dir() and (p / ".datalad").exists() or (p / ".git").exists():
            ds_root = p
            # rel path is relative to ds_root; for the dataset itself, use '.'
            rel = path.relative_to(ds_root)
            return ds_root, rel
        if dm_root and (p == dm_root or p == dm_root.parent):
            break
        if p.parent == p:
            break
        p = p.parent
    return None, None


def get_dataset_version(ds: Dataset) -> str:
    try:
        return ds.repo.get_hexsha()
    except IncompleteResultsError:
        # ensure at least one commit by touching .gitignore and saving
        (Path(ds.path) / ".gitignore").touch(exist_ok=True)
        dl.save(dataset=str(ds.path), path=[str(Path(ds.path) / ".gitignore")], message="Initial commit (auto)")
        return ds.repo.get_hexsha()


def get_dataset_nodetype(ds_path: str | Path, dm_root: str | Path) -> str:
    """
    Determine the node type of a dataset based on its position to the datamanager root directory. Node types are
    'user', 'project', 'campaign', and 'experiment'. The nodetype 'below-experiment' is given for folders below
    the experiment level, which are not a dataset.
    :param ds_path: (str or Path) dataset path
    :param dm_root: (str or Path) datamanager root directory
    :return: (str) node type
    """
    # creative use of ensure_paths
    dm_root, ds_path, ds_absolute_path, ds_relposix = ensure_paths(dm_root, ds_path)

    level = len(ds_path.parts)
    if ds_relposix == '.':
        return 'user'
    elif level == 1:
        return 'project'
    elif level == 2:
        return 'campaign'
    elif level == 3:
        return 'experiment'

    return 'below-experiment'


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
        return f"https://{host}/{path}"
    return u

