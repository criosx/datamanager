from __future__ import annotations

from pathlib import Path, PurePosixPath

from datalad import api as dl
from datalad.distribution.dataset import Dataset
from datalad.support.exceptions import IncompleteResultsError


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
    rel_path = path

    return ds_path, rel_path, absolute_path, relposix


def get_dataset_id(dataset: str | Path) -> str | None:
    """
    Retrieve the dataset id
    :param dataset: (str or Path) the dataset path
    :return: (str) dataset id or None
    """
    dataset = Path(dataset).expanduser().resolve()
    ds = Dataset(str(dataset))
    if ds.is_installed():
        return ds.id
    else:
        return None


def get_dataset_version(ds: Dataset) -> str:
    try:
        return ds.repo.get_hexsha()
    except IncompleteResultsError:
        # ensure at least one commit by touching .gitignore and saving
        (Path(ds.path) / ".gitignore").touch(exist_ok=True)
        dl.save(dataset=str(ds.path), path=[str(Path(ds.path) / ".gitignore")], message="Initial commit (auto)")
        return ds.repo.get_hexsha()
