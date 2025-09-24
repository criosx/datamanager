from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Iterable, Optional
from datalad import api as dl


@dataclass(frozen=True)
class TreeCtx:
    root: Path
    user: str
    project: Optional[str] = None
    campaign: Optional[str] = None
    experiment: Optional[str] = None


def slug(s: str) -> str:
    return s.strip().replace(" ", "_")


def p_user(ctx: TreeCtx) -> Path:
    return ctx.root / slug(ctx.user)


def p_project(ctx: TreeCtx) -> Path:
    return p_user(ctx) / slug(ctx.project or "project")


def p_campaign(ctx: TreeCtx) -> Path:
    return p_project(ctx) / slug(ctx.campaign or "campaign")


def p_experiment(ctx: TreeCtx) -> Path:
    return p_campaign(ctx) / slug(ctx.experiment or "experiment")


def ensure_dataset(path: Path):
    if not (path / ".datalad").exists():
        dl.create(path=str(path), force=True)
    return path


def register_subdataset(parent: Path, child: Path):
    # add child as subdataset relative to parent
    dl.subdatasets(dataset=str(parent), path=str(child), add=True)


def save(path: Path, msg: str):
    dl.save(path=str(path), message=msg)


def add_files(ds_path: Path, files: Iterable[Path], to_git: bool = False):
    dl.add(dataset=str(ds_path), path=[str(f) for f in files], to_git=to_git)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
