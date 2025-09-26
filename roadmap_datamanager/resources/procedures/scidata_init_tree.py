#!/usr/bin/env python3
"""
Create (user)/(project)/(campaign) as nested DataLad datasets and save minimal JSON-LD at each level.
Usage (from root dataset path or anywhere):
  datalad run-procedure scidata_init_tree dataset=<ROOT> user=alice project=virusKinetics campaign=2025_summer
"""
import json
import os
import sys
import subprocess

from datetime import datetime, timezone
from pathlib import Path


def _git(ds: Path, *args, capture=True, check=True):
    kw = dict(check=check, text=True)
    if capture:
        kw.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return subprocess.run(["git", "-C", str(ds), *args], **kw)


def _get_dataset_id(ds: Path) -> str:
    # DataLad stores the dataset id in .datalad/config under key datalad.dataset.id
    p = _git(ds, "config", "-f", str(Path(ds, ".datalad/config")), "datalad.dataset.id")
    if p.returncode == 0 and p.stdout.strip():
        return p.stdout.strip()
    # Fallback: try datalad configuration (rarely needed)
    p = subprocess.run(["datalad", "-C", str(ds), "configuration", "get", "datalad.dataset.id"],
                       text=True, stdout=subprocess.PIPE)
    if p.returncode == 0 and p.stdout.strip():
        return p.stdout.strip()
    raise RuntimeError(f"Could not read dataset id in {ds} (.datalad/config missing?)")


def _get_dataset_version(ds: Path) -> str:
    p = _git(ds, "rev-parse", "HEAD")
    if p.returncode == 0:
        return p.stdout.strip()
    raise RuntimeError(f"Could not determine dataset version (HEAD) in {ds}")


def _get_git_identity(ds: Path) -> tuple[str, str]:
    name = _git(ds, "config", "--get", "user.name").stdout.strip() or ""
    email = _git(ds, "config", "--get", "user.email").stdout.strip() or ""
    # Safe fallbacks if not configured
    if not name:
        name = "unknown"
    if not email:
        email = "unknown@example.org"
    return name, email


def _is_registered_in_super(superds: Path, path: Path) -> bool:
    # Check .gitmodules for a submodule whose path matches `rel`
    rel = str(Path(os.path.relpath(path, superds)))
    p = subprocess.run(
        ["git", "-C", str(superds), "config", "-f", ".gitmodules",
         "--get-regexp", r"^submodule\..*\.path$"],
        text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    if p.returncode != 0 or not p.stdout:
        return False
    for line in p.stdout.splitlines():
        # "<key> <value>" where value is the path recorded for the submodule
        parts = line.split(None, 1)
        if len(parts) == 2 and parts[1].strip() == rel:
            return True
    return False


def _register_existing_subdataset(superds: Path, path: Path):
    if _is_registered_in_super(superds, path):
        return
    rel = os.path.relpath(path, superds)
    # Idempotent: if already registered, this returns "notneeded"
    sh("-C", str(superds), "subdatasets", "--add", rel)
    sh("-C", str(superds), "save", "-m", f"Register existing subdataset {rel}")


def sh(*args, check=True, capture=False):
    # Example: sh("-C", str(path), "create", "-d", str(super), str(child))
    kw = dict(check=check, text=True)
    if capture:
        kw.update(stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    return subprocess.run(["datalad", *args], **kw)


def ensure_ds(path: Path, superds: Path = None):
    """
    Idempotently ensure that `path` is a DataLad dataset.
    If `superds` is given, create/register it as a subdataset of `superds`.
    """
    path = path.resolve()
    if (path / ".datalad").exists():
        # Already a dataset: if super is specified, ensure registration in super
        if superds is not None:
            _register_existing_subdataset(superds.resolve(), path)
        return

    path.parent.mkdir(parents=True, exist_ok=True)
    if superds is None:
        sh("create", str(path))
    else:
        # Create and register in one step
        rel = os.path.relpath(path, superds)
        sh("-C", str(superds), "create", "-d", str(superds), rel)


def save_meta(ds: Path, node_type: str, name: str):
    ds = Path(ds).resolve()

    # Envelope fields required by datalad-metalad
    extractor_name = "scidata_node_v1"
    extractor_version = "1.0"
    extraction_parameter = {"node_type": node_type, "name": name}  # record what we stored
    extraction_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()  # RFC3339/ISO8601 with timezone

    dataset_id = _get_dataset_id(ds)
    dataset_version = _get_dataset_version(ds)
    agent_name, agent_email = _get_git_identity(ds)

    payload = {
        "type": "dataset",
        "extractor_name": extractor_name,
        "extractor_version": extractor_version,
        "extraction_parameter": extraction_parameter,
        "extraction_time": extraction_time,
        "agent_name": agent_name,
        "agent_email": agent_email,
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
        ["datalad", "meta-add", "-d", str(ds), "-"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    out, err = p.communicate(json.dumps(payload))
    if p.returncode != 0:
        raise RuntimeError(f"meta-add failed for {ds} ({node_type}={name}): {err.strip()}")
    # Ensure metadata is recorded even if this ds isn’t registered yet
    sh("-C", str(ds), "save", "-m", f"scidata: metadata for {node_type}={name}")


def main(argv):
    # parse key=value args (procedure convention)
    kv = dict(a.split("=", 1) for a in argv if "=" in a)
    if "user" not in kv:
        raise SystemExit("missing required argument: user=<name>")

    root = Path(kv.get("dataset", ".")).expanduser().resolve()
    user = kv["user"]
    project = kv.get("project")
    campaign = kv.get("campaign")

    # Construct paths
    up = root
    pp = up / project if project else None
    cp = pp / campaign if (pp and campaign) else None

    # Ensure datasets (registering children in their super)
    ensure_ds(up, superds=None)
    save_meta(up, "user", user)

    if pp:
        ensure_ds(pp, superds=up)
        save_meta(pp, "project", project)

    if cp:
        ensure_ds(cp, superds=pp)
        save_meta(cp, "campaign", campaign)

    # One save at the top-level will record state; use -r to include subs.
    sh("-C", str(up), "save", "-r", "-m", f"scidata: initialized tree for {user}/{project or ''}/{campaign or ''}")

    # Friendly final message
    pieces = [user]
    if project:
        pieces.append(project)
    if campaign:
        pieces.append(campaign)
    print(f"[scidata] initialized/verified tree at {root} for {'/'.join(pieces)}")


if __name__ == "__main__":
    main(sys.argv[1:])
