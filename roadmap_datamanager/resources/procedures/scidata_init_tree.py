#!/usr/bin/env python3
"""
Create (user)/(project)/(campaign) as nested DataLad datasets and save minimal JSON-LD at each level.
Usage (from root dataset path or anywhere):
  datalad run-procedure scidata_init_tree dataset=<ROOT> user=alice project=virusKinetics campaign=2025_summer
"""
import json
import sys
import shlex
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


def sh(*args, check=True):
    return subprocess.run(["datalad", *args], check=check)


def ensure_ds(p: Path):
    if not (p/".datalad").exists():
        sh("create", f"{p}")


def save_meta(ds: Path, node_type: str, name: str):
    ds = Path(ds).resolve()

    # Envelope fields required by datalad-metalad
    extractor_name = "scidata_node_v1"
    extractor_version = "1.0"
    extraction_parameter = {"node_type": node_type, "name": name}  # record what we stored
    extraction_time = datetime.now(timezone.utc).isoformat()  # RFC3339/ISO8601 with timezone

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


def main(argv):
    # parse key=value args (procedure convention)
    kv = dict(a.split("=", 1) for a in argv if "=" in a)
    root = Path(kv.get("dataset", ".")).expanduser().resolve()
    user = kv["user"]
    project = kv.get("project")
    campaign = kv.get("campaign")

    up = root
    pp = up / project if project else None
    cp = pp / campaign if (pp and campaign) else None

    ensure_ds(up)
    save_meta(up, "user", user)
    if pp:
        ensure_ds(pp)
        sh("subdatasets", f"dataset={up}", f"path={pp}", "add=true")
        save_meta(pp, "project", project)
    if cp:
        ensure_ds(cp)
        sh("subdatasets", f"dataset={pp}", f"path={cp}", "add=true")
        save_meta(cp, "campaign", campaign)

    # one save at the deepest node is enough (subdatasets add saves too)
    print(f"[scidata] initialized tree at {root} for {user}/{project}/{campaign}")


if __name__ == "__main__":
    main(sys.argv[1:])
