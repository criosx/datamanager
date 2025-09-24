#!/usr/bin/env python3
"""
Create (user)/(project)/(campaign) as nested DataLad datasets and save minimal JSON-LD at each level.
Usage (from root dataset path or anywhere):
  datalad run-procedure scidata_init_tree dataset=<ROOT> user=alice project=virusKinetics campaign=2025_summer
"""
import json, sys, shlex, subprocess
from pathlib import Path


def sh(*args, check=True):
    return subprocess.run(["datalad", *args], check=check)


def ensure_ds(p: Path):
    if not (p/".datalad").exists():
        sh("create", f"path={p}")


def save_meta(ds: Path, node_type: str, name: str):
    meta = {
      "type": "dataset",
      "extractor_name": "scidata_node_v1",
      "extracted_metadata": {
        "@context": {"@vocab": "http://schema.org/", "scidata":"https://example.org/scidata#"},
        "@type": "Dataset",
        "name": name, "scidata:nodeType": node_type
      }
    }
    p = subprocess.Popen(["datalad","meta-add","-"], stdin=subprocess.PIPE)
    p.communicate(json.dumps(meta).encode("utf-8"))


def main(argv):
    # parse key=value args (procedure convention)
    kv = dict(a.split("=", 1) for a in argv if "=" in a)
    root = Path(kv.get("dataset",".")).resolve()
    user = kv["user"]; project = kv.get("project"); campaign = kv.get("campaign")

    up = root / user
    pp = up / project if project else None
    cp = pp / campaign if (pp and campaign) else None

    ensure_ds(up); save_meta(up, "user", user)
    if pp:
        ensure_ds(pp); sh("subdatasets", f"dataset={up}", f"path={pp}", "add=true"); save_meta(pp, "project", project)
    if cp:
        ensure_ds(cp); sh("subdatasets", f"dataset={pp}", f"path={cp}", "add=true"); save_meta(cp, "campaign", campaign)

    # one save at the deepest node is enough (subdatasets add saves too)
    print(f"[scidata] initialized tree at {root} for {user}/{project}/{campaign}")


if __name__ == "__main__":
    main(sys.argv[1:])
