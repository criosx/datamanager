#!/usr/bin/env python3
"""
Normalize arbitrary newly-added files into an experiment's artifact folder(s), tag with git-annex K/V,
write JSON-LD, and aggregate metadata.

Usage:
  datalad run-procedure scidata_intake dataset=<EXPERIMENT_PATH> kind=measurements name=run_YYYYMMDD role=raw
Notes:
  - Run from the experiment dataset (or pass dataset=<path> to it).
  - 'kind' ∈ {models,templates,measurements,experimental_optimization}
  - Will collect any files at the experiment root that are not already within those artifact dirs.
"""
import json, sys, subprocess, shlex
from pathlib import Path

KINDS = {"models", "templates", "measurements", "experimental_optimization"}


def sh(*args, check=True):
    return subprocess.run(["datalad", *args], check=check)


def annex_meta(path: Path, **kvs):
    for k,v in kvs.items():
        subprocess.run(["git","annex","metadata", str(path), "--set", f"{k}={v}"], check=False)


def meta_add(ds: Path, kind: str, name: str, payload_paths):
    extracted = {
      "@context":{"@vocab":"http://schema.org/","scidata":"https://example.org/scidata#"},
      "@type":"Dataset","name":name,
      "scidata:artifactKind": kind,
      "scidata:roles": [{"path": p, "role": "raw"} for p in payload_paths]
    }
    rec = {"type":"dataset","extractor_name":"scidata_artifact_v1","extracted_metadata": extracted}
    p = subprocess.Popen(["datalad","meta-add","-"], stdin=subprocess.PIPE, cwd=str(ds))
    p.communicate(json.dumps(rec).encode("utf-8"))


def main(argv):
    kv = dict(a.split("=",1) for a in argv if "=" in a)
    ds = Path(kv.get("dataset",".")).resolve()
    kind = kv.get("kind","measurements")
    name = kv.get("name","intake")
    role = kv.get("role","raw")
    if kind not in KINDS:
        print(f"ERROR: kind must be one of {KINDS}", file=sys.stderr); sys.exit(1)

    # find orphans (files not in artifact dirs)
    protected = KINDS | {".git", ".datalad", ".scidata"}

    def is_orphan(p: Path) -> bool:
        parts = set(p.relative_to(ds).parts[:1])
        return p.is_file() and parts.isdisjoint(protected)

    orphans = [p for p in ds.rglob("*") if is_orphan(p)]
    if not orphans:
        print("[scidata] nothing to intake"); return

    # destination subdir (create a new container)
    dest = ds / kind / name / ("raw" if role == "raw" else ".")
    dest.mkdir(parents=True, exist_ok=True)

    moved = []
    for f in orphans:
        tgt = dest / f.name
        tgt.parent.mkdir(parents=True, exist_ok=True)
        f.replace(tgt)
        moved.append(tgt)
        annex_meta(tgt, role=role, scidata_kind=kind)

    # save & metadata
    sh("save", f"dataset={ds}", "message=scidata: intake normalized files")
    meta_add(ds, kind, name, [str(p.relative_to(ds)) for p in moved])
    # aggregate up (ignore failure if MetaLad not installed)
    try: sh("meta-aggregate", f"dataset={ds}")
    except Exception: pass

    print(f"[scidata] intake -> {dest.relative_to(ds)} ({len(moved)} files)")


if __name__ == "__main__":
    main(sys.argv[1:])
