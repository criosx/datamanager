#!/usr/bin/env python3
"""
Validate experiment layout. Fails (exit 1) if critical issues found.
Usage:
  datalad run-procedure scidata_validate dataset=<EXPERIMENT_PATH> strict=true
"""
import sys
from pathlib import Path

KINDS = ("models", "templates", "measurements", "experimental_optimization")


def main(argv):
    kv = dict(a.split("=",1) for a in argv if "=" in a)
    ds = Path(kv.get("dataset",".")).resolve()
    strict = kv.get("strict","false").lower() == "true"

    issues = []
    for sub in KINDS:
        p = ds / sub
        if not p.exists(): issues.append(f"missing dir: {sub}")

    # simple meta check
    if not (ds/".datalad").exists():
        issues.append("not a DataLad dataset (no .datalad/)")

    if strict:
        # forbid files at dataset root (must live under artifact dirs)
        roots = [p for p in ds.iterdir() if p.is_file()]
        if roots: issues.append(f"files at root not allowed: {[r.name for r in roots]}")

    if issues:
        for i in issues: print("ERROR:", i, file=sys.stderr)
        sys.exit(1)
    print("[scidata] validation OK")


if __name__ == "__main__":
    main(sys.argv[1:])
