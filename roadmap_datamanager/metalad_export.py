#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from datetime import datetime


def guess_kind(ds: Path) -> str | None:
    for k in ("models", "templates", "measurements", "experimental_optimization"):
        if (ds/k).exists():
            return None  # looks like a node (experiment) not a single artifact
    # if ds contains a .scidata/marker, you could read kind; here we fall back:
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", default=".")
    ap.add_argument("--name", default=None)
    ap.add_argument("--kind", choices=["models", "templates", "measurements", "experimental_optimization"])
    args = ap.parse_args()

    ds = Path(args.dataset).resolve()
    name = args.name or ds.name
    kind = args.kind or guess_kind(ds)

    payload = {
      "type":"dataset",
      "extractor_name":"scidata_artifact_v1" if kind else "scidata_node_v1",
      "extracted_metadata":{
        "@context":{"@vocab":"http://schema.org/","scidata":"https://example.org/scidata#"},
        "@type":"Dataset",
        "name": name,
        "dateCreated": datetime.utcnow().isoformat(timespec="seconds")+"Z",
    }}
    if kind:
        payload["extracted_metadata"]["scidata:artifactKind"] = kind

    print(json.dumps(payload))


if __name__ == "__main__":
    main()
