#!/usr/bin/env python3
"""
Create an experiment as a subdataset under (user/project/campaign).
Usage:
  datalad run-procedure scidata_new_experiment dataset=<ROOT> user=alice project=virusKinetics campaign=2025_summer name=exp_001_frap
"""
import json, sys, subprocess
from pathlib import Path


def sh(*args, check=True):
    return subprocess.run(["datalad", *args], check=check)


def meta(ds: Path, name: str):
    record = {
      "type":"dataset",
      "extractor_name":"scidata_node_v1",
      "extracted_metadata":{
        "@context":{"@vocab":"http://schema.org/","scidata":"https://example.org/scidata#"},
        "@type":"Dataset","name":name,"scidata:nodeType":"experiment"
      }
    }
    p = subprocess.Popen(["datalad","meta-add","-"], stdin=subprocess.PIPE)
    p.communicate(json.dumps(record).encode("utf-8"))


def main(argv):
    kv = dict(a.split("=",1) for a in argv if "=" in a)
    root = Path(kv.get("dataset",".")).resolve()
    user, project, campaign, name = kv["user"], kv["project"], kv["campaign"], kv["name"]
    parent = root / user / project / campaign
    exp = parent / name
    sh("create", f"path={exp}")
    sh("subdatasets", f"dataset={parent}", f"path={exp}", "add=true")
    # lay out artifact dirs (plain dirs; promote to subdatasets on demand)
    for sub in ("models","templates","measurements","experimental_optimization",".scidata"):
        (exp/sub).mkdir(parents=True, exist_ok=True)
    meta(exp, name)
    print(f"[scidata] new experiment: {exp}")


if __name__ == "__main__":
    main(sys.argv[1:])
