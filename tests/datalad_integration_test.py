import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from roadmap_datamanager.roadmap_datamanager import DataManager
from roadmap_datamanager.helpers import set_git_annex_path

# --------- hard requirements check (do NOT silently skip) ----------
ENV_ERRORS = []

# 1) DataLad import
try:
    import datalad
except ImportError:
    ENV_ERRORS.append("datalad (Python package) not importable in this interpreter")

# 2) datalad-metalad CLI available
try:
    p = subprocess.run(["datalad", "meta-dump", "-h"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        ENV_ERRORS.append("datalad-metalad CLI not available (failed: `datalad meta-dump -h`)")
except FileNotFoundError:
    ENV_ERRORS.append("`datalad` executable not on PATH for this Python process")


# 3) git-annex available and recent enough
def _annex_ok():
    try:
        set_git_annex_path()
        out = subprocess.check_output(["git-annex", "version"], text=True)
    except ImportError:
        return "git-annex not found on PATH for this Python process"
    # Optional: light version check (DataLad requires ≥ 8.20200309)
    # We just assert a version line exists; DataLad will enforce exact min version later.
    if "git-annex version:" not in out:
        return "git-annex present but version string not detected"
    return None


annex_err = _annex_ok()
if annex_err:
    ENV_ERRORS.append(annex_err)

ENV_READY = not ENV_ERRORS


class TestEnvironment(unittest.TestCase):
    def test_000_requirements_present(self):
        """Fail (not skip) if required tooling is missing."""
        if not ENV_READY:
            self.fail(
                "Environment not ready for integration tests:\n"
                + "\n".join(f"- {msg}" for msg in ENV_ERRORS)
                + "\n\nFix: ensure `datalad`, `datalad-metalad`, and `git-annex` are on PATH "
                  "for THIS Python process (conda activate hooks or PATH shim)."
            )


@unittest.skipUnless(ENV_READY, "Environment check failed; see TestEnvironment.test_000_requirements_present")
class DataManagerE2ETest(unittest.TestCase):
    def test_init_tree_end_to_end(self):
        import tempfile
        root_dir = tempfile.mkdtemp()
        root = Path(root_dir)

        dm = DataManager(
            root,
            user_name="Frank Heinrich",
            user_email="fheinrich@cmu.edu",
            organization="CMU / NCNR",
            default_project="roadmap",
            datalad_profile="text2git",
        )

        # run twice to assert idempotency
        dm.init_tree(project="roadmap", campaign="2025_summer")
        dm.init_tree(project="roadmap", campaign="2025_summer")

        up = root
        pp = up / "roadmap"
        cp = pp / "2025_summer"

        # datasets exist
        self.assertTrue((up / ".datalad").exists())
        self.assertTrue((pp / ".datalad").exists())
        self.assertTrue((cp / ".datalad").exists())

        # meta present at each level
        def has_meta(ds: Path, node_type: str, name: str) -> bool:
            p = subprocess.run(
                ["datalad", "meta-dump", "-d", str(ds)],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True
            )
            for line in p.stdout.splitlines():
                if not line.strip():
                    continue
                obj = json.loads(line)
                if (
                    obj.get("extractor_name") == "scidata_node_v1"
                    and obj.get("extracted_metadata", {}).get("scidata:nodeType") == node_type
                    and obj.get("extracted_metadata", {}).get("name") == name
                ):
                    return True
            return False

        self.assertTrue(has_meta(up, "user", "Frank Heinrich"))
        self.assertTrue(has_meta(pp, "project", "roadmap"))
        self.assertTrue(has_meta(cp, "campaign", "2025_summer"))


if __name__ == "__main__":
    unittest.main()
