import json
import subprocess
import tempfile
import unittest
from pathlib import Path

from roadmap_datamanager.roadmap_datamanager import DataManager
from roadmap_datamanager.helpers import set_git_annex_path

# --------- hard requirements check (do NOT silently skip) ----------
ENV_ERRORS = []

# DataLad import
try:
    import datalad
except ImportError:
    ENV_ERRORS.append("datalad (Python package) not importable in this interpreter")

# datalad-metalad CLI available
try:
    p = subprocess.run(["datalad", "meta-dump", "-h"],
                       stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        ENV_ERRORS.append("datalad-metalad CLI not available (failed: `datalad meta-dump -h`)")
except FileNotFoundError:
    ENV_ERRORS.append("`datalad` executable not on PATH for this Python process")


# git-annex available and recent enough
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
class DataManagerInitTreeTest(unittest.TestCase):
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
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

        up = root
        pp = up / "roadmap"
        cp = pp / "2025_summer"
        ep = cp / "NR1_0"

        # datasets exist
        self.assertTrue((up / ".datalad").exists())
        self.assertTrue((pp / ".datalad").exists())
        self.assertTrue((cp / ".datalad").exists())
        self.assertTrue((ep / ".datalad").exists())

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
        self.assertTrue(has_meta(ep, "experiment", "NR1_0"))

@unittest.skipUnless(ENV_READY, "Environment check failed; see TestEnvironment.test_000_requirements_present")
class DataManagerInstallIntoTreeTest(unittest.TestCase):

    def _has_meta(self, ds: Path, *, node_type: str, name: str) -> bool:
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

    def _mk_temp_file(self, parent: Path, name: str, content: str = "x") -> Path:
        parent.mkdir(parents=True, exist_ok=True)
        p = parent / name
        p.write_text(content)
        return p

    def test_install_file_into_category_root(self):
        # Setup
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dm = DataManager(
                root,
                user_name="Frank Heinrich",
                user_email="fheinrich@cmu.edu",
                organization="CMU / NCNR",
                default_project="roadmap",
                datalad_profile="text2git",
            )
            dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

            up = root
            ep = up / "roadmap" / "2025_summer" / "NR1_0"
            cat = ep / "raw"

            # Source file
            src = self._mk_temp_file(root, "sample_raw.dat", "abc123")

            # Act: install into category root (no dest_rel)
            dm.install_into_tree(
                source=src,
                project="roadmap",
                campaign="2025_summer",
                experiment="NR1_0",
                category="raw",
            )

            # Assert: file lives under category dataset, not directly under experiment
            self.assertTrue((cat / "sample_raw.dat").exists(), "file not copied into category dataset")
            self.assertFalse((ep / "sample_raw.dat").exists(), "file must not be placed under experiment root")

            # Category is a dataset
            self.assertTrue((cat / ".datalad").exists(), "category is expected to be a dataset")

            # Metadata written at category level, including filename in the name field
            self.assertTrue(self._has_meta(cat, node_type="category", name="raw (sample_raw.dat)"))

    def test_install_folder_recursively_as_subdatasets(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dm = DataManager(
                root,
                user_name="Frank Heinrich",
                user_email="fheinrich@cmu.edu",
                organization="CMU / NCNR",
                default_project="roadmap",
                datalad_profile="text2git",
            )
            dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

            ep = root / "roadmap" / "2025_summer" / "NR1_0"
            cat = ep / "analysis"

            # Build a small folder tree to import
            src_dir = root / "to_import"
            (src_dir / "subA").mkdir(parents=True, exist_ok=True)
            (src_dir / "subB" / "deep").mkdir(parents=True, exist_ok=True)
            (src_dir / "subA" / "a.txt").write_text("A")
            (src_dir / "subB" / "deep" / "b.txt").write_text("B")

            # Act
            dm.install_into_tree(
                source=src_dir,
                project="roadmap",
                campaign="2025_summer",
                experiment="NR1_0",
                category="analysis",
                name="bundleA",
            )

            top = cat / "bundleA"
            subA = top / "subA"
            deep = top / "subB" / "deep"

            # Each directory replicated as a dataset (subdatasets)
            self.assertTrue((top / ".datalad").exists(), "top folder should be a dataset")
            self.assertTrue((subA / ".datalad").exists(), "subA should be a dataset")
            self.assertTrue((deep / ".datalad").exists(), "deep should be a dataset")

            # Files copied into their respective datasets
            self.assertTrue((subA / "a.txt").exists())
            self.assertTrue((deep / "b.txt").exists())

            # Metadata created on the top dataset for the folder
            self.assertTrue(self._has_meta(top, node_type="dataset", name="bundleA"))

    def test_install_into_existing_subdataset_with_dest_rel_file(self):
        from datalad import api as dl

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dm = DataManager(
                root,
                user_name="Frank Heinrich",
                user_email="fheinrich@cmu.edu",
                organization="CMU / NCNR",
                default_project="roadmap",
                datalad_profile="text2git",
            )
            dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

            ep = root / "roadmap" / "2025_summer" / "NR1_0"
            cat = ep / "analysis"

            # Ensure category dataset exists (e.g., by a no-op install of a tiny file)
            priming_file = self._mk_temp_file(root, "prime.txt", "p")
            dm.install_into_tree(
                source=priming_file,
                project="roadmap",
                campaign="2025_summer",
                experiment="NR1_0",
                category="analysis",
            )

            # Create an existing subdataset at dest_rel ("run_001")
            target = cat / "run_001"
            dl.create(path=str(target), dataset=str(cat), cfg_proc="text2git")
            # Save registration in category superdataset
            dl.save(dataset=str(cat), message="register run_001 subdataset")

            # Now install a file into that existing subdataset
            src = self._mk_temp_file(root, "result.csv", "x,y\n1,2\n")
            dm.install_into_tree(
                source=src,
                project="roadmap",
                campaign="2025_summer",
                experiment="NR1_0",
                category="analysis",
                dest_rel="run_001",   # <- must already exist
            )

            # Assert: file is in the target subdataset
            self.assertTrue((target / "result.csv").exists(), "file not placed into the dest_rel dataset")

            # Metadata added to the target dataset, includes filename
            self.assertTrue(self._has_meta(target, node_type="dataset", name="run_001 (result.csv)"))

    def test_install_into_missing_target_raises(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            dm = DataManager(
                root,
                user_name="Frank Heinrich",
                user_email="fheinrich@cmu.edu",
                organization="CMU / NCNR",
                default_project="roadmap",
                datalad_profile="text2git",
            )
            dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

            # Build a source file
            src = self._mk_temp_file(root, "x.bin", "data")

            # Expect: dest_rel points to a non-existent dataset -> RuntimeError
            with self.assertRaises(RuntimeError):
                dm.install_into_tree(
                    source=src,
                    project="roadmap",
                    campaign="2025_summer",
                    experiment="NR1_0",
                    category="analysis",
                    dest_rel="missing_ds",   # <-- not created: should error by design
                )


if __name__ == "__main__":
    unittest.main()
