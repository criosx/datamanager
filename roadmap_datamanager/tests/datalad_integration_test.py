import datalad.api as dl

import json
import os
import subprocess
import tempfile
import unittest
import uuid

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

SSH_HOST_SHELL = os.getenv("SCIDATA_TEST_SSH_HOST_SHELL", "bluehost-full")
SSH_HOST_GIT = os.getenv("SCIDATA_TEST_SSH_HOST_GIT",   "bluehost-data")
GIT_BASE = os.getenv("SCIDATA_TEST_GIT_BASE", "/home2/frankhei/gittest")
WHITELIST = os.getenv("SCIDATA_TEST_WHITELIST", "/home2/frankhei/gittest")


def _ssh_ok(host):  # shell or git
    try:
        subprocess.run(["ssh","-o","BatchMode=yes","-o","ConnectTimeout=6",host,"exit 0"],
                       check=True, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except Exception:
        return False


def _ssh(host_cmd: str) -> None:
    subprocess.run(["ssh", SSH_HOST_SHELL, host_cmd], check=True, text=True)

def _ensure_remote_base():
    _ssh(f"mkdir -p {GIT_BASE}")


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
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

        # Build a source file at root
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


@unittest.skipUnless(_ssh_ok(SSH_HOST_SHELL), "Shell SSH host not reachable")
class DataManagerResetSiblingTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _ssh_ok(SSH_HOST_SHELL):
            raise unittest.SkipTest("Shell SSH host not reachable")
        _ensure_remote_base()

    def setUp(self):
        # local dataset with one subdataset so we exercise recursion
        self.work = Path(tempfile.mkdtemp())
        self.root = self.work / "scidata"
        self.dm = DataManager(
            self.root,
            user_name="Frank Heinrich",
            user_email="fheinrich@cmu.edu",
            datalad_profile="text2git",
        )
        self.dm.init_tree(project="p", campaign="c", experiment="e")
        sub = self.root / "p" / "c" / "e" / "analysis"
        dl.create(path=str(sub), dataset=str(self.root / "p" / "c" / "e"), cfg_proc="text2git")
        dl.save(dataset=str(self.root), recursive=True, message="add analysis subdataset")

        # per-test unique bare path on the real server
        self.remote_path = f"{GIT_BASE}/scidata-{uuid.uuid4().hex}.git"

    def _assert_bare_and_has_refs(self, remote_abs_path: str):
        # Check bare via remote git (exec on server)
        _ssh(f"bash -lc 'git -C {remote_abs_path} rev-parse --is-bare-repository'")
        # Must have at least one head after push
        _ssh(f"bash -lc 'test -d {remote_abs_path}/refs/heads && "
             f"ls -1 {remote_abs_path}/refs/heads | grep -q .'" )

    def test_happy_path(self):
        self.dm.reset_git_sibling(
            name="origin",
            ssh_host=SSH_HOST_SHELL,
            remote_abs_path=self.remote_path,
            recursive=True, nuke_remote=True, force=True,
            whitelist_root=WHITELIST,
        )
        # assert bare & refs exist on server
        subprocess.run(["ssh", SSH_HOST_SHELL, f"bash -lc 'git -C {self.remote_path} rev-parse --is-bare-repository'"],
                       check=True, text=True)
        subprocess.run(["ssh", SSH_HOST_SHELL, f"bash -lc 'ls -1 {self.remote_path}/refs/heads | grep -q .'"],
                       check=True, text=True)

    def test_nonempty_requires_nuke(self):
        # Pre-create and make non-empty
        _ssh(f"mkdir -p {self.remote_path} && touch {self.remote_path}/SOMETHING")
        with self.assertRaises(RuntimeError):
            self.dm.reset_git_sibling(
                name="origin",
                ssh_host=SSH_HOST_SHELL,
                remote_abs_path=self.remote_path,
                recursive=True,
                nuke_remote=False,
                force=True,
                whitelist_root=WHITELIST,
            )

    def test_refuses_without_force(self):
        with self.assertRaises(RuntimeError):
            self.dm.reset_git_sibling(
                name="origin",
                ssh_host=SSH_HOST_SHELL,
                remote_abs_path=self.remote_path,
                recursive=True,
                nuke_remote=True,
                force=False,
                whitelist_root=WHITELIST,
            )

    def test_refuses_outside_whitelist(self):
        with self.assertRaises(RuntimeError):
            self.dm.reset_git_sibling(
                name="origin",
                ssh_host=SSH_HOST_SHELL,
                remote_abs_path=f"/home2/frankhei/NOT-ALLOWED/scidata.git",
                recursive=True,
                nuke_remote=True,
                force=True,
                whitelist_root=WHITELIST,
            )