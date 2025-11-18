import datalad.api as dl

import os
import re
import requests
import subprocess
import tempfile
import unittest
import uuid

from datalad.distribution.dataset import Dataset
from pathlib import Path, PurePosixPath

from roadmap_datamanager.datamanager import DataManager
from roadmap_datamanager.helpers import set_git_annex_path
from roadmap_datamanager.metadata import Metadata

from typing import ClassVar
from urllib.parse import urlparse

# hard requirements check (do NOT silently skip)
ENV_ERRORS = []

# DataLad import
try:
    import datalad
except ImportError:
    ENV_ERRORS.append("datalad (Python package) not importable in this interpreter")


# git-annex available and recent enough
def annex_ok():
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


annex_err = annex_ok()
if annex_err:
    ENV_ERRORS.append(annex_err)
ENV_READY = not ENV_ERRORS


def create_tmp_dm_instance():
    """
    Creates a datamanager instance in a temporary directory
    :return: (the dm instance, (Path) the root directory of the instance)
    """
    root = Path(tempfile.mkdtemp())
    configdir = Path(tempfile.mkdtemp())
    os.environ["ROADMAP_DM_CONFIG"] = str(configdir / "dm.json")

    dm = DataManager(
        root,
        user_name="Frank Heinrich",
        user_email="fheinrich@cmu.edu",
        default_project="roadmap",
        datalad_profile="text2git",
    )
    return dm, root


def fresh_clone(gin_url: str) -> Path:
    """
    Clone the published root into a fresh temp dir and install subdatasets (repos only).
    :param gin_url: (str) URL of the gin repo
    :return: (Path) the root directory of the cloned repo
    """
    other_dir = Path(tempfile.mkdtemp()) / "clone"
    other_dir.mkdir(parents=True, exist_ok=True)
    dl.clone(source=gin_url, path=str(other_dir))
    dl.get(dataset=str(other_dir), path=str(other_dir), recursive=True, get_data=False)
    return other_dir


def has_meta(ds: Path, *, rel_path: Path, node_type: str) -> bool:
    dds = Dataset(ds)
    dataset_id = dds.id

    # POSIX-normalized relative path string, '' for dataset itself
    relposix = '.' if rel_path == Path() else str(PurePosixPath(*rel_path.parts))
    # Empty relpath identifies the dataset itself
    if relposix != '.':
        node_id = f"datalad:{node_type}{dataset_id}:{relposix}"
    else:
        node_id = f"datalad:{node_type}{dataset_id}"

    try:
        # If you want to narrow by a file/folder, pass path (non-empty).
        # For dataset-level records (path = None, or path='.'), query without a path filter.
        meta = Metadata(ds_root=ds, path=relposix)
        records = meta.get(mode='envelope')  # list of envelope dicts
        print("Retrieved records:", records)
    except ValueError:
        return False

    if (
            records.get("extractor_name") == "datamanager_v1"
            and records.get("extracted_metadata", {}).get("@id") == node_id
            and records.get("extracted_metadata", {}).get("identifier") == relposix
    ):
        return True
    return False


def mk_temp_file(parent: Path, name: str, content: str = "x") -> Path:
    parent.mkdir(parents=True, exist_ok=True)
    p = parent / name
    p.write_text(content)
    return p


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
        dm, root = create_tmp_dm_instance()

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
        self.assertTrue(has_meta(up, rel_path=Path(), node_type='user'))
        self.assertTrue(has_meta(pp, rel_path=Path(), node_type='project'))
        self.assertTrue(has_meta(cp, rel_path=Path(), node_type='campaign'))
        self.assertTrue(has_meta(ep, rel_path=Path(), node_type='experiment'))


@unittest.skipUnless(ENV_READY, "Environment check failed; see TestEnvironment.test_000_requirements_present")
class DataManagerInstallIntoTreeTest(unittest.TestCase):
    def test_install_file_into_category_root(self):
        # Setup
        dm, root = create_tmp_dm_instance()
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

        up = root
        ep = up / "roadmap" / "2025_summer" / "NR1_0"
        cat = ep / "raw"

        # Source file
        src = mk_temp_file(root, "sample_raw.dat", "abc123")

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

        # Category is a dataset (not anymore)
        # self.assertTrue((cat / ".datalad").exists(), "category is expected to be a dataset")

        # Metadata written at experiment level, including filename in the name field
        dest = cat / src.name
        self.assertTrue(has_meta(ep, rel_path=dest.relative_to(ep), node_type="experiment"))

    def test_install_folder_recursively_as_subfolders(self):
        dm, root = create_tmp_dm_instance()
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
            rename="bundleA",
        )

        top = cat / "bundleA"
        subA = top / "subA"
        deep = top / "subB" / "deep"

        # Each directory replicated as a dataset (subdatasets) (not anymore)
        # self.assertTrue((top / ".datalad").exists(), "top folder should be a dataset")
        # self.assertTrue((subA / ".datalad").exists(), "subA should be a dataset")
        # self.assertTrue((deep / ".datalad").exists(), "deep should be a dataset")

        # Files copied into their respective datasets
        self.assertTrue((subA / "a.txt").exists())
        self.assertTrue((deep / "b.txt").exists())

        # Metadata created on the top dataset for the folder
        self.assertTrue(has_meta(ep, rel_path=Path("analysis/bundleA"), node_type="experiment"))

    def test_install_into_existing_subdataset_with_dest_rel_file(self):
        dm, root = create_tmp_dm_instance()
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

        ep = root / "roadmap" / "2025_summer" / "NR1_0"
        cat = ep / "analysis"

        # Ensure category dataset exists (e.g., by a no-op install of a tiny file)
        priming_file = mk_temp_file(root, "prime.txt", "p")
        dm.install_into_tree(
            source=priming_file,
            project="roadmap",
            campaign="2025_summer",
            experiment="NR1_0",
            category="analysis",
        )

        # Subfolder at dest_rel ("run_001")
        target = cat / "run_001"
        target.mkdir()

        # Now install a file into that existing subdataset
        src = mk_temp_file(root, "result.csv", "x,y\n1,2\n")
        dm.install_into_tree(
            source=src,
            project="roadmap",
            campaign="2025_summer",
            experiment="NR1_0",
            category="analysis",
            dest_rel="run_001",  # <- the subfolder
        )

        # Assert: file is in the target subdataset
        self.assertTrue((target / "result.csv").exists(), "file not placed into the dest_rel dataset")

        # Metadata added to the target dataset, includes filename
        self.assertTrue(has_meta(ep, rel_path=Path("analysis/run_001/result.csv"), node_type="experiment"))

    def test_install_into_missing_target_raises(self):
        dm, root = create_tmp_dm_instance()
        dm.init_tree(project="roadmap", campaign="2025_summer", experiment="NR1_0")

        # Build a source file at root
        src = mk_temp_file(root, "x.bin", "data")

        # Expect: dest_rel points to a non-existent dataset -> Should be created
        dm.install_into_tree(
            source=src,
            project="roadmap",
            campaign="2025_summer",
            experiment="NR1_0",
            category="analysis",
            dest_rel="missing_dir",   # <-- not created: should not error but be created
        )
        self.assertTrue((root / "roadmap" / "2025_summer" / "NR1_0" / "analysis" / "missing_dir" / "x.bin").exists(),
                        "file not placed into the dest_rel dataset")


# --- GIN test gating / config ---
GIN_TEST = os.getenv("SCIDATA_TEST_GIN", "1") == "1"
GIN_NAMESPACE = os.getenv("GIN_NAMESPACE", "fhein")  # e.g. your GIN username or org
GIN_ACCESS = os.getenv("GIN_ACCESS", "https-ssh")    # "ssh" (recommended) or "https"
# CRED = os.getenv("SCIDATA_GIN_CRED")               # only if using https with a stored credential


@unittest.skipUnless(GIN_TEST, "GIN test disabled (set SCIDATA_TEST_GIN=1 to enable)")
class DataManagerPublishGINSiblingTest(unittest.TestCase):
    work = ClassVar[Path]
    root = ClassVar[Path]
    dm = ClassVar[DataManager]

    def _ensure_published(self):
        """
        Publish the tree to a unique GIN repo and return the HTTPS clone URL.
        :return:
        """
        self.dm.publish_gin_sibling(
            sibling_name="gin",
            repo_name=self.repo_name,
            access_protocol=GIN_ACCESS,  # e.g., "https-ssh"
            credential=None,
            private=False,
            recursive=True,
        )
        sibs = dl.siblings(dataset=str(self.root), action="query", return_type="list")
        gin_urls = [s.get("url") for s in sibs if s.get("name") == "gin" and s.get("url")]
        self.assertTrue(gin_urls, "Could not determine GIN clone URL from siblings()")
        # Prefer HTTPS if both exist
        gin_urls.sort(key=lambda u: (not u.startswith("http"), u))
        return gin_urls[0]

    def _gin_clone_url(self) -> str:
        sibs = dl.siblings(dataset=str(self.root), action="query", return_type="list")
        # prefer HTTPS URL for easy parsing
        urls = [s.get("url") for s in sibs if s.get("name") == "gin" and s.get("url")]
        urls.sort(key=lambda u: (not u.startswith("http"), u))
        return urls[0]

    @classmethod
    def setUpClass(cls):
        # Local dataset with one subdataset so recursion is exercised
        cls.dm, cls.root = create_tmp_dm_instance()
        cls.dm.init_tree(project="p", campaign="c", experiment="e")

        # Add a subdataset under the experiment
        sub = cls.root / "p" / "c" / "e" / "analysis"
        dl.create(path=str(sub), dataset=str(cls.root / "p" / "c" / "e"))
        dl.save(dataset=str(cls.root), recursive=True, message="add analysis subdataset")

        # Make sure there is at least one commit to push everywhere
        (cls.root / "README.md").write_text("root readme\n")
        dl.save(dataset=str(cls.root), path=[str(cls.root / "README.md")], message="seed root")

        (sub / "note.bin").write_bytes(b"\x00\x01")
        dl.save(dataset=str(sub), path=[str(sub / "note.bin")], message="seed subdataset")

        cls.repo_name = f"scidata-{uuid.uuid4().hex}"

    def test_01_publish_sibling_to_gin(self):
        # Wire to GIN (create or reconfigure), recursively
        self.dm.publish_gin_sibling(
            sibling_name="gin",
            repo_name=self.repo_name,
            access_protocol=GIN_ACCESS,
            credential=None,
            private=False,
            recursive=True,
        )

        # Sibling exists on root
        root_sibs = dl.siblings(dataset=str(self.root), action="query", return_type="list")
        self.assertTrue(any(s.get("name") == "gin" for s in root_sibs), "root missing 'gin' sibling")

        # Sibling exists on subdataset
        sub = self.root / "p" / "c" / "e" / "analysis"
        sub_sibs = dl.siblings(dataset=str(sub), action="query", return_type="list")
        self.assertTrue(any(s.get("name") == "gin" for s in sub_sibs), "subdataset missing 'gin' sibling")

        # Try a lightweight publish to ensure remote usability (Git + annex content)
        #    (publish_gin_sibling already pushes, but we do a small follow-up change to verify)
        (self.root / "TOUCH.txt").write_text("tick\n")
        dl.save(dataset=str(self.root), path=[str(self.root / "TOUCH.txt")], recursive=True, message="touch")
        dl.push(dataset=str(self.root), to="gin", recursive=True, data="anything")

        # Optional integrity check: drop local content for annexed file and get it back from GIN
        #    This proves annex on GIN is actually serving content.
        dl.drop(dataset=str(sub), path=[str(sub / "note.bin")], what="filecontent", reckless="availability")
        self.assertFalse(dl.Dataset(str(sub)).repo.file_has_content("note.bin"))
        dl.get(dataset=str(sub), path=[str(sub / "note.bin")])  # fetches from 'gin' if needed
        self.assertTrue((sub / "note.bin").exists() and (sub / "note.bin").stat().st_size == 2)

    def test_02_push_to_gin(self):
        gin_url = self._ensure_published()

        # Make a root change and a subdataset annex change
        (self.root / "CHANGES.md").write_text("root change\n")
        sub = self.root / "p" / "c" / "e" / "analysis"
        (sub / "new.bin").write_bytes(b"\xAA\xBB\xCC")
        dl.save(dataset=str(self.root), recursive=True, message="prepare push_to_remotes test")

        # Use DataManager API
        self.dm.push_to_remotes(dataset=str(self.root), recursive=True, message="dm push_to_remotes")

        # Verify by cloning fresh and checking both commits and annex content
        other = fresh_clone(gin_url)
        self.assertTrue((other / "CHANGES.md").exists(), "Root commit did not reach GIN")
        # Annex file exists as pointer initially; fetch bytes:
        dl.get(dataset=str(other / "p" / "c" / "e" / "analysis"),
               path=[str(other / "p" / "c" / "e" / "analysis" / "new.bin")])
        self.assertEqual((other / "p" / "c" / "e" / "analysis" / "new.bin").stat().st_size, 3)

    def test_03_pull_from_gin(self):
        gin_url = self._ensure_published()
        # Second working copy simulates another computer
        other = fresh_clone(gin_url)
        dm_other = DataManager(other, user_name="Frank Heinrich", user_email="fheinrich@cmu.edu")

        # Change on original and push
        (self.root / "NOTE.txt").write_text("note v1\n")
        dl.save(dataset=str(self.root), path=[str(self.root / "NOTE.txt")], message="v1")
        self.dm.push_to_remotes(dataset=str(self.root), recursive=True, message="push v1")

        # Pull into the other clone
        dm_other.pull_from_remotes(dataset=str(other), recursive=True)
        self.assertTrue((other / "NOTE.txt").exists(), "Pull did not bring down new file")

        # Update again and verify second pull
        (self.root / "NOTE.txt").write_text("note v2\n")
        dl.save(dataset=str(self.root), path=[str(self.root / "NOTE.txt")], message="v2")
        self.dm.push_to_remotes(dataset=str(self.root), recursive=True, message="push v2")

        dm_other.pull_from_remotes(dataset=str(other), recursive=True)
        self.assertEqual((other / "NOTE.txt").read_text(), "note v2\n", "Pull did not merge latest changes")

    def test_04_get_data(self):
        gin_url = self._ensure_published()
        other = fresh_clone(gin_url)
        dm_other = DataManager(other, user_name="Frank Heinrich", user_email="fheinrich@cmu.edu")

        sub_other = other / "p" / "c" / "e" / "analysis"
        target = sub_other / "note.bin"

        # Ensure local content is absent
        dl.drop(dataset=str(sub_other), path=[str(target)], what="filecontent", reckless="availability")
        self.assertFalse(dl.Dataset(str(sub_other)).repo.file_has_content("note.bin"))

        # Fetch bytes using DataManager API
        dm_other.get_data(dataset=str(sub_other), path=str(target), recursive=False)
        self.assertTrue(dl.Dataset(str(sub_other)).repo.file_has_content("note.bin"))
        self.assertEqual(target.stat().st_size, 2)

    def test_05_drop_local(self):
        gin_url = self._ensure_published()
        other = fresh_clone(gin_url)
        dm_other = DataManager(other, user_name="Frank Heinrich", user_email="fheinrich@cmu.edu")

        sub_other = other / "p" / "c" / "e" / "analysis"
        target = sub_other / "note.bin"

        # Ensure we have bytes locally first
        dl.get(dataset=str(sub_other), path=[str(target)])
        self.assertTrue(dl.Dataset(str(sub_other)).repo.file_has_content("note.bin"))

        # Drop via DataManager API
        dm_other.drop_local(dataset=str(sub_other), path=target, recursive=False)
        self.assertFalse(dl.Dataset(str(sub_other)).repo.file_has_content("note.bin"))

    def test_06_remove_gin_repository(self):
        def _parse_owner_repo_from_url(url: str) -> tuple[str, str]:
            # works for https://gin.g-node.org/owner/repo(.git) and ssh git@gin.g-node.org:owner/repo(.git)
            if url.startswith("git@"):
                # git@gin.g-node.org:owner/repo.git
                path = url.split(":", 1)[1]
            else:
                path = urlparse(url).path.lstrip("/")
            path = re.sub(r"\.git$", "", path)
            owner, repo = path.split("/", 1)
            return owner, repo

        def delete_gin_repo(gin_url: str) -> tuple[bool, str]:
            """DELETE the repo via Gitea API; returns (ok, message)."""
            base = os.getenv("GIN_BASE", "https://gin.g-node.org").rstrip("/")
            token = os.getenv("GIN_TOKEN")
            if not token:
                return False, "GIN_TOKEN not set; cannot delete GIN repo automatically."

            owner_env = os.getenv("GIN_OWNER")
            try:
                owner, repo = _parse_owner_repo_from_url(gin_url)
            except RuntimeError:
                if owner_env:
                    # fallback if parsing failed
                    _, repo = "", ""
                    # try last path segment as repo
                    repo = gin_url.rsplit("/", 1)[-1].removesuffix(".git")
                    owner = owner_env
                else:
                    return False, f"Cannot parse owner/repo from {gin_url} and GIN_OWNER not set."

            api = f"{base}/api/v1/repos/{owner}/{repo}"
            r = requests.delete(api, headers={"Authorization": f"token {token}"}, timeout=30)
            if r.status_code in (200, 202, 204):
                return True, f"Deleted {owner}/{repo}."
            return False, f"Delete failed ({r.status_code}): {r.text}"

        # If we created a GIN repo, try to remove it
        try:
            gin_url = self._gin_clone_url()
        except RuntimeError:
            return  # no sibling, nothing to delete
        ok, msg = delete_gin_repo(gin_url)
        # It’s OK if deletion fails in CI; just log it, so you can fix creds
        print("[GIN CLEANUP]", msg)
