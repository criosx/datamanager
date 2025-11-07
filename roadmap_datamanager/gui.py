from __future__ import annotations

import json
import sys

from pathlib import Path

from PySide6.QtCore import Qt, QThreadPool, QObject, Signal, QRunnable, QDir
from PySide6.QtGui import QStandardItemModel, QStandardItem, QAction, QColor
from PySide6.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QDockWidget, QFileDialog, QFileSystemModel, QFormLayout, QHBoxLayout,
    QInputDialog, QLabel, QLineEdit, QListWidget, QListWidgetItem,
    QMainWindow, QMessageBox, QPlainTextEdit, QPushButton, QTreeView, QSplitter, QStatusBar, QToolBar,
    QVBoxLayout, QWidget
)

# import your datamanager
from roadmap_datamanager.datamanager import DataManager, ALLOWED_CATEGORIES


class WorkerSignals(QObject):
    done = Signal(object)
    error = Signal(str)
    progress = Signal(str)


class FirstRunDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Initial DataManager setup")
        layout = QFormLayout(self)
        self.name_edit = QLineEdit(self)
        self.email_edit = QLineEdit(self)
        layout.addRow("User name:", self.name_edit)
        layout.addRow("User email:", self.email_edit)
        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def get_values(self):
        return self.name_edit.text().strip(), self.email_edit.text().strip()


class Worker(QRunnable):
    def __init__(self, fn, *args, **kwargs):
        super().__init__()
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.signals = WorkerSignals()

    def run(self):
        try:
            out = self.fn(*self.args, **self.kwargs)
            self.signals.done.emit(out)
        except Exception as e:
            self.signals.error.emit(str(e))


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DataManager GUI")
        self.pool = QThreadPool.globalInstance()
        self.status = QStatusBar()
        self.setStatusBar(self.status)

        self.dm: DataManager | None = None  # will be set below
        self.dm_current_path: Path | None = None  # which node we are currently viewing

        self._create_menubar()
        self._create_split_view()

        # bootstrap DM
        self.bootstrap_datamanager()

    def _choose_browser_root(self):
        path = QFileDialog.getExistingDirectory(self, "Select folder to browse")
        if path:
            self.fs_tree.setRootIndex(self.fs_model.index(path))

    @staticmethod
    def _classify_dm_entry(path: Path) -> str:
        """
        Return one of:
          - "dataset"         (directory that looks like a DataLad/Git dataset)
          - "folder"          (directory, but not a dataset)
          - "file-local"      (regular file OR symlink whose target exists)
          - "file-remote"     (symlink whose target does NOT exist — typical dropped annex content)
          - "other"
        """
        if path.is_dir():
            # dataset?
            if (path / ".datalad").exists() or (path / ".git").exists():
                return "dataset"
            return "folder"

        # files / symlinks
        if path.is_symlink():
            # for annexed content: symlink may point to non-existing target (dropped)
            target = path.resolve(strict=False)
            if target.exists():
                return "file-local"
            else:
                return "file-remote"

        if path.is_file():
            return "file-local"

        return "other"

    def _create_menubar(self):
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("&File")

        # Select root action
        act_select_root = QAction("Select datamanager root…", self)
        act_select_root.triggered.connect(self.select_root)
        file_menu.addAction(act_select_root)

        # Add later:
        # file_menu.addSeparator()
        # file_menu.addAction("Exit", self.close)

    def _create_split_view(self):
        splitter = QSplitter()
        splitter.setOrientation(Qt.Horizontal)

        # ----- Left: filesystem browser -----
        self.fs_panel = QWidget()
        fs_layout = QVBoxLayout(self.fs_panel)
        fs_layout.setContentsMargins(4, 4, 4, 4)

        tb = QToolBar()
        act_home = QAction("Home", self)
        act_home.triggered.connect(self._go_home)
        act_choose = QAction("Choose folder…", self)
        act_choose.triggered.connect(self._choose_browser_root)
        act_install = QAction("Install into DM", self)
        act_install.triggered.connect(self.install_selected_sources_into_dm)
        tb.addAction(act_home)
        tb.addAction(act_choose)
        tb.addAction(act_install)
        fs_layout.addWidget(tb)

        self.fs_model = QFileSystemModel()
        self.fs_model.setRootPath(QDir.homePath())
        self.fs_model.setReadOnly(True)

        self.fs_tree = QTreeView()
        self.fs_tree.setModel(self.fs_model)
        self.fs_tree.setRootIndex(self.fs_model.index(QDir.homePath()))
        self.fs_tree.setSelectionMode(QTreeView.ExtendedSelection)
        self.fs_tree.setSortingEnabled(True)
        self.fs_tree.sortByColumn(0, Qt.AscendingOrder)
        self.fs_tree.setColumnWidth(0, 280)
        fs_layout.addWidget(self.fs_tree, 1)

        splitter.addWidget(self.fs_panel)

        # ----- Center: datamanager panel -----
        self.dm_panel = QWidget()
        dm_layout = QVBoxLayout(self.dm_panel)
        dm_layout.setContentsMargins(4, 4, 4, 4)

        # top bar: show level + name
        top_bar = QVBoxLayout()
        self.lbl_root = QLabel("Root Dir: —")
        self.lbl_project = QLabel("Project: —")
        self.lbl_campaign = QLabel("Campaign: —")
        self.lbl_experiment = QLabel("Experiment: —")
        self.lbl_category = QLabel("Category: —")
        top_bar.addWidget(self.lbl_root)
        top_bar.addWidget(self.lbl_project)
        top_bar.addWidget(self.lbl_campaign)
        top_bar.addWidget(self.lbl_experiment)
        top_bar.addWidget(self.lbl_category)
        top_bar.addStretch(1)
        dm_layout.addLayout(top_bar)

        # nav buttons
        nav_bar = QHBoxLayout()
        self.btn_up = QPushButton("↑ Up")
        self.btn_open = QPushButton("Open")
        self.btn_new_dataset = QPushButton("New dataset here…")
        self.btn_show_meta = QPushButton("Show metadata")
        self.btn_up.clicked.connect(self.dm_go_up)
        self.btn_open.clicked.connect(self.dm_open_selected)
        self.btn_new_dataset.clicked.connect(self.dm_create_dataset_here)
        self.btn_show_meta.clicked.connect(self.show_selected_metadata)
        nav_bar.addWidget(self.btn_up)
        nav_bar.addWidget(self.btn_open)
        nav_bar.addWidget(self.btn_new_dataset)
        nav_bar.addWidget(self.btn_show_meta)
        dm_layout.addLayout(nav_bar)

        # list of children at current level
        self.dm_list = QListWidget()
        self.dm_list.itemActivated.connect(self._dm_open_item)
        self.dm_list.setEditTriggers(QListWidget.NoEditTriggers)
        self.dm_list.setSelectionMode(QListWidget.SingleSelection)
        dm_layout.addWidget(self.dm_list, 1)

        splitter.addWidget(self.dm_panel)

        # ----- RIGHT: Metadata viewer (NEW) -----
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel)
        meta_layout.setContentsMargins(4, 4, 4, 4)
        self.meta_title = QLabel("Metadata: —")
        self.meta_view = QPlainTextEdit()
        self.meta_view.setReadOnly(True)
        meta_layout.addWidget(self.meta_title)
        meta_layout.addWidget(self.meta_view, 1)
        splitter.addWidget(self.meta_panel)

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        self.setCentralWidget(splitter)

    def _dm_current_level(self):
        """
        Returns (level, parts)
        level ∈ {"root", "project", "campaign", "experiment", "below-experiment"}
        parts = path parts after dm.root
        """
        pcec = ["Not set.", "", "", "", ""]
        level = "root"
        if self.dm is None or self.dm_current_path is None:
            return level, pcec

        root = self.dm.root
        cur = self.dm_current_path
        rel = cur.relative_to(root)
        pcec[0] = str(root)
        if str(rel) == ".":
            return level, pcec

        level_list = ["project", "campaign", "experiment", "category"]
        for i, part in enumerate(list(rel.parts)):
            if i > 3:
                # folders in categories are not reported here
                break
            pcec[i+1] = part
            level = level_list[i]
        return level, pcec

    def _dm_open_item(self, item: QListWidgetItem):
        path_str = item.data(Qt.UserRole)
        if not path_str:
            return
        p = Path(path_str)
        if p.is_dir():
            self.dm_current_path = p
            self.refresh_dm_panel()
        # else: it's a file/symlink — ignore or handle later (open in Finder, fetch annex, etc.)

    def _find_dataset_root_and_rel(self, path: Path) -> tuple[Path | None, Path | None]:
        """
        Walk up from `path` until we find a directory containing a dataset.
        Returns (ds_root, relpath_within_dataset) or (None, None) if not found.
        If `path` *is* the dataset root, relpath is Path('.').
        """
        p = path
        if p.is_file() or p.is_symlink():
            parent = p.parent
        else:
            parent = p

        # climb up until DM root
        dm_root = self.dm.root if self.dm else None
        while True:
            if p.is_dir() and self._is_dataset_dir(p):
                ds_root = p
                # rel path is relative to ds_root; for the dataset itself, use '.'
                rel = Path(".") if path == ds_root else path.relative_to(ds_root)
                return ds_root, rel
            if dm_root and (p == dm_root or p == dm_root.parent):
                break
            if p.parent == p:
                break
            p = p.parent
        return None, None

    def _go_home(self):
        home = QDir.homePath()
        self.fs_tree.setRootIndex(self.fs_model.index(home))

    @staticmethod
    def _is_dataset_dir(p: Path) -> bool:
        return (p / ".datalad").exists() or (p / ".git").exists()

    def bootstrap_datamanager(self):
        """
        Try to loas a persistent config at GUI startup
        :return: no return value
        """
        try:
            dm = DataManager.from_persisted()
        except FileNotFoundError:
            # no persistent configuration yet — ask for root
            self.status.showMessage("No persisted datamanager yet — please select a root.")
            self.select_root(first_time=True)
            return
        else:
            self.set_datamanager(dm)

    def dm_go_up(self):
        if self.dm is None or self.dm_current_path is None:
            return
        if self.dm_current_path == self.dm.root:
            return
        self.dm_current_path = self.dm_current_path.parent
        self.refresh_dm_panel()

    def dm_open_selected(self):
        item = self.dm_list.currentItem()
        if not item:
            return
        path_str = item.data(Qt.UserRole)
        if not path_str:
            return
        p = Path(path_str)
        if p.is_dir():
            self.dm_current_path = p
            self.refresh_dm_panel()

    def dm_create_dataset_here(self):
        """
        Create exactly one dataset at the current level, using DataManager.init_tree().
        Allowed:
          - at root: create a project
          - at project: create a campaign
          - at campaign: create an experiment
        Disabled:
          - at experiment or deeper
        """
        if self.dm is None or self.dm_current_path is None:
            return

        level, parts = self._dm_current_level()

        # we don't create datasets below experiment in this GUI
        if level in ("experiment", "category"):
            QMessageBox.information(
                self,
                "Not allowed here",
                "Datasets can only be created at root, project, or campaign level.\n"
                "At the experiment level and below, please install files/folders instead."
            )
            return

        # ask for the name
        if level == "root":
            title = "New project"
            label = "Project name:"
        elif level == "project":
            title = "New campaign"
            label = f"Campaign name for project “{parts[1]}”:"
        else:  # level == "campaign"
            title = "New experiment"
            label = f"Experiment name for {parts[1]} / {parts[2]}:"

        name, ok = QInputDialog.getText(self, title, label)
        if not ok or not name.strip():
            return
        name = name.strip()

        # call datamanager
        if level == "root":
            # create project
            self.dm.init_tree(project=name)
            # current view is root -> refresh
        elif level == "project":
            project = parts[1]
            self.dm.init_tree(project=project, campaign=name)
        elif level == "campaign":
            project, campaign = parts[1], parts[2]
            self.dm.init_tree(project=project, campaign=campaign, experiment=name)

        # after creating, refresh the panel so the new item shows up
        self.refresh_dm_panel()

    def install_selected_sources_into_dm(self):
        """
        Take the selected files/folders from the right file browser
        and install them into the *current* DM location on the left.
        """
        if self.dm is None or self.dm_current_path is None:
            QMessageBox.warning(self, "No datamanager", "Please select or create a datamanager first.")
            return

        # gather selected sources from the right file tree
        indexes = self.fs_tree.selectedIndexes()
        sources = []
        for idx in indexes:
            # column 0 only
            if idx.column() != 0:
                continue
            path = self.fs_model.filePath(idx)
            if path:
                sources.append(path)

        if not sources:
            QMessageBox.information(self, "No files selected",
                                    "Please select one or more files/folders in the right file browser first.")
            return

        # where are we in the DM?
        level, parts = self._dm_current_level()

        # install is only allowed at experiment and below
        if level in ("root", "project", "campaign"):
            QMessageBox.information(
                self,
                "Install not allowed here",
                "You can only install files at the experiment level or below.\n"
                "Please open a campaign → experiment → (optional) category first."
            )
            return

        # ----- CASE 1: we are EXACTLY at experiment: root / proj / camp / exp
        if level == "experiment":
            # parts = [root_dir, project, campaign, experiment]
            root_dir, project, campaign, experiment = parts

            # ask user for category
            category, ok = QInputDialog.getItem(
                self,
                "Select category",
                f"Install into experiment “{experiment}” under which category?",
                ALLOWED_CATEGORIES,
                0,
                False
            )
            if not ok:
                return

            for src in sources:
                try:
                    self.dm.install_into_tree(
                        source=src,
                        project=project,
                        campaign=campaign,
                        experiment=experiment,
                        category=category,
                        metadata={"installed_by_gui": True},
                    )
                except Exception as e:
                    QMessageBox.critical(self, "Install failed", f"Could not install {src}:\n{e}")
                    # continue with other files

            # refresh left panel to show new folders
            self.refresh_dm_panel()
            self.status.showMessage("Installed into experiment.")

            return

        # ----- CASE 2: we are BELOW experiment:
        # path looks like: root / proj / camp / exp / category / (maybe subdirs…)
        if level == "below-experiment":
            # we expect at least 4 parts: [project, campaign, experiment, category, ...]
            if len(parts) < 4:
                QMessageBox.critical(
                    self,
                    "Unexpected path",
                    "This path is below experiment but I cannot determine project/campaign/experiment/category."
                )
                return

            project = parts[0]
            campaign = parts[1]
            experiment = parts[2]
            category = parts[3]
            # anything deeper than category becomes dest_rel
            if len(parts) > 4:
                dest_rel = Path(*parts[4:])
            else:
                dest_rel = None

            for src in sources:
                try:
                    self.dm.install_into_tree(
                        source=src,
                        project=project,
                        campaign=campaign,
                        experiment=experiment,
                        category=category,
                        dest_rel=dest_rel,
                        metadata={"installed_by_gui": True},
                    )
                except Exception as e:
                    QMessageBox.critical(self, "Install failed", f"Could not install {src}:\n{e}")
                    # continue

            self.refresh_dm_panel()
            self.status.showMessage("Installed into subfolder of experiment.")

    def refresh_dm_panel(self):
        """
        Refresh the data manager panel.
        :return: no return value
        """

        level, parts = self._dm_current_level()
        root, project, campaign, experiment, category = parts
        root = root if len(root) <= 40 else "…" + root[-39:]
        self.lbl_root.setText(f"Root: {root}")
        self.lbl_project.setText(f'Project: {project}')
        self.lbl_campaign.setText(f"Campaign: {campaign}")
        self.lbl_experiment.setText(f'Experiment: {experiment}')
        self.lbl_category.setText(f'Category: {category}')

        if self.dm is None or self.dm_current_path is None:
            self.dm_list.clear()
            return

        # enable/disable “new dataset” by level
        self.btn_new_dataset.setEnabled(experiment == "")
        self.btn_up.setEnabled(project != "")

        # list children of current path
        # list children of current path
        self.dm_list.clear()
        for child in self.dm_current_path.iterdir():
            # still skip dot dirs/files
            if child.name.startswith("."):
                continue

            kind = self._classify_dm_entry(child)

            item = QListWidgetItem(child.name)
            item.setData(Qt.UserRole, str(child))

            # color-code
            if kind == "dataset":
                item.setForeground(QColor("#1f6feb"))  # blue-ish
                item.setToolTip("Dataset (DataLad/Git)")
            elif kind == "folder":
                item.setForeground(QColor("#237804"))  # green
                item.setToolTip("Folder")
            elif kind == "file-local":
                item.setForeground(QColor("#000000"))  # black
                item.setToolTip("Local file")
            elif kind == "file-remote":
                item.setForeground(QColor("#808080"))  # grey
                item.setToolTip("Remotely available (annex), content not present")
                # a little visual hint
                font = item.font()
                font.setItalic(True)
                item.setFont(font)
            else:
                item.setForeground(QColor("#555555"))
                item.setToolTip("Other")

            self.dm_list.addItem(item)

    def select_root(self, first_time: bool = False):
        """
        Select datamanager root to load
        :param first_time: (bool) GUI startup?
        :return: no return value
        """
        path = QFileDialog.getExistingDirectory(self, "Choose datamanager root")
        if not path:
            if first_time:
                QMessageBox.critical(self, "No root selected",
                                     "A datamanager root is required on first run.")
            return

        # IMPORTANT: we re-create the DataManager with the new root.
        # It will merge with persisted values (user_name, user_email, etc.)
        try:
            dm = DataManager(root=path)
        except RuntimeError:
            dlg = FirstRunDialog(self)
            if dlg.exec() == QDialog.Accepted:
                name, email = dlg.get_values()
                dm = DataManager(root=path, user_name=name, user_email=email)
            else:
                return
        self.set_datamanager(dm)

    def set_datamanager(self, dm: DataManager):
        self.dm = dm
        self.dm_current_path = dm.root  # start at root
        self.status.showMessage(
            f"Using datamanager at {self.dm.root} as {self.dm.cfg.user_name} <{self.dm.cfg.user_email}>"
        )
        self.refresh_dm_panel()

    import json

    def show_selected_metadata(self):
        """
        Fetch metadata via DataManager.load_meta() for the selected item in the DM list.
        If nothing is selected, try the current DM path.
        """
        if self.dm is None or self.dm_current_path is None:
            QMessageBox.information(self, "No datamanager", "Select or create a datamanager first.")
            return

        # target path: selected item in DM panel, else current DM dir
        item = self.dm_list.currentItem()
        target_path = Path(item.data(Qt.UserRole)) if item else self.dm_current_path

        ds_root, rel = self._find_dataset_root_and_rel(target_path)
        if ds_root is None:
            self.meta_title.setText("Metadata: —")
            self.meta_view.setPlainText("No enclosing dataset found for this selection.")
            return

        # Prepare args for load_meta()
        rel_arg = None if rel == Path(".") else rel.as_posix()

        try:
            payload = self.dm.load_meta(ds_path=ds_root, path=rel_arg, return_='envelope', raise_on_missing=False)
        except Exception as e:
            self.meta_title.setText(f"Metadata: {target_path.name}")
            self.meta_view.setPlainText(f"Error while reading metadata:\n{e}")
            return

        title_name = target_path.name if rel_arg else f"{ds_root.name} (dataset)"
        self.meta_title.setText(f"Metadata: {title_name}")

        if not payload:
            self.meta_view.setPlainText("No metadata found for this selection.")
            return

        # pretty-print JSON-LD
        try:
            self.meta_view.setPlainText(json.dumps(payload, indent=2, ensure_ascii=False))
        except Exception:
            self.meta_view.setPlainText(str(payload))

    def sync_with_gin(self, recursive=True):
        if self.dm is None:
            return
        # simple pull
        self.dm.pull_from_remotes(dataset=self.dm.root, recursive=recursive)
        self.status.showMessage("Pulled from GIN.")


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Force a stable, light theme regardless of OS setting
    app.setStyle("Fusion")  # consistent cross-platform widget style
    app.setPalette(app.style().standardPalette())  # the Fusion light palette

    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())
