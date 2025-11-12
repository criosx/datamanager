from __future__ import annotations

import json
import logging
import sys

from pathlib import Path

from PySide6.QtCore import Qt, QThreadPool, QObject, Signal, Slot, QRunnable, QDir
from PySide6.QtGui import QPalette, QAction, QColor
from PySide6.QtWidgets import (
    QAbstractItemView, QApplication, QComboBox, QDialog, QDialogButtonBox, QFileDialog, QFileSystemModel,
    QFormLayout, QHBoxLayout, QInputDialog, QLabel, QLineEdit, QListWidget, QListWidgetItem,
    QMainWindow, QMenu, QMessageBox, QPlainTextEdit, QPushButton, QTreeView, QSplitter, QStatusBar, QToolBar,
    QVBoxLayout, QWidget
)

# import datamanager
from roadmap_datamanager.datamanager import DataManager, ALLOWED_CATEGORIES

from remote import GinRemoteDialog

METADATA_MANUAL_ADD_ITEMS = [
    'condition',
    'description',
    'sample',
]


def create_light_palette():
    p = QPalette()
    p.setColor(QPalette.Window, QColor(240, 240, 240))
    p.setColor(QPalette.WindowText, Qt.black)
    p.setColor(QPalette.Base, QColor(255, 255, 255))
    p.setColor(QPalette.AlternateBase, QColor(240, 240, 240))
    p.setColor(QPalette.ToolTipBase, Qt.white)
    p.setColor(QPalette.ToolTipText, Qt.black)
    p.setColor(QPalette.Text, Qt.black)
    p.setColor(QPalette.Button, QColor(240, 240, 240))
    p.setColor(QPalette.ButtonText, Qt.black)
    p.setColor(QPalette.BrightText, Qt.red)
    p.setColor(QPalette.Link, QColor(42, 130, 218))
    p.setColor(QPalette.Highlight, QColor(42, 130, 218))
    p.setColor(QPalette.HighlightedText, Qt.white)

    # --- inactive buttons ---
    p.setColor(QPalette.Inactive, QPalette.Button, QColor(230, 230, 230))
    p.setColor(QPalette.Inactive, QPalette.ButtonText, QColor(80, 80, 80))

    # --- disabled buttons ---
    p.setColor(QPalette.Disabled, QPalette.Button, QColor(230, 230, 230))
    p.setColor(QPalette.Disabled, QPalette.ButtonText, QColor(150, 150, 150))
    return p


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


class GuiLogHandler(logging.Handler, QObject):
    # Same signal as EmittingStream
    textWritten = Signal(str)

    def __init__(self):
        super().__init__()
        QObject.__init__(self)

    def emit(self, record):
        msg = self.format(record)
        self.textWritten.emit(msg + '\n')


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


class WorkerSignals(QObject):
    done = Signal(object)
    error = Signal(str)
    progress = Signal(str)


class EmittingStream(QObject):
    textWritten = Signal(str)

    def write(self, text):
        self.textWritten.emit(str(text))
        # in addition, write to the original stdout to see console output
        sys.__stdout__.write(text)
        self.flush()    # Ensure immediate output

    def flush(self):
        # Required for file-like objects, but can be empty for this use case
        pass

    @staticmethod
    def isatty():
        # Returns False, as this is not a TTY device.
        return False


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

        # state for current metadata context
        self.meta_current_ds_root: Path | None = None
        self.meta_current_rel: str | None = None  # posix path or None for dataset
        self.meta_current_payload: dict | None = None  # extracted_metadata being edited

        # Redirect stdout
        self.stdout_redirect = EmittingStream()
        self.stdout_redirect.textWritten.connect(self.logviewer_append_text)
        sys.stdout = self.stdout_redirect
        sys.stderr = self.stdout_redirect

        # --- Redirect Logging to GUI ---
        log_handler = GuiLogHandler()
        log_handler.textWritten.connect(self.logviewer_append_text)
        # Set format (optional, matches standard log output)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)

        # Get the root logger and add our handler
        root_logger = logging.getLogger()
        root_logger.addHandler(log_handler)
        # Set minimum logging level to capture (e.g., INFO, WARNING, DEBUG)
        root_logger.setLevel(logging.INFO)

    def _choose_browser_root(self):
        path = QFileDialog.getExistingDirectory(self, "Select folder to browse")
        if path:
            self.fs_tree.setRootIndex(self.fs_model.index(path))

    def _create_menubar(self):
        menubar = self.menuBar()

        # File menu
        file_menu = menubar.addMenu("&File")

        act_select_root = QAction("Select datamanager root...", self)
        act_select_root.triggered.connect(self.select_root)
        file_menu.addAction(act_select_root)

        file_menu.addSeparator()
        act_close = QAction("&Close", self)
        act_close.triggered.connect(self.close)
        file_menu.addAction(act_close)

        remote_menu = menubar.addMenu("&Remote")

        act_select_remote = QAction("Select datamanager remote...", self)
        act_select_remote.triggered.connect(self.select_remote)
        remote_menu.addAction(act_select_remote)

        act_clone_from_GIN = QAction("Clone from GIN into empty", self)
        act_clone_from_GIN.triggered.connect(self.clone_from_gin)
        remote_menu.addAction(act_clone_from_GIN)



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
        act_install.triggered.connect(self.fileviewer_install_selected_sources_into_dm)
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
        self.btn_refresh = QPushButton("Refresh")
        self.btn_new_dataset = QPushButton("New dataset here…")
        self.btn_show_meta = QPushButton("Show metadata")
        self.btn_up.clicked.connect(self.dm_go_up)
        self.btn_refresh.clicked.connect(self.dm_refresh_panel)
        self.btn_new_dataset.clicked.connect(self.dm_create_dataset_here)
        self.btn_show_meta.clicked.connect(self.dm_show_selected_metadata)
        nav_bar.addWidget(self.btn_up)
        nav_bar.addWidget(self.btn_refresh)
        nav_bar.addWidget(self.btn_new_dataset)
        nav_bar.addWidget(self.btn_show_meta)
        dm_layout.addLayout(nav_bar)

        # list of children at current level
        self.dm_list = QListWidget()
        self.dm_list.itemActivated.connect(self._dm_open_item)
        self.dm_list.setEditTriggers(QListWidget.NoEditTriggers)
        self.dm_list.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.dm_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.dm_list.customContextMenuRequested.connect(self._dm_show_context_menu)
        dm_layout.addWidget(self.dm_list, 1)

        splitter.addWidget(self.dm_panel)

        # ----- RIGHT: Metadata viewer -----
        # viewer
        self.meta_panel = QWidget()
        meta_layout = QVBoxLayout(self.meta_panel)
        meta_layout.setContentsMargins(4, 4, 4, 4)
        self.meta_title = QLabel("Metadata: —")
        self.meta_view = QPlainTextEdit()
        self.meta_view.setReadOnly(True)
        meta_layout.addWidget(self.meta_title)
        meta_layout.addWidget(self.meta_view, 1)

        # editor row at bottom
        editor_row = QHBoxLayout()
        self.meta_key = QComboBox()
        self.meta_key.setEditable(True)
        self.meta_key.addItems(METADATA_MANUAL_ADD_ITEMS)
        self.meta_value = QLineEdit()
        self.meta_apply_btn = QPushButton("Add/Update")
        self.meta_save_btn = QPushButton("Save to dataset")
        self.meta_apply_btn.clicked.connect(self.apply_metadata_field)
        self.meta_save_btn.clicked.connect(self.metadata_save_changes)
        editor_row.addWidget(self.meta_key, 2)
        editor_row.addWidget(self.meta_value, 4)
        editor_row.addWidget(self.meta_apply_btn, 2)
        editor_row.addWidget(self.meta_save_btn, 2)

        meta_layout.addLayout(editor_row)
        splitter.addWidget(self.meta_panel)

        # Give center & right more space
        splitter.setStretchFactor(0, 2)  # FS
        splitter.setStretchFactor(1, 2)  # DM
        splitter.setStretchFactor(2, 2)  # Meta

        # Wrap splitter + log in a vertical layout
        container = QWidget()
        vbox = QVBoxLayout(container)
        vbox.setContentsMargins(0, 0, 0, 0)
        vbox.addWidget(splitter, 4)

        # ----- LOG PANEL -----
        self.log_view = QPlainTextEdit()
        self.log_view.setReadOnly(True)
        self.log_view.setMaximumHeight(160)
        self.log_view.setMaximumBlockCount(1000)
        self.log_view.setPlaceholderText("Log output (DataLad, DataManager, errors) will appear here...")
        vbox.addWidget(self.log_view, 1)

        self.setCentralWidget(container)

    @staticmethod
    def _dm_classify_entry(path: Path) -> str:
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

    def _dm_current_level(self):
        """
        Returns (level, parts)
        level ∈ {"root", "project", "campaign", "experiment", "category"}
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
            self.dm_refresh_panel()
        # else: it's a file/symlink — ignore or handle later (open in Finder, fetch annex, etc.)

    def _dm_show_context_menu(self, pos):
        items = self.dm_list.selectedItems()
        if not items:
            return

        menu = QMenu(self)
        act_open = menu.addAction("Open")
        remove_menu = QMenu("Remove", self)
        act_drop = remove_menu.addAction("Drop content (annex)")
        act_remove = remove_menu.addAction("Remove content (safely)")
        act_remove_reckless = remove_menu.addAction("Remove content (reckless)")
        menu.addMenu(remove_menu)

        action = menu.exec(self.dm_list.mapToGlobal(pos))
        if action == act_open:
            self.dm_open_selected()
        elif action == act_drop:
            self.dm_drop_selected()
        elif action == act_remove:
            self.dm_remove_selected()
        elif action == act_remove_reckless:
            self.dm_remove_selected(reckless=True)

    def _find_dataset_root_and_rel(self, path: Path) -> tuple[Path | None, Path | None]:
        """
        Walk up from `path` until we find a directory containing a dataset.
        Returns (ds_root, relpath_within_dataset) or (None, None) if not found.
        If `path` *is* the dataset root, relpath is Path('.').
        """

        if not path.exists():
            return None, None

        # if it's a file/symlink, start from parent when searching dataset root
        search_from = path if path.is_dir() else path.parent

        # climb up until DM root
        dm_root = self.dm.root if self.dm else None
        p = search_from
        while True:
            if p.is_dir() and self._is_dataset_dir(p):
                ds_root = p
                # rel path is relative to ds_root; for the dataset itself, use '.'
                rel = Path("../roadmap_datamanager") if path == ds_root else path.relative_to(ds_root)
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

    def _run_in_worker(self, fn, *args, **kwargs):
        worker = Worker(fn, *args, **kwargs)
        # pipe worker error to log
        worker.signals.error.connect(
            lambda msg: self.logviewer_append_text(f"[ERROR] {msg}\n")
        )
        self.pool.start(worker)

    def _selected_dm_paths(self) -> list[Path]:
        paths: list[Path] = []
        for item in self.dm_list.selectedItems():
            path_str = item.data(Qt.UserRole)
            if path_str:
                paths.append(Path(path_str))
        return paths

    @Slot(str)
    def apply_metadata_field(self):
        """
        Add or update a key in the in-memory metadata payload and refresh the viewer.
        Does NOT write to disk yet.
        """
        if self.meta_current_ds_root is None:
            QMessageBox.information(self, "No selection", "Select an item and click 'Show metadata' first.")
            return

        key = self.meta_key.currentText().strip()
        value = self.meta_value.text().strip()
        if not key:
            return

        if self.meta_current_payload is None:
            self.meta_current_payload = {}

        # simple flat update; nested keys can be handled later if needed
        self.meta_current_payload[key] = value

        # reflect changes in viewer immediately
        self.meta_view.setPlainText(json.dumps(self.meta_current_payload, indent=2, ensure_ascii=False))
        self.meta_value.clear()

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

    def clone_from_gin(self):
        """
        Clones the GIN superdataset into an empty datamanager directory
        :return: no return value
        """
        self._run_in_worker(
            self.dm.clone_from_gin,
            dest=self.dm_current_path,
            source_url=self.dm.cfg.GIN_url
        )

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
        self.dm_refresh_panel()

    def dm_drop_selected(self):
        if self.dm is None:
            return
        paths = self._selected_dm_paths()
        if not paths:
            return

        for p in paths:
            ds_root, rel = self._find_dataset_root_and_rel(p)
            if ds_root is None or rel is None:
                continue
            rel_str = "." if rel == Path("../roadmap_datamanager") else rel.as_posix()

            try:
                self._run_in_worker(
                    self.dm.drop_local,
                    dataset=str(ds_root),
                    path=rel_str,
                    what="filecontent",
                    recursive=False
                )
                self.logviewer_append_text(f"[INFO] Dropped content: {p}\n")
            except Exception as e:
                self.logviewer_append_text(f"[WARN] Could not drop {p}: {e}\n")

        self.dm_refresh_panel()

    def dm_go_up(self):
        if self.dm is None or self.dm_current_path is None:
            return
        if self.dm_current_path == self.dm.root:
            return
        self.dm_current_path = self.dm_current_path.parent
        self.dm_refresh_panel()

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
            self.dm_refresh_panel()

    def dm_refresh_panel(self):
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

            kind = self._dm_classify_entry(child)

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

    def dm_show_selected_metadata(self):
        """
        Fetch metadata via DataManager.load_meta() for the selected item in the DM list.
        If nothing is selected, try the current DM path.
        """
        if self.dm is None or self.dm_current_path is None:
            QMessageBox.information(self, "No datamanager", "Select or create a datamanager first.")
            return

        item = self.dm_list.currentItem()
        target_path = Path(item.data(Qt.UserRole)) if item else self.dm_current_path

        ds_root, rel = self._find_dataset_root_and_rel(target_path)
        if ds_root is None:
            self.meta_title.setText("Metadata: —")
            self.meta_view.setPlainText("No enclosing dataset found for this selection.")
            self.meta_current_ds_root = None
            self.meta_current_rel = None
            self.meta_current_payload = None
            return

        # Prepare args for load_meta()
        rel_arg = None if rel == Path("../roadmap_datamanager") else rel.as_posix()

        try:
            payload = self.dm.load_meta(ds_path=ds_root, path=rel_arg, return_='payload', raise_on_missing=False)
        except ValueError as e:
            self.meta_title.setText(f"Metadata: {target_path.name}")
            self.meta_view.setPlainText(f"Error while reading metadata:\n{e}")
            self.meta_current_ds_root = None
            self.meta_current_rel = None
            self.meta_current_payload = None
            return

        self.meta_current_ds_root = ds_root
        self.meta_current_rel = rel_arg
        # if nothing yet, start from empty dict to allow adding
        self.meta_current_payload = dict(payload) if payload else {}

        title_name = target_path.name if rel_arg else f"{ds_root.name} (dataset)"
        self.meta_title.setText(f"Metadata: {title_name}")

        if self.meta_current_payload:
            self.meta_view.setPlainText(json.dumps(self.meta_current_payload, indent=2, ensure_ascii=False))
        else:
            self.meta_view.setPlainText("No metadata found. You can add fields below.")

    def dm_remove_selected(self, reckless=False):
        if self.dm is None:
            return
        paths = self._selected_dm_paths()
        if not paths:
            return

        # confirmation
        names = "\n".join(str(p) for p in paths)
        if not reckless:
            ret = QMessageBox.question(
                self,
                "Remove content",
                f"Remove the following content safely from the hierarchy? "
                f"Check that a remote copy exist. \n\n{names}",
                QMessageBox.Yes | QMessageBox.No
            )
            reckless_flag = None
        else:
            ret = QMessageBox.question(
                self,
                "Remove content",
                f"Remove the following dataset(s) or content from the hierarchy?\n\n{names}",
                QMessageBox.Yes | QMessageBox.No
            )
            reckless_flag = "kill"

        if ret != QMessageBox.Yes:
            return

        for p in paths:
            ds_root, rel = self._find_dataset_root_and_rel(p)
            if ds_root is None or rel is None:
                continue
            rel_str = None if rel == Path("../roadmap_datamanager") else rel.as_posix()

            try:
                self._run_in_worker(
                    self.dm.remove_from_tree,
                    dataset=str(ds_root),
                    path=rel_str,
                    recursive=True,
                    reckless=reckless_flag
                )
                self.logviewer_append_text(f"[INFO] Removed dataset: {p}\n")
            except Exception as e:
                self.logviewer_append_text(f"[WARN] Could not remove {p}: {e}\n")

        self.dm_refresh_panel()

    def fileviewer_install_selected_sources_into_dm(self):
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
        # parts = [root_dir, project, campaign, experiment, category]
        root_dir, project, campaign, experiment, category = parts

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
                    self._run_in_worker(
                        self.dm.install_into_tree,
                        source=src,
                        project=project,
                        campaign=campaign,
                        experiment=experiment,
                        category=category,
                        metadata={"installed_by_gui": True},
                    )
                except Exception as e:
                    QMessageBox.critical(self, "Install failed", f"Could not install {src}:\n{e}")

        elif level == "category":
            for src in sources:
                try:
                    cat_path = Path(root_dir) / Path(project) / Path(campaign) / Path(experiment) / Path(category)
                    dm_path = Path(self.dm_current_path)
                    dest_rel = dm_path.relative_to(cat_path)
                    self._run_in_worker(
                        self.dm.install_into_tree,
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

        self.dm_refresh_panel()
        self.status.showMessage("Installed into subfolder of experiment.")

    def logviewer_append_text(self, text):
        self.log_view.insertPlainText(text)
        self.log_view.verticalScrollBar().setValue(
            self.log_view.verticalScrollBar().maximum())  # Scroll to bottom

    def select_remote(self):
        default_user = "fhein"
        default_repo = "datamanager"
        default_protocol = "SSH"

        # If we already have a URL, you can parse it to prefill:
        try:
            current = getattr(self.dm.cfg, "GIN_url", "") if self.dm else ""
            # naive parse:
            # git@gin.g-node.org:user/repo.git  OR  https://gin.g-node.org/user/repo.git
            if current:
                if current.startswith("git@"):
                    default_protocol = "SSH"
                    tail = current.split(":", 1)[1]
                elif current.startswith("https://"):
                    default_protocol = "HTTPS"
                    tail = current.split("gin.g-node.org/", 1)[1]
                else:
                    tail = ""
                if tail:
                    parts = tail.rstrip(".git").split("/", 1)
                    if len(parts) == 2:
                        default_user, default_repo = parts
        except Exception:
            pass

        dlg = GinRemoteDialog(self, default_user=default_user, default_repo=default_repo,
                              default_protocol=default_protocol)
        if dlg.exec() == QDialog.Accepted:
            url = dlg.url()
            # Store to config
            if self.dm is not None:
                setattr(self.dm.cfg, "GIN_url", url)
                self.dm.save_current_dm_configuration()
            self.status.showMessage(f"GIN remote set to: {url}", 5000)

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

    def metadata_save_changes(self):
        """
        Persist the current in-memory metadata payload to the dataset using DataManager.save_meta().
        Currently: writes a fresh metadata record for this (ds_root, rel) with the edited fields.
        """
        if self.dm is None:
            QMessageBox.information(self, "No datamanager", "No active datamanager.")
            return
        if self.meta_current_ds_root is None:
            QMessageBox.information(self, "No selection", "Nothing to save. Select an item and load metadata first.")
            return
        if self.meta_current_payload is None:
            QMessageBox.information(self, "No changes", "No metadata to save.")
            return

        ds_root = self.meta_current_ds_root
        rel = self.meta_current_rel  # None for dataset-level
        extra = dict(self.meta_current_payload)

        # optional: use 'name' from payload if present
        name = extra.get("name")

        try:
            self._run_in_worker(
                self.dm.save_meta,
                ds_path=ds_root,
                path=rel,
                name=name,
                extra=extra,
                # node_type: keep default or infer later; default 'experiment' in your API is fine for now
            )
        except Exception as e:
            QMessageBox.critical(self, "Save failed", f"Could not save metadata:\n{e}")
            return

        QMessageBox.information(self, "Metadata saved", "Metadata has been saved into the dataset.")
        # reload to show canonical stored version
        self.dm_show_selected_metadata()

    def set_datamanager(self, dm: DataManager):
        self.dm = dm
        self.dm_current_path = dm.root  # start at root
        self.status.showMessage(
            f"Using datamanager at {self.dm.root} as {self.dm.cfg.user_name} <{self.dm.cfg.user_email}>"
        )
        self.dm_refresh_panel()

    def sync_with_gin(self, recursive=True):
        if self.dm is None:
            return
        # simple pull
        self.dm.pull_from_remotes(dataset=self.dm.root, recursive=recursive)
        self.status.showMessage("Pulled from GIN.")


if __name__ == "__main__":
    app = QApplication(sys.argv)

    app.setStyle("Fusion")
    # Create a light palette
    light_palette = QPalette()
    app.setPalette(create_light_palette())

    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())
