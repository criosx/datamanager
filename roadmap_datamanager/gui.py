from __future__ import annotations

import sys

from pathlib import Path

from PySide6.QtCore import Qt, QThreadPool, QObject, Signal, QRunnable, QDir
from PySide6.QtGui import QStandardItemModel, QStandardItem, QAction
from PySide6.QtWidgets import (
    QApplication, QDialog, QDialogButtonBox, QDockWidget, QFileDialog, QFileSystemModel, QFormLayout, QHBoxLayout,
    QInputDialog, QLabel, QLineEdit, QListWidget, QListWidgetItem,
    QMainWindow, QMessageBox, QPushButton, QTreeView, QSplitter, QStatusBar, QToolBar,
    QVBoxLayout, QWidget
)

# import your datamanager
from roadmap_datamanager.datamanager import DataManager


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

    def _dm_current_level(self):
        """
        Returns (level, parts)
        level ∈ {"root", "project", "campaign", "experiment", "below-experiment"}
        parts is a tuple/list of path parts *after* the dm root.
        """
        if self.dm is None or self.dm_current_path is None:
            return "root", []
        root = self.dm.root
        cur = self.dm_current_path
        rel = cur.relative_to(root)
        if str(rel) == ".":
            return "root", []
        parts = list(rel.parts)
        depth = len(parts)
        if depth == 1:
            return "project", parts
        elif depth == 2:
            return "campaign", parts
        elif depth == 3:
            return "experiment", parts
        else:
            return "below-experiment", parts

    def _go_home(self):
        home = QDir.homePath()
        self.fs_tree.setRootIndex(self.fs_model.index(home))

    def _choose_browser_root(self):
        path = QFileDialog.getExistingDirectory(self, "Select folder to browse")
        if path:
            self.fs_tree.setRootIndex(self.fs_model.index(path))

    def _create_split_view(self):
        splitter = QSplitter()
        splitter.setOrientation(Qt.Horizontal)

        # ----- LEFT: datamanager panel -----
        self.dm_panel = QWidget()
        dm_layout = QVBoxLayout(self.dm_panel)
        dm_layout.setContentsMargins(4, 4, 4, 4)

        # top bar: show level + name
        top_bar = QHBoxLayout()
        self.lbl_level = QLabel("Level: —")
        self.lbl_name = QLabel("Name: —")
        top_bar.addWidget(self.lbl_level)
        top_bar.addWidget(self.lbl_name)
        top_bar.addStretch(1)
        dm_layout.addLayout(top_bar)

        # nav buttons
        nav_bar = QHBoxLayout()
        self.btn_up = QPushButton("↑ Up")
        self.btn_open = QPushButton("Open")
        self.btn_new_dataset = QPushButton("New dataset here…")
        self.btn_up.clicked.connect(self.dm_go_up)
        self.btn_open.clicked.connect(self.dm_open_selected)
        self.btn_new_dataset.clicked.connect(self.dm_create_dataset_here)
        nav_bar.addWidget(self.btn_up)
        nav_bar.addWidget(self.btn_open)
        nav_bar.addWidget(self.btn_new_dataset)
        dm_layout.addLayout(nav_bar)

        # list of children at current level
        self.dm_list = QListWidget()
        dm_layout.addWidget(self.dm_list, 1)

        splitter.addWidget(self.dm_panel)

        # ----- RIGHT: filesystem browser -----
        self.fs_panel = QWidget()
        fs_layout = QVBoxLayout(self.fs_panel)
        fs_layout.setContentsMargins(4, 4, 4, 4)

        tb = QToolBar()
        act_home = QAction("Home", self)
        act_home.triggered.connect(self._go_home)
        act_choose = QAction("Choose folder…", self)
        act_choose.triggered.connect(self._choose_browser_root)
        tb.addAction(act_home)
        tb.addAction(act_choose)
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

        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)

        self.setCentralWidget(splitter)

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
        if level in ("experiment", "below-experiment"):
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
            label = f"Campaign name for project “{parts[0]}”:"
        else:  # level == "campaign"
            title = "New experiment"
            label = f"Experiment name for {parts[0]} / {parts[1]}:"

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
            project = parts[0]
            self.dm.init_tree(project=project, campaign=name)
        elif level == "campaign":
            project, campaign = parts[0], parts[1]
            self.dm.init_tree(project=project, campaign=campaign, experiment=name)

        # after creating, refresh the panel so the new item shows up
        self.refresh_dm_panel()

    def refresh_dm_panel(self):
        if self.dm is None or self.dm_current_path is None:
            self.lbl_level.setText("Level: —")
            self.lbl_name.setText("Name: —")
            self.dm_list.clear()
            return

        level, parts = self._dm_current_level()

        if level == "root":
            name = self.dm.root.name
        else:
            name = parts[-1]

        self.lbl_level.setText(f"Level: {level}")
        self.lbl_name.setText(f'Name: "{name}"')

        # enable/disable “new dataset” by level
        self.btn_new_dataset.setEnabled(level in ("root", "project", "campaign"))
        self.btn_up.setEnabled(level != "root")

        # list children of current path
        self.dm_list.clear()
        for child in self.dm_current_path.iterdir():
            if not child.is_dir() or child.name.startswith("."):
                continue
            item = QListWidgetItem(child.name)
            item.setData(Qt.UserRole, str(child))
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

    def sync_with_gin(self, recursive=True):
        if self.dm is None:
            return
        # simple pull
        self.dm.pull_from_remotes(dataset=self.dm.root, recursive=recursive)
        self.status.showMessage("Pulled from GIN.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MainWindow()
    w.resize(1100, 700)
    w.show()
    sys.exit(app.exec())
