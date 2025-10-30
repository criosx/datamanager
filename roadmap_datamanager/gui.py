from PySide6.QtWidgets import (
    QApplication, QMainWindow, QFileDialog, QTreeView, QDockWidget, QWidget,
    QVBoxLayout, QPushButton, QStatusBar
)
from PySide6.QtCore import Qt, QThreadPool, QObject, Signal, QRunnable
import sys
import pathlib


class WorkerSignals(QObject):
    done = Signal(object)
    error = Signal(str)
    progress = Signal(str)


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
        self.setWindowTitle("ROADMAP Data Manager")
        self.pool = QThreadPool.globalInstance()
        self.status = QStatusBar(); self.setStatusBar(self.status)

        # Center staging panel
        center = QWidget()
        layout = QVBoxLayout(center)
        self.btn_select_root = QPushButton("Select datamanager root…")
        self.btn_select_root.clicked.connect(self.select_root)
        layout.addWidget(self.btn_select_root)
        self.setCentralWidget(center)

        # Left: dataset tree (placeholder)
        self.tree = QTreeView()
        left = QDockWidget("Projects / Campaigns / Experiments", self)
        left.setWidget(self.tree)
        self.addDockWidget(Qt.LeftDockWidgetArea, left)

    def select_root(self):
        path = QFileDialog.getExistingDirectory(self, "Choose datamanager root")
        if not path:
            return
        root = pathlib.Path(path)
        # Kick off a background validation (e.g., datalad status)
        self.status.showMessage(f"Selected: {root}")
        self.run_job(self.validate_root, root)

    def validate_root(self, root: pathlib.Path):
        # TODO: call your DataManager API here; raise on invalid
        # e.g., dl.Dataset(str(root)).status()
        return {"root": str(root), "ok": True}

    def run_job(self, fn, *args, **kwargs):
        w = Worker(fn, *args, **kwargs)
        w.signals.progress.connect(lambda msg: self.status.showMessage(msg))
        w.signals.error.connect(lambda err: self.status.showMessage(f"Error: {err}"))
        w.signals.done.connect(lambda res: self.status.showMessage(f"OK: {res}"))
        self.pool.start(w)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    w = MainWindow(); w.resize(1100, 700); w.show()
    sys.exit(app.exec())
