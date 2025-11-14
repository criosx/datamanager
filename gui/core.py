from __future__ import annotations

import logging
import sys

from PySide6.QtCore import Qt, QObject, Signal, QRunnable
from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QLineEdit


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

