from __future__ import annotations

import logging
import sys

from PySide6.QtCore import Qt, QObject, Signal, Slot, QRunnable
from PySide6.QtGui import QPalette, QColor
from PySide6.QtWidgets import QDialog, QDialogButtonBox, QFormLayout, QLineEdit


def create_light_palette():
    p = QPalette()
    p.setColor(QPalette.ColorRole.Window, QColor(240, 240, 240))
    p.setColor(QPalette.ColorRole.WindowText, Qt.GlobalColor.black)
    p.setColor(QPalette.ColorRole.Base, QColor(255, 255, 255))
    p.setColor(QPalette.ColorRole.AlternateBase, QColor(240, 240, 240))
    p.setColor(QPalette.ColorRole.ToolTipBase, Qt.GlobalColor.white)
    p.setColor(QPalette.ColorRole.ToolTipText, Qt.GlobalColor.black)
    p.setColor(QPalette.ColorRole.Text, Qt.GlobalColor.black)
    p.setColor(QPalette.ColorRole.Button, QColor(240, 240, 240))
    p.setColor(QPalette.ColorRole.ButtonText, Qt.GlobalColor.black)
    p.setColor(QPalette.ColorRole.BrightText, Qt.GlobalColor.red)
    p.setColor(QPalette.ColorRole.Link, QColor(42, 130, 218))
    p.setColor(QPalette.ColorRole.Highlight, QColor(42, 130, 218))
    p.setColor(QPalette.ColorRole.HighlightedText, Qt.GlobalColor.white)

    # --- inactive buttons ---
    p.setColor(QPalette.ColorGroup.Inactive, QPalette.ColorRole.Button, QColor(230, 230, 230))
    p.setColor(QPalette.ColorGroup.Inactive, QPalette.ColorRole.ButtonText, QColor(80, 80, 80))

    # --- disabled buttons ---
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.Button, QColor(230, 230, 230))
    p.setColor(QPalette.ColorGroup.Disabled, QPalette.ColorRole.ButtonText, QColor(150, 150, 150))
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
        buttons = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel,
                                   parent=self)
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

    @Slot()
    def run(self):
        self.signals.started.emit()
        try:
            out = self.fn(*self.args, **self.kwargs)
            self.signals.done.emit(out)
        except Exception as e:
            self.signals.error.emit(str(e))


class WorkerSignals(QObject):
    started = Signal()
    done = Signal(object)
    error = Signal(str)
    progress = Signal(str)

