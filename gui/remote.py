from PySide6.QtCore import Qt, QRegularExpression
from PySide6.QtGui import QRegularExpressionValidator
from PySide6.QtWidgets import (
    QDialog, QDialogButtonBox, QFormLayout, QHBoxLayout, QLabel,
    QLineEdit, QComboBox
)


class GinRemoteDialog(QDialog):
    def __init__(self, parent=None, *, default_user: str = "", default_repo: str = "", default_protocol: str = "SSH"):
        super().__init__(parent)
        self.setWindowTitle("Set GIN Remote")
        self.setModal(True)

        self.protocol = QComboBox(self)
        self.protocol.addItems(["SSH", "HTTPS"])
        if default_protocol.upper() == "HTTPS":
            self.protocol.setCurrentIndex(1)

        self.user_edit = QLineEdit(self)
        self.user_edit.setPlaceholderText("e.g. your-gin-username")
        self.user_edit.setText(default_user)

        self.repo_edit = QLineEdit(self)
        self.repo_edit.setPlaceholderText("e.g. my-dataset")
        self.repo_edit.setText(default_repo)

        # Simple conservative validator: letters, numbers, dot, underscore, hyphen
        rx = QRegularExpression(r"^[A-Za-z0-9._-]+$")
        validator = QRegularExpressionValidator(rx, self.user_edit)
        self.user_edit.setValidator(validator)
        self.repo_edit.setValidator(validator)

        self.preview = QLabel(self)
        self.preview.setTextInteractionFlags(Qt.TextSelectableByMouse)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        self.ok_btn = buttons.button(QDialogButtonBox.Ok)
        self.ok_btn.setEnabled(False)

        layout = QFormLayout(self)
        layout.addRow("Protocol:", self.protocol)
        layout.addRow("GIN username:", self.user_edit)
        layout.addRow("Repository:", self.repo_edit)
        layout.addRow("Preview:", self.preview)
        layout.addWidget(buttons)

        # Wire up
        self.user_edit.textChanged.connect(self._update_state)
        self.repo_edit.textChanged.connect(self._update_state)
        self.protocol.currentIndexChanged.connect(self._update_state)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        self._update_state()  # initialize

    def _build_url(self) -> str:
        user = self.user_edit.text().strip()
        repo = self.repo_edit.text().strip()
        if not user or not repo:
            return ""
        if self.protocol.currentText() == "SSH":
            return f"git@gin.g-node.org:{user}/{repo}.git"
        else:
            return f"https://gin.g-node.org/{user}/{repo}.git"

    def _update_state(self):
        url = self._build_url()
        self.preview.setText(url if url else "—")
        self.ok_btn.setEnabled(bool(url))

    def url(self) -> str:
        return self._build_url()

    def username(self) -> str:
        return self.user_edit.text().strip()

    def repo(self) -> str:
        return self.repo_edit.text().strip()
