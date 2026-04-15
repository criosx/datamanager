from pathlib import Path

from PySide6.QtCore import Qt, QRegularExpression
from PySide6.QtGui import QRegularExpressionValidator
from PySide6.QtWidgets import (
    QDialog, QDialogButtonBox, QFormLayout, QLabel,
    QLineEdit, QComboBox
)

from roadmap_datamanager import datalad_gin_api as dgapi

class GinRemoteDialog(QDialog):
    def __init__(self,
                 parent=None,
                 *,
                 default_user: str = "",
                 default_repo: str = "",
                 default_protocol: str = "SSH",
                 default_hostname: str = "gin.g-node.org",
                 default_host_alias: str = ""):
        super().__init__(parent)
        self.setWindowTitle("Set GIN Remote")
        self.setModal(True)

        self.repo = default_repo
        self.protocol = default_protocol

        self.user_edit = QLineEdit(self)
        self.user_edit.setPlaceholderText("e.g. your-gin-username")
        self.user_edit.setText(default_user)

        self.hostname_edit = QLineEdit(self)
        self.hostname_edit.setPlaceholderText("e.g. gin.g-node.org")
        self.hostname_edit.setText(default_hostname)

        self.host_alias_edit = QLineEdit(self)
        self.host_alias_edit.setVisible(True)
        self.host_alias_edit.setPlaceholderText("e.g. gin.g-node.org")
        self.host_alias_edit.setText(default_host_alias or default_hostname)

        self.preview = QLabel(self)
        self.preview.setTextInteractionFlags(Qt.TextSelectableByMouse)
        self.preview.setWordWrap(True)

        self.ssh_info = QLabel(self)
        self.ssh_info.setWordWrap(True)
        self.ssh_info.setVisible(True)
        self.ssh_info.setText(
            "SSH mode uses key-based authentication. The SSH host alias should match the "
            "entry in your ~/.ssh/config that points to the remote service. Only change remote information for new "
            "repositories before creation or cloning. Mixed remotes are currently not supported"
        )

        self.ssh_connection_info = QLabel(self)
        self.ssh_connection_info.setWordWrap(True)
        self.ssh_connection_info.setText(" ")

        self.ssh_key_box = QDialogButtonBox(parent=self)
        self.ssh_key_btn = self.ssh_key_box.addButton("Create SSH key pair", QDialogButtonBox.ActionRole)
        self.ssh_key_box.setCenterButtons(False)

        buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel, parent=self)
        self.ok_btn = buttons.button(QDialogButtonBox.Ok)
        self.ok_btn.setEnabled(False)

        layout = QFormLayout(self)
        layout.addRow("GIN username:", self.user_edit)
        layout.addRow("Host name:", self.hostname_edit)
        layout.addRow("SSH host alias:", self.host_alias_edit)
        layout.addRow("Connection info:", self.ssh_info)
        layout.addRow("Remote URL preview:", self.preview)
        layout.addRow("Connection info:", self.ssh_connection_info)
        layout.addRow("SSH key setup:", self.ssh_key_btn)
        layout.addWidget(buttons)

        self._host_alias_autofill_enabled = not bool(default_host_alias)

        # Wire up
        self.user_edit.textChanged.connect(self._update_state)
        self.hostname_edit.textChanged.connect(self._on_hostname_changed)
        self.host_alias_edit.textEdited.connect(self._on_host_alias_edited)
        self.ssh_key_btn.clicked.connect(self._create_ssh_key_pair)
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)

        self._update_state()  # initialize

    def _on_hostname_changed(self, text: str):
        if self._host_alias_autofill_enabled:
            self.host_alias_edit.setText(text.strip())
        self._update_state()

    def _on_host_alias_edited(self, _text: str):
        self._host_alias_autofill_enabled = False
        self._update_state()

    def _build_url(self) -> str:
        user = self.user_edit.text().strip()
        repo = self.repo.strip()
        hostname = self.hostname_edit.text().strip()
        if not user or not repo or not hostname:
            return ""
        return f"git@{hostname}:{user}/{repo}.git"

    def _build_preview_text(self) -> str:
        remote_url = self._build_url()
        if not remote_url:
            return ""
        host_alias = self.host_alias_edit.text().strip()
        ssh_user = "git" if self.hostname() == "gin.g-node.org" else self.username()
        ssh_target = f"{ssh_user}@{host_alias}" if host_alias else ""
        if ssh_target:
            return f"Remote URL: {remote_url}\nSSH target: {ssh_target}"
        return remote_url

    def _create_ssh_key_pair(self):
        hostname = self.hostname()
        ssh_host_user = self.ssh_user()
        suggested_private_key = dgapi.ssh_default_key_path(hostname, ssh_host_user)
        private_key_path = Path(suggested_private_key).expanduser()

        comment = f"{ssh_host_user}@{hostname}"
        success, message = dgapi.ssh_generate_keypair(private_key_path=private_key_path, comment=comment)

        if success:
            self.ssh_connection_info.setText(message)
        else:
            self.ssh_connection_info.setText(f"Creating SSH key pair failed: {message}")

        self._update_state()

    def _update_state(self):
        preview_text = self._build_preview_text()
        self.preview.setText(preview_text if preview_text else "—")

        user = self.username()
        repo = self.repo
        hostname = self.hostname()
        host_alias = self.host_alias()
        ok = bool(user and repo and hostname and host_alias)
        self.ok_btn.setEnabled(ok)

        ssh_host_user = self.ssh_user()
        found, message = dgapi.ssh_config_has_entry(host_alias, hostname, ssh_host_user)

        suggested_private_key = dgapi.ssh_default_key_path(hostname, ssh_host_user)
        private_key_path = Path(suggested_private_key).expanduser()
        public_key_path = private_key_path.with_suffix('.pub')

        if found:
            if public_key_path.exists():
                self.ssh_connection_info.setText(
                    f"Here is the folder with your public key '{str(public_key_path.name)}' that should be provided to "
                    f"your gin.g-node.org account.")
                self.ssh_key_btn.setEnabled(False)
            else:
                self.ssh_connection_info.setText(
                    f"Although an entry for the host and user was found in the SSH config file, no key was found under "
                    f"the canonical name: '{str(public_key_path.name)}'. It might be missing or under a different name."
                    f" Either regenerate a new key pair or provide the differently named key to gin.g-node.org. Inspect"
                    f" or clean up .ssh/config for a coherent setup.")
                self.ssh_key_btn.setEnabled(True)
        else:
            self.ssh_connection_info.setText(
                f"{message}\n\n"
                "No SSH key pair found. Create a new SSH ed25519 key pair locally. You can then copy the public key into "
                "your GIN account manually."
            )
            self.ssh_key_btn.setEnabled(True)


    def username(self) -> str:
        return self.user_edit.text().strip()

    def hostname(self) -> str:
        return self.hostname_edit.text().strip()

    def host_alias(self) -> str:
        return self.host_alias_edit.text().strip()

    def ssh_user(self) -> str:
        return "git" if self.hostname() == "gin.g-node.org" else self.username()
