from __future__ import annotations

import os
import platform
import shlex
import streamlit as st
import subprocess

from pathlib import Path

def file_browser_button(path: Path, label="↗️"):
    if path.exists():
        if st.button(label, help=f"Open {path}"):
            open_in_file_browser(path)

def open_in_file_browser(path: Path):
    if not path.exists():
        return

    system = platform.system()

    if system == "Darwin":        # macOS
        subprocess.run(["open", path])
    elif system == "Windows":
        subprocess.run(["explorer", path])
    elif system == "Linux":
        subprocess.run(["xdg-open", path])

def ssh_config_block(host_alias: str, hostname: str, username: str) -> str:
    return (
        f"Host {host_alias}\n"
        f"    HostName {hostname}\n"
        f"    User {username}\n"
    )


def ssh_config_path() -> Path:
    # Standard location across macOS, Linux, and most Windows OpenSSH installs
    return Path.home() / ".ssh" / "config"


def ssh_config_has_entry(host_alias: str, hostname: str | None = None, username: str | None = None) -> tuple[bool, str]:
    """
    Checks whether an entry for the host already exists in ~/.ssh/config.
    Returns (found, message).
    """

    config_file = ssh_config_path()

    if not config_file.exists():
        return False, f"SSH config file does not exist yet: {config_file}"

    try:
        text = config_file.read_text(encoding="utf-8")
    except Exception as exc:
        return False, f"Could not read SSH config: {exc}"

    lines = text.splitlines()

    current_host = None
    host_blocks = {}

    for line in lines:
        stripped = line.strip()
        if stripped.lower().startswith("host "):
            current_host = stripped.split(maxsplit=1)[1]
            host_blocks[current_host] = []
        elif current_host:
            host_blocks[current_host].append(stripped)

    if host_alias in host_blocks:
        block = "\n".join(host_blocks[host_alias])
        if hostname and f"hostname {hostname}".lower() not in block.lower():
            return True, f"Host '{host_alias}' exists but different from hostname ({hostname})."
        if username and f"user {username}".lower() not in block.lower():
            return True, f"Host '{host_alias}' exists but user differs."
        return True, f"Host '{host_alias}' exists in {config_file}."

    return False, f"No SSH config entry for host '{host_alias}'."

def ssh_default_key_path(hostname: str, username: str) -> Path:
    safe_host = hostname.replace(".", "_").replace("/", "_")
    safe_user = username.replace(".", "_").replace("/", "_")
    return Path.home() / ".ssh" / f"id_ed25519_{safe_user}_{safe_host}"

def ssh_ensure_ssh_dir() -> Path:
    ssh_dir = Path.home() / ".ssh"
    ssh_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
    try:
        os.chmod(ssh_dir, 0o700)
    except OSError:
        pass
    return ssh_dir

def ssh_generate_keypair(private_key_path: Path, comment: str = "") -> tuple[bool, str]:
    ssh_ensure_ssh_dir()

    if private_key_path.exists() or private_key_path.with_suffix(".pub").exists():
        return False, f"Key file already exists: {private_key_path}"

    cmd = [
        "ssh-keygen",
        "-t", "ed25519",
        "-f", str(private_key_path),
        "-N", "",
    ]
    if comment:
        cmd.extend(["-C", comment])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except FileNotFoundError:
        return False, "ssh-keygen was not found on this system."
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        stdout = (exc.stdout or "").strip()
        message = stderr if stderr else stdout if stdout else str(exc)
        return False, f"ssh-keygen failed: {message}"

    try:
        os.chmod(private_key_path, 0o600)
    except OSError:
        pass

    output = (result.stdout or "").strip()
    return True, output if output else f"Created SSH key pair at {private_key_path}"

def ssh_test_connection(host_alias: str) -> tuple[bool, str, str]:
    """
    Test SSH connectivity using the configured host alias.
    Returns (success, summary_message, detailed_output).
    """
    cmd = [
        "ssh",
        "-T",
        "-o", "BatchMode=yes",
        host_alias,
    ]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
    except FileNotFoundError:
        return False, "ssh was not found on this system.", ""
    except subprocess.TimeoutExpired:
        return False, f"SSH connection test timed out for host '{host_alias}'.", ""
    except Exception as exc:
        return False, f"SSH connection test failed: {exc}", ""

    stdout = (result.stdout or "").strip()
    stderr = (result.stderr or "").strip()
    combined = "\n".join(part for part in [stdout, stderr] if part).strip()

    success_markers = [
        "successfully authenticated",
        "welcome to gin",
        "you've successfully authenticated",
    ]
    permission_markers = [
        "shell access is not supported",
        "pty allocation request failed",
    ]

    lowered = combined.lower()
    if any(marker in lowered for marker in success_markers) or any(marker in lowered for marker in permission_markers):
        return True, f"SSH connection to '{host_alias}' appears to work.", combined

    if result.returncode == 0:
        return True, f"SSH connection to '{host_alias}' succeeded.", combined

    return False, f"SSH connection to '{host_alias}' failed.", combined


def UI_fragment_SSH_connection(cfg):
    gin_user = st.text_input('GIN User', value=cfg.GIN_user)
    if gin_user is not None and  gin_user != cfg.GIN_user:
        cfg.GIN_user = gin_user


    ssh_hostname = st.text_input('GIN URL / SSH Host Name', value=cfg.GIN_url)
    if ssh_hostname != cfg.GIN_url:
        cfg.GIN_url = ssh_hostname

    ssh_host_alias = st.text_input('GIN / SSH Host Alias for .ssh/config', value=cfg.SSH_host_alias)
    if ssh_host_alias != cfg.SSH_host_alias:
        cfg.SSH_host_alias = ssh_host_alias

    st.text("We use SSH key authentication for communicating with GIN. This section will ensure the proper key setup.")

    # GIN SSH UI section
    ssh_host_alias_default = ssh_hostname
    ssh_host_user = 'git' if ssh_hostname == 'gin.g-node.org' else gin_user

    config_file = ssh_config_path()
    found, message = ssh_config_has_entry(ssh_host_alias, ssh_hostname, ssh_host_user)

    suggested_private_key = ssh_default_key_path(ssh_hostname, ssh_host_user)
    private_key_path = Path(suggested_private_key).expanduser()
    public_key_path = private_key_path.with_suffix('.pub')

    if found:
        st.success(message)
        if public_key_path.exists():
            st.text(f"Here is the folder with you public key '{str(public_key_path.name)}' that should be provided to "
                    f"your gin.g-node.org account.")
        else:
            st.text(f"Although an entry for the host and user was found in the SSH config file, no key was found under "
                    f"the canonical name: '{str(public_key_path.name)}'. It might be missing or under a different name."
                    f" Either regenerate a new key pair or provide the differently named key to gin.g-node.org. Inspect"
                    f" or clean up .ssh/config for a coherent setup.")
    else:
        st.info(message)
        if st.button("Create new SSH key pair."):


            st.text("This creates an SSH ed25519 key pair locally. You can then copy the public key into your GIN "
                    "account manually.")

            comment = f"{ssh_host_user}@{ssh_hostname}"
            success, message = ssh_generate_keypair(private_key_path=private_key_path, comment=comment)
            if success:
                st.success(message)
            else:
                st.error(message)
        st.stop()

    col11, col12, col13 = st.columns([3, 4, 3])
    with col11:
        file_browser_button(public_key_path.parent, label="Show SSH Directory ↗️")
    with col12:
        if st.button("Test SSH Connection", type='primary'):
            ok, summary, details = ssh_test_connection(ssh_host_alias)
            if ok:
                st.success(summary)
            else:
                st.error(summary)
            if details:
                st.code(details)
            st.caption(f"Command: {shlex.join(['ssh', '-T', '-o', 'BatchMode=yes', ssh_host_alias])}")

            if not ok:
                st.stop()

    return cfg