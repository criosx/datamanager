from __future__ import annotations

import json
import os
import platform
import shlex
import streamlit as st
import subprocess

from roadmap_datamanager import datamanager
from roadmap_datamanager import datalad_gin_api as dgapi

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

def UI_fragment_datalad(cfg):
    """
    Datalad Streamlit UI fragment
    :param cfg: the calling apps configuration dataclass
    :return: (dataclass , Datamanager) the modified configuration dataclass, the Datamanager instance (None if invalid)
    """
    st.write("""
    ## DataLad
    """)

    use_datalad = st.toggle(label='Use DataLad', value=st.session_state.cfg.use_datalad)
    if use_datalad != st.session_state.cfg.use_datalad:
        cfg.use_datalad = use_datalad

    if not cfg.use_datalad:
        return cfg, None

    root_dir = Path(cfg.dm_root).expanduser().resolve()

    dm = datamanager.DataManager(
        root=root_dir,
        user_name=cfg.user_name,
        user_email=cfg.user_email,
        default_project=cfg.project,
        default_campaign=cfg.campaign,
        GIN_url=cfg.GIN_url,
        GIN_repo=cfg.user_name,
        GIN_user=cfg.GIN_user,
        verbose=True
    )

    project_dir = root_dir / cfg.project
    campaign_dir = project_dir / cfg.campaign
    exp_dir = campaign_dir / cfg.experiment

    _, r_installed, r_status = dm.get_status(dataset=root_dir, recursive=False)
    _, p_installed, p_status = dm.get_status(dataset=project_dir, recursive=False)
    _, c_installed, c_status = dm.get_status(dataset=campaign_dir, recursive=False)
    _, e_installed, e_status = dm.get_status(dataset=exp_dir, recursive=False)
    ds_installed = r_installed and p_installed and c_installed and e_installed

    # all dirs exists at this point in the script as checked above
    stc7, stc8 = st.columns([7, 3])
    if not ds_installed:
        with stc7:
            st.info('DataLad branch (project / campaign / experiment) is not (fully) initialized.')
        with stc8:
            if st.button("Initialize DataLad Tree.", type='primary'):
                # ensure that data structure is a datalad tree
                dm.init_tree(project=cfg.project, campaign=cfg.campaign, experiment=cfg.experiment, force=True)
                st.rerun()
        return cfg, None

    status = r_status + p_status + c_status + e_status
    with st.expander(label='Detailed Status', expanded=False):
        only_non_clean = st.toggle(label='Show only non-clean entries.', value=True)
        if only_non_clean:
            status = [element for element in status if element['state'] != 'clean']
        # Pretty-print the combined DataLad status (list of dicts) as JSON.
        st.text(json.dumps(status, indent=2, sort_keys=True, default=str))

    clean = True
    for element in status:
        if element['state'] != 'clean':
            clean = False
            break

    if not clean:
        with stc7:
            st.warning('DataLad branch (project / campaign / experiment) has unsaved changes.')
        with stc8:
            if st.button("Save DataLad Branch.", type='primary'):
                dgapi.save_branch(path=exp_dir)
                st.rerun()
        return cfg, None

    with stc7:
        st.success('DataLad branch (project / campaign / experiment) is saved (clean).')
    return cfg, dm

def UI_fragment_PCE(cfg):
    """
    Implemenents a Project / Campaign / Experiment selection Streamlit UI fragment
    :param cfg: a datamanager compatible configuration dataclase
    :return: (cfg, Bool) the (modified) configuration dataclass, whether to create the P/C/E folders upon return
    """
    def category_input(ds_root, cfg_item, category_name):
        category_list = []
        default_category = None
        if ds_root.is_dir():
            category_list = [p.name for p in ds_root.iterdir() if p.is_dir() and not p.name.startswith(".")]
            category_list.sort()
        if cfg_item is not None:
            if cfg_item not in category_list:
                category_list.append(cfg_item)
                category_list.sort()
            default_category = category_list.index(cfg_item)
        category = st.selectbox(
            label=category_name,
            options=category_list,
            index=default_category,
            placeholder=f"Create or select a {category_name}",
            accept_new_options=True)
        return category

    st.write("""
        ## Project / Campaign / Experiment
        """)

    dm_root = Path(cfg.dm_root).expanduser().resolve()

    project = category_input(
        ds_root=dm_root,
        cfg_item=cfg.project,
        category_name='project'
    )
    if project and project != cfg.project:
        cfg.project = project
    if cfg.project is None:
        return cfg, False

    campaign = category_input(
        ds_root=dm_root / cfg.project,
        cfg_item=cfg.campaign,
        category_name='campaign'
    )
    if campaign and campaign != cfg.campaign:
        cfg.campaign = campaign
    if cfg.campaign is None:
        return cfg, False

    experiment = category_input(
        ds_root=dm_root / cfg.project / cfg.campaign,
        cfg_item=cfg.experiment,
        category_name='experiment'
    )
    if experiment and experiment != cfg.experiment:
        cfg.experiment = experiment
    if cfg.experiment is None:
        return cfg, False

    col4, col5, col6 = st.columns([6, 1, 3])
    exp_dir = dm_root / cfg.project / cfg.campaign / cfg.experiment
    info_text = "Experiment directory " + str(exp_dir)
    if exp_dir.is_dir():
        info_text += " exists."
        with col4:
            st.text(info_text)
        with col5:
            file_browser_button(exp_dir)
    else:
        info_text += " has not been created, yet."
        with col4:
            st.text(info_text)
        with col6:
            if st.button("Create Experimental Directory", type='primary'):
                exp_dir.mkdir(parents=True, exist_ok=True)
                return cfg, True

    return cfg, False

def UI_fragment_SSH_connection(cfg):
    """
    A Stremlit UI fragment that facilates the setup of an SSH connection via a public / private key pair to a service
    such as gin.g-node.org.
    :param cfg: a datamanager compatible configuration dataclase
    :return: the (modified) configuration dataclass
    """
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

    stc1, stc2, stc3 = st.columns([3, 4, 3])
    with stc1:
        file_browser_button(public_key_path.parent, label="Show SSH Directory ↗️")
    with stc2:
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

def UI_fragment_user(cfg, user_root_dir, enable_user_selection=True):
    """
    Implementeation of a user selection dialog as a Streamlit UI fragment
    :param cfg: a datamanager compatibile configuration dataclass
    :param user_root_dir: (str | Path) the root directory in which datamanager trees for each user are placed
    :param enable_user_selection: (bool) whether to enable user selection
    :return: the modified configuration dataclass
    """
    st.write("""
        ## User
                 """)
    user_list = []
    default_user = None
    root = Path(user_root_dir).expanduser().resolve()
    if root.is_dir():
        user_list = [p.name for p in root.iterdir() if p.is_dir() and not p.name.startswith(".")]
        user_list.sort()
    if cfg.dm_root is not None:
        dm_root = Path(cfg.dm_root).expanduser().resolve()
        if dm_root.name not in user_list:
            user_list.append(dm_root.name)
            user_list.sort()
        default_user = user_list.index(dm_root.name)
    user = st.selectbox(
        "User Name",
        options=user_list,
        index=default_user,
        placeholder='Create or select a user.',
        accept_new_options=True,
        disabled=not enable_user_selection,
    )
    if user and user != cfg.user_name:
        cfg.user_name = user
        cfg.project = None
        cfg.campaign = None
        cfg.experiment = None

    if cfg.user_name is None:
        cfg.dm_root = None
        return cfg

    dm_root = user_root_dir / cfg.user_name
    cfg.dm_root = dm_root

    stc4, stc5, stc6 = st.columns([6, 1, 3])
    info_text = "Data root directory " + str(dm_root)
    if dm_root.is_dir():
        info_text += " exists."
        with stc4:
            st.text(info_text)
        with stc5:
            file_browser_button(dm_root)
    else:
        info_text += (" has not been created, yet. If you intend to use GIN remote storage, make sure that the SSH "
                      "connection is working properly. The script will attempt to clone an existing repository for "
                      "this user when creating a data root. Potential reconfigurations in that cloned repository "
                      "might prompt for a GIN ")

        with stc4:
            st.text(info_text)
        with stc6:
            if st.button("Create Data Root Directory", type='primary'):
                dm_root.mkdir(parents=True, exist_ok=True)
                # check SSH connection
                ssh_host_alias = cfg.SSH_host_alias
                ok = False
                if ssh_host_alias is not None:
                    ok, summary, details = ssh_test_connection(ssh_host_alias)
                if not ok:
                    st.warning("SSH connection failed.")
                else:
                    try:
                        source_url = 'https://' + cfg.GIN_url + '/' + cfg.GIN_user + '/' + cfg.user_name
                        dgapi.clone_from_remote(
                            dest=dm_root,
                            user_name=cfg.GIN_user,
                            repo_name=cfg.user_name
                        )
                    except Exception as e:
                        st.info("No remote repository found.")
                        st.text(f"Detailed response: {e}")
                st.rerun()

        st.write("""## Remote Connection Setup""")
        cfg = UI_fragment_SSH_connection(cfg)

    return cfg