from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import shlex
from typing import Optional
import os


@dataclass(frozen=True)
class TreeCtx:
    root: Path
    user: str
    project: Optional[str] = None
    campaign: Optional[str] = None
    experiment: Optional[str] = None


def set_git_annex_path():
    def which_in_zsh(exe="git-annex"):
        shell = os.environ.get("SHELL", "/bin/zsh")
        # -i => interactive so .zshrc is sourced; use the shell builtin `command -v`
        cmd = f"command -v {shlex.quote(exe)} || true"
        p = subprocess.run([shell, "-i", "-c", cmd],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        path = p.stdout.strip().splitlines()[-1] if p.stdout else ""
        return path or None

    if shutil.which("git-annex") is not None:
        return True

    path = which_in_zsh("git-annex")
    if path:
        os.environ["PATH"] = f"{os.path.dirname(path)}:{os.environ.get('PATH', '')}"
        return True
    else:
        return False


def ssh_to_https(u: str) -> str:
    # git@gin.g-node.org:/owner/repo(.git) -> https://gin.g-node.org/owner/repo
    if u.startswith('git@'):
        host = u.split('@', 1)[1].split(':', 1)[0]
        path = u.split(':', 1)[1]
        if path.endswith('.git'):
            path = path[:-4]
        return f"https://{host}/{path}"
    return u

