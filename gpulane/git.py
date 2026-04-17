import subprocess
from pathlib import Path
from typing import Any, Dict, Optional


def git_output(repo_root: Path, *args: str) -> Optional[str]:
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return result.stdout.strip()


def parse_github_owner(remote_url: str) -> Optional[str]:
    import re
    patterns = (
        r"^https?://github\.com/(?P<owner>[^/]+)/[^/]+(?:\.git)?$",
        r"^git@github\.com:(?P<owner>[^/]+)/[^/]+(?:\.git)?$",
        r"^ssh://git@github\.com/(?P<owner>[^/]+)/[^/]+(?:\.git)?$",
    )
    for pattern in patterns:
        match = re.match(pattern, remote_url.strip())
        if match:
            owner = match.group("owner").strip()
            return owner or None
    return None


def detect_github_username_via_cli() -> Optional[str]:
    try:
        result = subprocess.run(
            ["gh", "api", "user", "--jq", ".login"],
            capture_output=True,
            text=True,
            check=True,
        )
        login = result.stdout.strip()
        return login or None
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None


def detect_github_submission_prefix(repo_root: Path) -> Optional[str]:
    login = detect_github_username_via_cli()
    if login:
        return login
    remote_url = git_output(repo_root, "config", "--get", "remote.origin.url")
    if remote_url:
        remote_owner = parse_github_owner(remote_url)
        if remote_owner:
            return remote_owner
    git_username = git_output(repo_root, "config", "user.name")
    if git_username:
        return git_username
    return None


def get_git_metadata(repo_root: Path) -> Dict[str, str]:
    branch = git_output(repo_root, "rev-parse", "--abbrev-ref", "HEAD") or "unknown"
    commit = git_output(repo_root, "rev-parse", "HEAD") or "unknown"
    status = git_output(repo_root, "status", "--porcelain") or ""
    return {
        "git_branch": branch,
        "git_commit": commit,
        "git_dirty": "true" if bool(status.strip()) else "false",
    }
