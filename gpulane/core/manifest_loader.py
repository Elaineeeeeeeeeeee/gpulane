from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from ..config import PROTECTED_ENV_VARS
from ..models import RepoContext
from .validation import validate_manifest_data


def load_yaml(path: Path) -> Dict[str, Any]:
    with open(path, "r") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"Expected a YAML mapping in {path}")
    return data


def normalize_args(raw_args: Optional[list], field_name: str) -> list:
    if raw_args is None:
        return []
    if not isinstance(raw_args, list):
        raise ValueError(f"Expected '{field_name}' to be a list")
    return [str(arg) for arg in raw_args]


def normalize_mapping(
    raw_value: Optional[Dict[str, Any]],
    field_name: str,
) -> Dict[str, Any]:
    if raw_value is None:
        return {}
    if not isinstance(raw_value, dict):
        raise ValueError(f"Expected '{field_name}' to be a mapping")
    return dict(raw_value)


def normalize_env_vars(
    raw_value: Optional[Dict[str, Any]],
    field_name: str,
) -> Dict[str, str]:
    env_vars = normalize_mapping(raw_value, field_name)
    normalized: Dict[str, str] = {}
    for key, value in env_vars.items():
        key_str = str(key)
        if key_str in PROTECTED_ENV_VARS:
            raise ValueError(
                f"{field_name} may not override protected variable '{key_str}'. "
                "Let Ray assign visible GPUs."
            )
        normalized[key_str] = str(value)
    return normalized


def find_manifest(start_path: Path, manifest_name: str = "ray_tasks.yaml") -> Path:
    current = start_path.resolve()
    if current.is_file():
        current = current.parent

    while True:
        candidate = current / manifest_name
        if candidate.exists():
            return candidate
        if current.parent == current:
            raise FileNotFoundError(
                f"Could not find {manifest_name} from {start_path.resolve()}"
            )
        current = current.parent


def resolve_manifest_path(
    manifest_value: Optional[str],
    repo_root_value: Optional[str],
) -> Path:
    if manifest_value:
        return Path(manifest_value).expanduser().resolve()

    if repo_root_value:
        return (Path(repo_root_value).expanduser().resolve() / "ray_tasks.yaml").resolve()

    return find_manifest(Path.cwd())


def load_repo_context(
    manifest_value: Optional[str],
    repo_root_value: Optional[str],
) -> RepoContext:
    manifest_path = resolve_manifest_path(manifest_value, repo_root_value)
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest file does not exist: {manifest_path}")

    data = load_yaml(manifest_path)
    validated_data = validate_manifest_data(data, manifest_path)
    repo_config = normalize_mapping(validated_data.get("repo"), "repo")
    tasks = normalize_mapping(validated_data.get("tasks"), "tasks")
    repo_root = (
        Path(repo_root_value).expanduser().resolve()
        if repo_root_value
        else manifest_path.parent.resolve()
    )
    repo_name = str(repo_config.get("name") or repo_root.name)

    normalized_tasks: Dict[str, Dict[str, Any]] = {}
    for task_name, task_cfg in tasks.items():
        if not isinstance(task_cfg, dict):
            raise ValueError(f"Task '{task_name}' must map to a configuration object")
        normalized_tasks[str(task_name)] = dict(task_cfg)

    return RepoContext(
        manifest_path=manifest_path,
        repo_root=repo_root,
        repo_name=repo_name,
        repo_config=repo_config,
        tasks=normalized_tasks,
    )
