from pathlib import Path
from typing import Any, Dict, Optional

from .core import manifest_loader
from .models import RepoContext


def load_yaml(path: Path) -> Dict[str, Any]:
    return manifest_loader.load_yaml(path)


def normalize_args(raw_args: Optional[list], field_name: str) -> list:
    return manifest_loader.normalize_args(raw_args, field_name)


def normalize_mapping(raw_value: Optional[Dict[str, Any]], field_name: str) -> Dict[str, Any]:
    return manifest_loader.normalize_mapping(raw_value, field_name)


def normalize_env_vars(raw_value: Optional[Dict[str, Any]], field_name: str) -> Dict[str, str]:
    return manifest_loader.normalize_env_vars(raw_value, field_name)


def find_manifest(start_path: Path, manifest_name: str = "ray_tasks.yaml") -> Path:
    return manifest_loader.find_manifest(start_path, manifest_name)


def resolve_manifest_path(
    manifest_value: Optional[str],
    repo_root_value: Optional[str],
) -> Path:
    return manifest_loader.resolve_manifest_path(manifest_value, repo_root_value)


def load_repo_context(
    manifest_value: Optional[str],
    repo_root_value: Optional[str],
) -> RepoContext:
    return manifest_loader.load_repo_context(manifest_value, repo_root_value)
