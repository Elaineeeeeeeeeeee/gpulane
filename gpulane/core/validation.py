from pathlib import Path
from typing import Any, Dict, Iterable, Optional


SUPPORTED_MANIFEST_VERSION = 1
ALLOWED_REPO_FIELDS = {
    "name",
    "output_root",
    "submission_prefix",
}
ALLOWED_TASK_FIELDS = {
    "command",
    "cwd",
    "default_args",
    "description",
    "entrypoint_resources",
    "env",
    "kind",
    "metadata",
    "priority",
    "resources",
    "resume_run_name_arg",
    "runtime_env",
}
ALLOWED_RESOURCE_FIELDS = {
    "cpu",
    "gpu",
    "memory",
    "custom",
}


def _ensure_mapping(value: Any, field_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError(f"Expected '{field_name}' to be a mapping")
    return dict(value)


def _ensure_list(value: Any, field_name: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise ValueError(f"Expected '{field_name}' to be a list")


def _ensure_stringish(value: Any, field_name: str) -> None:
    if value is None:
        return
    if isinstance(value, (dict, list, tuple, set)):
        raise ValueError(f"Expected '{field_name}' to be a string-like value")


def _ensure_known_fields(
    payload: Dict[str, Any],
    *,
    field_name: str,
    allowed_fields: Iterable[str],
) -> None:
    allowed = set(allowed_fields)
    unknown = sorted(str(key) for key in payload if str(key) not in allowed)
    if unknown:
        raise ValueError(
            f"Unknown field(s) in {field_name}: {', '.join(unknown)}"
        )


def _validate_resource_mapping(raw_resources: Any, field_name: str) -> None:
    resources = _ensure_mapping(raw_resources, field_name)
    _ensure_known_fields(
        resources,
        field_name=field_name,
        allowed_fields=ALLOWED_RESOURCE_FIELDS,
    )
    for numeric_field in ("cpu", "gpu", "memory"):
        if numeric_field not in resources:
            continue
        try:
            float(resources[numeric_field])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Expected '{field_name}.{numeric_field}' to be numeric"
            ) from exc

    if "custom" in resources:
        custom = _ensure_mapping(resources["custom"], f"{field_name}.custom")
        for key, value in custom.items():
            if not str(key).strip():
                raise ValueError(f"Resource key in '{field_name}.custom' may not be empty")
            try:
                float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"Expected '{field_name}.custom.{key}' to be numeric"
                ) from exc


def _validate_runtime_env(runtime_env: Any, field_name: str) -> None:
    runtime_env_mapping = _ensure_mapping(runtime_env, field_name)
    env_vars = runtime_env_mapping.get("env_vars")
    if env_vars is not None:
        _ensure_mapping(env_vars, f"{field_name}.env_vars")


def _validate_task_config(task_name: str, task_cfg: Any) -> None:
    field_name = f"tasks.{task_name}"
    task_mapping = _ensure_mapping(task_cfg, field_name)
    _ensure_known_fields(
        task_mapping,
        field_name=field_name,
        allowed_fields=ALLOWED_TASK_FIELDS,
    )
    _ensure_stringish(task_mapping.get("cwd"), f"{field_name}.cwd")
    _ensure_list(task_mapping.get("command"), f"{field_name}.command")
    _ensure_list(task_mapping.get("default_args"), f"{field_name}.default_args")
    _ensure_mapping(task_mapping.get("env"), f"{field_name}.env")
    _ensure_mapping(task_mapping.get("metadata"), f"{field_name}.metadata")
    _ensure_stringish(task_mapping.get("description"), f"{field_name}.description")
    _ensure_stringish(task_mapping.get("kind"), f"{field_name}.kind")
    _ensure_stringish(task_mapping.get("priority"), f"{field_name}.priority")
    _ensure_stringish(
        task_mapping.get("resume_run_name_arg"),
        f"{field_name}.resume_run_name_arg",
    )
    _validate_runtime_env(task_mapping.get("runtime_env"), f"{field_name}.runtime_env")
    _validate_resource_mapping(task_mapping.get("resources"), f"{field_name}.resources")
    _validate_resource_mapping(
        task_mapping.get("entrypoint_resources"),
        f"{field_name}.entrypoint_resources",
    )


def _parse_manifest_version(raw_value: Any, manifest_path: Path) -> int:
    if raw_value is None:
        raise ValueError(
            f"Manifest {manifest_path} must define 'version: {SUPPORTED_MANIFEST_VERSION}'"
        )
    try:
        version = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Manifest version in {manifest_path} must be an integer"
        ) from exc
    if version != SUPPORTED_MANIFEST_VERSION:
        raise ValueError(
            f"Unsupported manifest version '{raw_value}' in {manifest_path}. "
            f"Supported version: {SUPPORTED_MANIFEST_VERSION}"
        )
    return version


def validate_manifest_data(
    data: Dict[str, Any],
    manifest_path: Path,
) -> Dict[str, Any]:
    _parse_manifest_version(data.get("version"), manifest_path)
    _ensure_known_fields(
        data,
        field_name="manifest root",
        allowed_fields={"version", "repo", "tasks"},
    )
    repo_config = _ensure_mapping(data.get("repo"), "repo")
    _ensure_known_fields(
        repo_config,
        field_name="repo",
        allowed_fields=ALLOWED_REPO_FIELDS,
    )
    for repo_field in ("name", "output_root", "submission_prefix"):
        _ensure_stringish(repo_config.get(repo_field), f"repo.{repo_field}")

    tasks = _ensure_mapping(data.get("tasks"), "tasks")
    for task_name, task_cfg in tasks.items():
        task_key = str(task_name).strip()
        if not task_key:
            raise ValueError("Task names may not be empty")
        _validate_task_config(task_key, task_cfg)

    return {
        "version": SUPPORTED_MANIFEST_VERSION,
        "repo": repo_config,
        "tasks": tasks,
    }
