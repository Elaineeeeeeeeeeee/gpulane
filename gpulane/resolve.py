import re
import shlex
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .manifest import normalize_args, normalize_env_vars, normalize_mapping
from .models import RepoContext, ResourceSpec, TaskSpec
from .settings import get_default_priority_label, get_priority_levels


def slugify(value: str) -> str:
    lowered = value.lower()
    normalized = re.sub(r"[^a-z0-9]+", "-", lowered)
    return normalized.strip("-") or "job"


def timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def build_submission_id(
    repo_name: str,
    task_name: str,
    name_override: Optional[str],
    submission_prefix: Optional[str] = None,
) -> str:
    name = name_override or f"{repo_name}-{task_name}"
    if submission_prefix:
        name = f"{submission_prefix}-{name}"
    return f"{slugify(name)}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"


def _resolve_resource_spec(
    raw_resources: Dict[str, Any],
    *,
    field_name: str,
) -> ResourceSpec:
    resources = normalize_mapping(raw_resources, field_name)
    cpu = float(resources.get("cpu", 1))
    gpu = float(resources.get("gpu", 0))
    if cpu < 0 or gpu < 0:
        raise ValueError(f"{field_name}.cpu and {field_name}.gpu must be non-negative")

    memory = int(resources["memory"]) if "memory" in resources else None
    custom = None
    if "custom" in resources:
        custom_mapping = normalize_mapping(resources["custom"], f"{field_name}.custom")
        custom = {str(key): float(value) for key, value in custom_mapping.items()}
    return ResourceSpec(
        cpu=cpu,
        gpu=gpu,
        memory=memory,
        custom=custom,
    )


def _task_config(task_cfg: Union[Dict[str, Any], TaskSpec]) -> Dict[str, Any]:
    if isinstance(task_cfg, TaskSpec):
        return task_cfg.config
    return task_cfg


def resolve_resources(task_cfg: Union[Dict[str, Any], TaskSpec]) -> Dict[str, Any]:
    task_cfg = _task_config(task_cfg)
    return _resolve_resource_spec(
        task_cfg.get("resources"),
        field_name="resources",
    ).to_dict()


def resolve_entrypoint_resources(task_cfg: Union[Dict[str, Any], TaskSpec]) -> Dict[str, Any]:
    task_cfg = _task_config(task_cfg)
    raw_resources = task_cfg.get("entrypoint_resources")
    if raw_resources is None:
        return resolve_resources(task_cfg)
    return _resolve_resource_spec(
        raw_resources,
        field_name="entrypoint_resources",
    ).to_dict()


def resolve_priority(task_cfg: Union[Dict[str, Any], TaskSpec]) -> str:
    task_cfg = _task_config(task_cfg)
    priority_levels = get_priority_levels()
    priority = str(task_cfg.get("priority", get_default_priority_label())).strip()
    if priority not in priority_levels:
        raise ValueError(
            f"Unknown priority '{priority}'. Valid values: {', '.join(priority_levels)}"
        )
    return priority


def resolve_runtime_env(task_cfg: Union[Dict[str, Any], TaskSpec]) -> Dict[str, Any]:
    task_cfg = _task_config(task_cfg)
    runtime_env = normalize_mapping(task_cfg.get("runtime_env"), "runtime_env")
    merged_env_vars = normalize_env_vars(runtime_env.get("env_vars"), "runtime_env.env_vars")
    merged_env_vars.update(resolve_task_env(task_cfg))
    runtime_env["env_vars"] = merged_env_vars
    return {key: value for key, value in runtime_env.items() if value not in ({}, None)}


def resolve_task_env(task_cfg: Union[Dict[str, Any], TaskSpec]) -> Dict[str, str]:
    task_cfg = _task_config(task_cfg)
    return normalize_env_vars(task_cfg.get("env"), "env")


def resolve_task_cwd(repo_context: RepoContext, task_cfg: Union[Dict[str, Any], TaskSpec]) -> Path:
    task_cfg = _task_config(task_cfg)
    cwd_value = task_cfg.get("cwd")
    if not cwd_value:
        raise ValueError("Task must define 'cwd'")
    cwd_path = Path(str(cwd_value))
    if cwd_path.is_absolute():
        raise ValueError("Task cwd must be relative to the repo root")
    cwd = (repo_context.repo_root / cwd_path).resolve()
    try:
        cwd.relative_to(repo_context.repo_root)
    except ValueError as exc:
        raise ValueError(f"Task cwd must stay within the repo root: {cwd}") from exc
    if not cwd.exists():
        raise ValueError(f"Task cwd does not exist: {cwd}")
    return cwd


def resolve_entrypoint(
    repo_context: RepoContext,
    task_cfg: Union[Dict[str, Any], TaskSpec],
    cli_extra_args: List[str],
) -> Tuple[Path, str]:
    task_cfg = _task_config(task_cfg)
    cwd = resolve_task_cwd(repo_context, task_cfg)
    command = normalize_args(task_cfg.get("command"), "command")
    if not command:
        raise ValueError("Task must define a non-empty 'command'")

    default_args = normalize_args(task_cfg.get("default_args"), "default_args")
    if cli_extra_args[:1] == ["--"]:
        cli_extra_args = cli_extra_args[1:]
    full_command = command + default_args + cli_extra_args
    return cwd, f"cd {shlex.quote(str(cwd))} && {shlex.join(full_command)}"


from .core import submission as submission_core


def resolve_submission_prefix(
    repo_context: RepoContext,
    cli_override: Optional[str],
) -> Optional[str]:
    return submission_core.resolve_submission_prefix(repo_context, cli_override)


def resolve_output_dir(
    repo_context: RepoContext,
    task_name: str,
    submission_id: str,
    submitted_by: str,
) -> Optional[Path]:
    return submission_core.resolve_output_dir(
        repo_context, task_name, submission_id, submitted_by
    )


def resolve_resume_run_name_arg(task_cfg: Dict[str, Any]) -> Optional[str]:
    return submission_core.resolve_resume_run_name_arg(task_cfg)


def inject_run_name_arg(
    cli_extra_args: List[str],
    task_cfg: Dict[str, Any],
    run_name: str,
) -> List[str]:
    return submission_core.inject_run_name_arg(cli_extra_args, task_cfg, run_name)


def resolve_metadata(
    repo_context: RepoContext,
    task_name: str,
    task_cfg: Dict[str, Any],
    submitted_by: str,
    git_metadata: Dict[str, str],
    cli_extra_args: Optional[List[str]] = None,
    run_name: Optional[str] = None,
) -> Dict[str, str]:
    return submission_core.resolve_metadata(
        repo_context,
        task_name,
        task_cfg,
        submitted_by,
        git_metadata,
        cli_extra_args,
        run_name,
    )


def build_injected_env(
    repo_context: RepoContext,
    task_name: str,
    submission_id: str,
    submitted_by: str,
    git_metadata: Dict[str, str],
    output_dir: Optional[Path],
    task_cfg: Dict[str, Any],
) -> Dict[str, str]:
    return submission_core.build_injected_env(
        repo_context,
        task_name,
        submission_id,
        submitted_by,
        git_metadata,
        output_dir,
        task_cfg,
    )


def build_submit_payload(
    repo_context: RepoContext,
    task_name: str,
    task_cfg: Dict[str, Any],
    cli_extra_args: List[str],
    address: str,
    name_override: Optional[str],
    submission_prefix_override: Optional[str] = None,
) -> Dict[str, Any]:
    return submission_core.build_submit_payload(
        repo_context,
        task_name,
        task_cfg,
        cli_extra_args,
        address,
        name_override,
        submission_prefix_override,
    )
