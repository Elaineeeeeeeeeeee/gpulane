import getpass
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from ..git import detect_github_submission_prefix, get_git_metadata
from ..manifest import normalize_mapping
from ..models import RepoContext, SubmissionPayload, TaskSpec
from ..resolve import (
    build_submission_id as resolve_build_submission_id,
    resolve_entrypoint,
    resolve_entrypoint_resources,
    resolve_priority,
    resolve_resources,
    resolve_runtime_env,
    resolve_task_env,
    slugify,
)


def resolve_submission_prefix(
    repo_context: RepoContext,
    cli_override: Optional[str],
) -> Optional[str]:
    raw_value = (
        cli_override
        if cli_override is not None
        else repo_context.repo_config.get("submission_prefix")
    )
    if raw_value is None:
        return detect_github_submission_prefix(repo_context.repo_root)
    submission_prefix = str(raw_value).strip()
    return submission_prefix or detect_github_submission_prefix(repo_context.repo_root)


def resolve_output_dir(
    repo_context: RepoContext,
    task_name: str,
    submission_id: str,
    submitted_by: str,
) -> Optional[Path]:
    output_root = repo_context.repo_config.get("output_root")
    if not output_root:
        return None

    root = Path(str(output_root)).expanduser()
    if not root.is_absolute():
        root = (repo_context.repo_root / root).resolve()
    return (
        root
        / slugify(submitted_by)
        / slugify(repo_context.repo_name)
        / slugify(task_name)
        / submission_id
    )


def resolve_resume_run_name_arg(task_cfg: Dict[str, Any]) -> Optional[str]:
    if isinstance(task_cfg, TaskSpec):
        task_cfg = task_cfg.config
    value = task_cfg.get("resume_run_name_arg")
    if value is None:
        return None
    resolved = str(value).strip()
    return resolved or None


def inject_run_name_arg(
    cli_extra_args: List[str],
    task_cfg: Union[Dict[str, Any], TaskSpec],
    run_name: str,
) -> List[str]:
    effective_args = list(cli_extra_args)
    run_name_arg = resolve_resume_run_name_arg(task_cfg)
    if not run_name_arg or run_name_arg in effective_args:
        return effective_args
    effective_args.extend([run_name_arg, run_name])
    return effective_args


def resolve_metadata(
    repo_context: RepoContext,
    task_name: str,
    task_cfg: Union[Dict[str, Any], TaskSpec],
    submitted_by: str,
    git_metadata: Dict[str, str],
    cli_extra_args: Optional[List[str]] = None,
    run_name: Optional[str] = None,
) -> Dict[str, str]:
    if isinstance(task_cfg, TaskSpec):
        task_cfg = task_cfg.config
    declared_resources = resolve_resources(task_cfg)
    entrypoint_resources = resolve_entrypoint_resources(task_cfg)
    metadata = normalize_mapping(task_cfg.get("metadata"), "metadata")
    metadata.update(
        {
            "repo_name": repo_context.repo_name,
            "repo_root": str(repo_context.repo_root),
            "manifest_path": str(repo_context.manifest_path),
            "task": task_name,
            "kind": str(task_cfg.get("kind", "job")),
            "priority": resolve_priority(task_cfg),
            "submitted_by": submitted_by,
            "extra_args_json": json.dumps(cli_extra_args or []),
            "requested_resources_json": json.dumps(declared_resources, sort_keys=True),
            "entrypoint_resources_json": json.dumps(
                entrypoint_resources, sort_keys=True
            ),
            **git_metadata,
        }
    )
    if run_name:
        metadata["run_name"] = run_name
    run_name_arg = resolve_resume_run_name_arg(task_cfg)
    if run_name_arg:
        metadata["resume_run_name_arg"] = run_name_arg
    return {str(key): str(value) for key, value in metadata.items()}


def build_injected_env(
    repo_context: RepoContext,
    task_name: str,
    submission_id: str,
    submitted_by: str,
    git_metadata: Dict[str, str],
    output_dir: Optional[Path],
    task_cfg: Union[Dict[str, Any], TaskSpec],
) -> Dict[str, str]:
    env_vars = resolve_task_env(task_cfg)
    env_vars.update(
        {
            "GPULANE_JOB_ID": submission_id,
            "GPULANE_TASK": task_name,
            "GPULANE_REPO_NAME": repo_context.repo_name,
            "GPULANE_REPO_ROOT": str(repo_context.repo_root),
            "GPULANE_MANIFEST_PATH": str(repo_context.manifest_path),
            "GPULANE_SUBMITTED_BY": submitted_by,
            "GPULANE_GIT_BRANCH": git_metadata["git_branch"],
            "GPULANE_GIT_COMMIT": git_metadata["git_commit"],
            "GPULANE_GIT_DIRTY": git_metadata["git_dirty"],
        }
    )
    if output_dir is not None:
        env_vars["GPULANE_OUTPUT_DIR"] = str(output_dir)
    return env_vars


def build_submit_payload_model(
    repo_context: RepoContext,
    task_name: str,
    task_cfg: Union[Dict[str, Any], TaskSpec],
    cli_extra_args: List[str],
    address: str,
    name_override: Optional[str],
    submission_prefix_override: Optional[str] = None,
) -> SubmissionPayload:
    if isinstance(task_cfg, TaskSpec):
        task_cfg = task_cfg.config
    submitted_by = getpass.getuser()
    git_metadata = get_git_metadata(repo_context.repo_root)
    submission_prefix = resolve_submission_prefix(
        repo_context, submission_prefix_override
    )
    submission_id = resolve_build_submission_id(
        repo_context.repo_name,
        task_name,
        name_override,
        submission_prefix=submission_prefix,
    )
    effective_cli_extra_args = inject_run_name_arg(
        cli_extra_args, task_cfg, submission_id
    )
    output_dir = resolve_output_dir(
        repo_context, task_name, submission_id, submitted_by
    )

    cwd, entrypoint = resolve_entrypoint(
        repo_context, task_cfg, effective_cli_extra_args
    )
    resources = resolve_resources(task_cfg)
    entrypoint_resources = resolve_entrypoint_resources(task_cfg)
    run_name_arg = resolve_resume_run_name_arg(task_cfg)
    metadata = resolve_metadata(
        repo_context,
        task_name,
        task_cfg,
        submitted_by,
        git_metadata,
        effective_cli_extra_args,
        run_name=submission_id if run_name_arg else None,
    )
    runtime_env = resolve_runtime_env(task_cfg)
    runtime_env["env_vars"] = dict(runtime_env.get("env_vars") or {})
    runtime_env["env_vars"].update(
        build_injected_env(
            repo_context=repo_context,
            task_name=task_name,
            submission_id=submission_id,
            submitted_by=submitted_by,
            git_metadata=git_metadata,
            output_dir=output_dir,
            task_cfg=task_cfg,
        )
    )

    return SubmissionPayload(
        address=address,
        submission_id=submission_id,
        repo_name=repo_context.repo_name,
        repo_root=str(repo_context.repo_root),
        manifest_path=str(repo_context.manifest_path),
        task=task_name,
        kind=str(task_cfg.get("kind", "job")),
        cwd=str(cwd),
        entrypoint=entrypoint,
        resources=resources,
        entrypoint_resources=entrypoint_resources,
        runtime_env=runtime_env,
        metadata=metadata,
        output_dir=str(output_dir) if output_dir else None,
    )


def build_submit_payload(
    repo_context: RepoContext,
    task_name: str,
    task_cfg: Union[Dict[str, Any], TaskSpec],
    cli_extra_args: List[str],
    address: str,
    name_override: Optional[str],
    submission_prefix_override: Optional[str] = None,
) -> Dict[str, Any]:
    payload = build_submit_payload_model(
        repo_context=repo_context,
        task_name=task_name,
        task_cfg=task_cfg,
        cli_extra_args=cli_extra_args,
        address=address,
        name_override=name_override,
        submission_prefix_override=submission_prefix_override,
    )
    return payload.to_dict()
