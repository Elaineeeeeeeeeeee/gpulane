import argparse
import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from .config import TERMINAL_STATES
from .manifest import load_repo_context, load_yaml, normalize_args
from .models import BatchJobSpec, TaskSpec
from .queue.scheduler import _handle_queue_on_submit
from .ray_client import get_job_submission_client
from .resolve import build_submit_payload, resolve_task_cwd, slugify
from .settings import get_queue_settings
from .summary import write_summary


def _batch_override_dir() -> Path:
    preferred = get_queue_settings().state_dir / "batch-overrides"
    try:
        preferred.mkdir(parents=True, exist_ok=True)
        if os.access(preferred, os.W_OK):
            return preferred
    except OSError:
        pass
    fallback = Path(tempfile.gettempdir()) / "gpulane" / "batch-overrides"
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback


def build_batch_index(
    *,
    batch_file: str,
    defaults: Dict[str, Any],
    jobs: List[Dict[str, Any]],
    started_at: str,
    finished_at: Optional[str] = None,
) -> Dict[str, Any]:
    counts = {
        "pending": 0,
        "submitted": 0,
        "running": 0,
        "succeeded": 0,
        "failed": 0,
        "stopped": 0,
        "other_terminal": 0,
    }
    for job in jobs:
        status = str(job.get("status", "pending")).upper()
        if status == "PENDING":
            counts["pending"] += 1
        elif status in {"SUBMITTED"}:
            counts["submitted"] += 1
        elif status in {"RUNNING", "PENDING_NODE_ASSIGNMENT"}:
            counts["running"] += 1
        elif status == "SUCCEEDED":
            counts["succeeded"] += 1
        elif status == "FAILED":
            counts["failed"] += 1
        elif status == "STOPPED":
            counts["stopped"] += 1
        elif status in TERMINAL_STATES:
            counts["other_terminal"] += 1
        else:
            counts["running"] += 1
    return {
        "batch_file": batch_file,
        "started_at": started_at,
        "finished_at": finished_at,
        "defaults": defaults,
        "counts": counts,
        "jobs": jobs,
    }


def write_batch_index(index_path: Optional[Path], payload: Dict[str, Any]) -> None:
    if index_path is None:
        return
    from .summary import write_json
    write_json(index_path, payload)
    print(f"Wrote batch index to: {index_path}")


def build_batch_job_record(spec: BatchJobSpec, payload: Dict[str, Any], position: int) -> Dict[str, Any]:
    return {
        "position": position,
        "task": spec.task,
        "name": spec.name,
        "overrides": spec.overrides,
        "manifest": spec.manifest,
        "repo_root": spec.repo_root,
        "summary_path": spec.summary_path,
        "status": "PENDING",
        "job_id": None,
        "submission_id": payload.get("submission_id"),
        "payload": payload,
    }


def make_submit_args_for_spec(spec: BatchJobSpec, address: str, dry_run: bool) -> argparse.Namespace:
    return argparse.Namespace(
        manifest=spec.manifest,
        repo_root=spec.repo_root,
        task=spec.task,
        extra_args=list(spec.extra_args),
        address=address,
        name=spec.name,
        submission_prefix=spec.submission_prefix,
        dry_run=dry_run,
        wait=False,
        poll_interval=get_queue_settings().job_status_poll_interval_s,
        log_path=None,
        summary_path=spec.summary_path,
        print_logs=False,
    )


def _task_cfg_map(task_cfg: Any) -> Dict[str, Any]:
    if isinstance(task_cfg, TaskSpec):
        return task_cfg.config
    return task_cfg


def _resolve_batch_config_arg(task_cfg: Any) -> str:
    task_cfg = _task_cfg_map(task_cfg)
    return str(task_cfg.get("batch_config_arg", "--config"))


def _find_last_flag_value(args: List[str], flag: str) -> Optional[str]:
    resolved: Optional[str] = None
    i = 0
    while i < len(args):
        if args[i] == flag and i + 1 < len(args):
            resolved = args[i + 1]
            i += 2
            continue
        i += 1
    return resolved


def _replace_or_append_flag(args: List[str], flag: str, value: str) -> List[str]:
    updated = list(args)
    replaced = False
    i = 0
    while i < len(updated):
        if updated[i] == flag and i + 1 < len(updated):
            updated[i + 1] = value
            replaced = True
            i += 2
            continue
        i += 1
    if replaced:
        return updated
    if updated[:1] == ["--"]:
        return ["--", flag, value, *updated[1:]]
    return [flag, value, *updated]


def _set_nested_override(target: Dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = [part.strip() for part in str(dotted_key).split(".") if part.strip()]
    if not parts:
        raise ValueError(f"Invalid override key: {dotted_key!r}")
    current = target
    for part in parts[:-1]:
        next_value = current.get(part)
        if next_value is None:
            current[part] = {}
            next_value = current[part]
        if not isinstance(next_value, dict):
            raise ValueError(
                f"Override path {dotted_key!r} conflicts at {part!r}: expected a mapping."
            )
        current = next_value
    current[parts[-1]] = value


def _materialize_batch_overrides(
    *,
    repo_context: Any,
    task_cfg: Any,
    spec: BatchJobSpec,
) -> List[str]:
    if not spec.overrides:
        return list(spec.extra_args)

    task_cfg = _task_cfg_map(task_cfg)
    config_arg = _resolve_batch_config_arg(task_cfg)
    default_args = normalize_args(task_cfg.get("default_args"), "default_args")
    config_value = _find_last_flag_value(default_args + list(spec.extra_args), config_arg)
    if not config_value:
        raise ValueError(
            f"Batch overrides require task {spec.task!r} to declare a config flag {config_arg!r}."
        )

    cwd = resolve_task_cwd(repo_context, task_cfg)
    config_path = Path(config_value)
    if not config_path.is_absolute():
        config_path = (cwd / config_path).resolve()
    if not config_path.exists():
        raise FileNotFoundError(f"Config file for batch overrides does not exist: {config_path}")

    config_data = load_yaml(config_path)
    merged_config = dict(config_data)
    for key, value in spec.overrides.items():
        _set_nested_override(merged_config, str(key), value)

    output_dir = _batch_override_dir()
    label = slugify(spec.name or spec.task)
    output_path = output_dir / f"{label}-{datetime.now().strftime('%Y%m%d-%H%M%S-%f')}.yaml"
    output_path.write_text(
        yaml.safe_dump(merged_config, sort_keys=False),
        encoding="utf-8",
    )
    return _replace_or_append_flag(list(spec.extra_args), config_arg, str(output_path))


def submit_batch_job(
    spec: BatchJobSpec,
    *,
    address: str,
    dry_run: bool,
) -> Dict[str, Any]:
    submit_args = make_submit_args_for_spec(spec, address=address, dry_run=dry_run)
    repo_context = load_repo_context(submit_args.manifest, submit_args.repo_root)
    if hasattr(repo_context, "require_task_spec"):
        task_spec = repo_context.require_task_spec(submit_args.task)
        task_cfg = task_spec
    else:
        if submit_args.task not in repo_context.tasks:
            raise ValueError(
                f"Unknown task '{submit_args.task}' in manifest {repo_context.manifest_path}"
            )
        task_cfg = repo_context.tasks[submit_args.task]
    effective_extra_args = _materialize_batch_overrides(
        repo_context=repo_context,
        task_cfg=task_cfg,
        spec=spec,
    )
    submit_args.extra_args = effective_extra_args

    if not dry_run and _handle_queue_on_submit(submit_args, task_cfg):
        payload = build_submit_payload(
            repo_context=repo_context,
            task_name=submit_args.task,
            task_cfg=task_cfg,
            cli_extra_args=effective_extra_args,
            address=submit_args.address,
            name_override=submit_args.name,
            submission_prefix_override=submit_args.submission_prefix,
        )
        write_summary(
            submit_args.summary_path,
            job_id=None,
            job_status="DEFERRED",
            payload=payload,
            log_path=None,
        )
        print(f"Deferred job: {payload['submission_id']}")
        return {
            "job_id": None,
            "status": "DEFERRED",
            "payload": payload,
        }

    payload = build_submit_payload(
        repo_context=repo_context,
        task_name=submit_args.task,
        task_cfg=task_cfg,
        cli_extra_args=effective_extra_args,
        address=submit_args.address,
        name_override=submit_args.name,
        submission_prefix_override=submit_args.submission_prefix,
    )
    print(json.dumps(payload, indent=2))

    if dry_run:
        return {
            "job_id": None,
            "status": "DRY_RUN",
            "payload": payload,
        }

    output_dir = payload.get("output_dir")
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    client = get_job_submission_client(submit_args.address)
    job_id = client.submit_job(
        entrypoint=payload["entrypoint"],
        submission_id=payload["submission_id"],
        runtime_env=payload["runtime_env"] or None,
        metadata=payload["metadata"] or None,
        entrypoint_num_cpus=payload["entrypoint_resources"]["cpu"],
        entrypoint_num_gpus=payload["entrypoint_resources"]["gpu"],
        entrypoint_memory=payload["entrypoint_resources"].get("memory"),
        entrypoint_resources=payload["entrypoint_resources"].get("custom"),
    )
    current_status = str(client.get_job_status(job_id))
    write_summary(
        submit_args.summary_path,
        job_id=job_id,
        job_status=current_status,
        payload=payload,
        log_path=None,
    )
    print(f"Submitted job: {job_id}")
    return {
        "job_id": job_id,
        "status": current_status,
        "payload": payload,
    }


def poll_batch_jobs(
    active_jobs: List[Dict[str, Any]],
    *,
    client_by_address: Dict[str, Any],
) -> bool:
    any_finished = False
    for job in active_jobs:
        job_id = job.get("job_id")
        if not job_id:
            continue
        address = str(job["payload"]["address"])
        client = client_by_address.setdefault(address, get_job_submission_client(address))
        status = str(client.get_job_status(job_id))
        if status != job.get("status"):
            job["status"] = status
        if status in TERMINAL_STATES:
            any_finished = True
    return any_finished
