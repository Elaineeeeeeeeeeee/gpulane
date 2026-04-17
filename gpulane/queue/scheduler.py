import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from ..config import (
    TERMINAL_STATES,
)
from ..manifest import load_repo_context
from ..models import DeferredJob
from ..ray_client import get_active_ray_jobs, get_job_submission_client, get_ray_available_resources
from ..resolve import resolve_priority, resolve_resources, timestamp
from ..settings import get_default_priority_label, get_priority_levels, get_queue_settings
from .backend import (
    _kafka_producer as _backend_kafka_producer,
    _publish_event,
    load_deferred,
    publish_cancel_request,
    save_deferred,
)


def _job_gpu_demand(job_info: Dict[str, Any]) -> float:
    requested_resources = job_info.get("requested_resources") or {}
    try:
        gpu = float(requested_resources.get("gpu", 0) or 0)
        if gpu > 0:
            return gpu
    except Exception:
        pass

    meta = job_info.get("metadata") or {}
    try:
        metadata_resources = json.loads(meta.get("requested_resources_json", "{}"))
        gpu = float(metadata_resources.get("gpu", 0) or 0)
        if gpu > 0:
            return gpu
    except Exception:
        pass

    try:
        repo_context = load_repo_context(meta.get("manifest_path"), meta.get("repo_root"))
        if hasattr(repo_context, "require_task_spec") and meta.get("task"):
            task_cfg = repo_context.require_task_spec(str(meta.get("task")))
            return float(resolve_resources(task_cfg).get("gpu", 0) or 0)
        task_cfg = repo_context.tasks.get(meta.get("task"))
        if isinstance(task_cfg, dict):
            return float(resolve_resources(task_cfg).get("gpu", 0) or 0)
    except Exception:
        pass
    return 0.0


def _job_started_sort_key(job_info: Dict[str, Any]) -> str:
    started_at = str(job_info.get("started_at") or "")
    if started_at:
        return started_at
    return str(job_info.get("job_id") or "")


def _select_preemption_victims(
    lower_running: List[Dict[str, Any]],
    *,
    required_gpu: float,
    free_gpu: float,
) -> List[Dict[str, Any]]:
    priority_levels = get_priority_levels()
    deficit_gpu = max(required_gpu - free_gpu, 0.0)
    if deficit_gpu <= 0:
        return []

    ordered = sorted(
        lower_running,
        key=lambda job: (
            priority_levels.get(job["priority"], 99),
            _job_started_sort_key(job),
        ),
        reverse=True,
    )
    selected: List[Dict[str, Any]] = []
    reclaimed_gpu = 0.0
    for job in ordered:
        selected.append(job)
        reclaimed_gpu += _job_gpu_demand(job)
        if reclaimed_gpu >= deficit_gpu:
            break
    return selected


def _ordered_preemption_candidates(lower_running: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    priority_levels = get_priority_levels()
    return sorted(
        lower_running,
        key=lambda job: (
            priority_levels.get(job["priority"], 99),
            _job_started_sort_key(job),
        ),
        reverse=True,
    )


def _wait_for_available_gpu(
    address: str,
    *,
    required_gpu: float,
    timeout_s: Optional[float] = None,
    poll_interval_s: Optional[float] = None,
) -> float:
    settings = get_queue_settings()
    timeout_s = (
        settings.preempt_resource_wait_timeout_s
        if timeout_s is None
        else timeout_s
    )
    poll_interval_s = (
        settings.preempt_resource_wait_poll_interval_s
        if poll_interval_s is None
        else poll_interval_s
    )
    deadline = time.time() + timeout_s
    latest_free_gpu = 0.0
    while True:
        cluster_resources = get_ray_available_resources(address)
        latest_free_gpu = float(cluster_resources.get("GPU", 0) or 0)
        if latest_free_gpu >= required_gpu:
            return latest_free_gpu
        if time.time() >= deadline:
            return latest_free_gpu
        time.sleep(poll_interval_s)


def _watcher_pid_file() -> Path:
    return get_queue_settings().state_dir / "watcher.pid"


def _ensure_watcher_running(address: str) -> None:
    pid_file = _watcher_pid_file()
    if pid_file.exists():
        try:
            pid = int(pid_file.read_text().strip())
            os.kill(pid, 0)  # raises if process is gone
            return  # already running
        except (ProcessLookupError, ValueError):
            pid_file.unlink(missing_ok=True)

    proc = subprocess.Popen(
        [sys.executable, "-m", "gpulane", "queue", "watch", "--address", address],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(proc.pid))
    print(f"Started queue watcher (pid={proc.pid})")


def _defer_job(
    args: argparse.Namespace,
    priority: str,
    reason: str,
    original_job_id: Optional[str] = None,
    run_name: Optional[str] = None,
    resume_run_name_arg: Optional[str] = None,
    requested_resources: Optional[Dict[str, float]] = None,
) -> None:
    job = DeferredJob(
        priority=priority,
        address=args.address,
        task=args.task,
        manifest=args.manifest,
        repo_root=args.repo_root,
        extra_args=list(getattr(args, "extra_args", []) or []),
        name=getattr(args, "name", None),
        submission_prefix=getattr(args, "submission_prefix", None),
        summary_path=getattr(args, "summary_path", None),
        deferred_at=timestamp(),
        reason=reason,
        original_job_id=original_job_id,
        run_name=run_name,
        resume_run_name_arg=resume_run_name_arg,
        requested_resources=requested_resources,
    )
    deferred = _load_deferred_jobs()
    deferred.append(job)
    _save_deferred_jobs(deferred)


def _load_deferred_jobs() -> List[DeferredJob]:
    return load_deferred()


def _save_deferred_jobs(jobs: List[DeferredJob]) -> None:
    save_deferred(jobs)


def _publish_preempted_event(job_id: str, task: Any, priority: str) -> None:
    _publish_event("preempted", {"job_id": job_id, "task": task, "priority": priority})


def _kafka_producer() -> Any:
    return _backend_kafka_producer()


def _preempt_job(client: Any, job_info: Dict[str, Any], address: str) -> None:
    job_id = job_info["job_id"]
    meta = job_info["metadata"]
    priority = meta.get("priority", get_default_priority_label())
    extra_args = json.loads(meta.get("extra_args_json", "[]"))
    preempt_ns = argparse.Namespace(
        address=address,
        task=meta.get("task", ""),
        manifest=meta.get("manifest_path"),
        repo_root=meta.get("repo_root"),
        extra_args=extra_args,
        name=None,
        submission_prefix=None,
        summary_path=None,
    )
    requested_resources = dict(job_info.get("requested_resources") or {})
    if not requested_resources:
        try:
            requested_resources = json.loads(meta.get("requested_resources_json", "{}"))
        except Exception:
            requested_resources = {}
    _defer_job(
        preempt_ns,
        priority,
        reason="preempted",
        original_job_id=job_id,
        run_name=meta.get("run_name"),
        resume_run_name_arg=meta.get("resume_run_name_arg"),
        requested_resources=requested_resources,
    )

    publish_cancel_request(job_id, meta.get("task"), priority)

    try:
        client.stop_job(job_id)
        print(f"Stopping {job_id} (task={meta.get('task', '?')}, priority={priority}) ...")
        settings = get_queue_settings()
        max_polls = max(
            int(
                settings.preempt_stop_max_wait_s
                / settings.preempt_stop_poll_interval_s
            ),
            1,
        )
        for _ in range(max_polls):
            if str(client.get_job_status(job_id)) in TERMINAL_STATES:
                break
            time.sleep(settings.preempt_stop_poll_interval_s)
        else:
            print(
                "Warning: "
                f"{job_id} did not reach terminal state within "
                f"{settings.preempt_stop_max_wait_s:.0f}s, proceeding anyway"
            )
        print(f"Preempted: {job_id}")
        _publish_preempted_event(job_id, meta.get("task"), priority)
    except Exception as exc:
        print(f"Warning: failed to stop {job_id}: {exc}")


def _handle_queue_on_submit(args: argparse.Namespace, task_cfg: Dict[str, Any]) -> bool:
    """
    Applies priority queue logic before submitting.
    Returns True if the job was deferred (caller should skip submission).
    Side-effect: stops any lower-priority running jobs.
    """
    priority = resolve_priority(task_cfg)
    priority_levels = get_priority_levels()
    priority_level = priority_levels[priority]
    client = get_job_submission_client(args.address)
    active = get_active_ray_jobs(client)

    # Step 1: if there are enough free GPUs, submit directly — priorities are irrelevant.
    required_gpu = resolve_resources(task_cfg).get("gpu", 0)
    cluster_resources = get_ray_available_resources(args.address)
    free_gpu = float(cluster_resources.get("GPU", 0) or 0)
    if free_gpu >= required_gpu:
        return False

    # GPUs are full — now priority determines what happens next.

    # Step 2: a higher-priority job is holding the GPUs → defer self and wait.
    higher_active = [j for j in active if priority_levels.get(j["priority"], 99) < priority_level]
    if higher_active:
        _defer_job(
            args,
            priority,
            reason="queued",
            requested_resources=resolve_resources(task_cfg),
        )
        ids = ", ".join(j["job_id"] for j in higher_active)
        print(f"Deferred (GPUs full, higher-priority active: {ids}): {args.task}")
        _ensure_watcher_running(args.address)
        return True

    # Step 3: lower-priority jobs are holding the GPUs → preempt them.
    lower_running = [j for j in active if priority_levels.get(j["priority"], 99) > priority_level]
    victims: List[Dict[str, Any]] = []
    remaining_candidates = _ordered_preemption_candidates(lower_running)
    while remaining_candidates and free_gpu < required_gpu:
        batch = _select_preemption_victims(
            remaining_candidates,
            required_gpu=required_gpu,
            free_gpu=free_gpu,
        )
        if not batch:
            break
        for job_info in batch:
            victims.append(job_info)
            _preempt_job(client, job_info, args.address)
        remaining_ids = {job["job_id"] for job in batch}
        remaining_candidates = [job for job in remaining_candidates if job["job_id"] not in remaining_ids]
        free_gpu = _wait_for_available_gpu(args.address, required_gpu=required_gpu)

    if free_gpu >= required_gpu and victims:
        _ensure_watcher_running(args.address)
        return False

    _defer_job(
        args,
        priority,
        reason="queued",
        requested_resources=resolve_resources(task_cfg),
    )
    blocking_ids = ", ".join(j["job_id"] for j in active) or "unknown"
    print(f"Deferred (GPUs full, no preemptable lower-priority jobs: {blocking_ids}): {args.task}")
    _ensure_watcher_running(args.address)
    return True


def _resolve_deferred_requested_resources(job: DeferredJob) -> Dict[str, float]:
    if isinstance(job.requested_resources, dict) and job.requested_resources:
        try:
            return {str(key): float(value) for key, value in job.requested_resources.items()}
        except Exception:
            pass

    try:
        repo_context = load_repo_context(job.manifest, job.repo_root)
        if hasattr(repo_context, "require_task_spec"):
            return resolve_resources(repo_context.require_task_spec(job.task))
        task_cfg = repo_context.tasks.get(job.task)
        if isinstance(task_cfg, dict):
            return resolve_resources(task_cfg)
    except Exception:
        pass
    return {"cpu": 1.0, "gpu": 0.0}


def _has_sufficient_cluster_resources(required: Dict[str, float], available: Dict[str, Any]) -> bool:
    cpu_required = float(required.get("cpu", 0) or 0)
    gpu_required = float(required.get("gpu", 0) or 0)
    memory_required = float(required.get("memory", 0) or 0)

    if float(available.get("CPU", 0) or 0) < cpu_required:
        return False
    if float(available.get("GPU", 0) or 0) < gpu_required:
        return False
    if float(available.get("memory", 0) or 0) < memory_required:
        return False

    custom_required = required.get("custom") or {}
    if isinstance(custom_required, dict):
        for key, value in custom_required.items():
            if float(available.get(str(key), 0) or 0) < float(value or 0):
                return False
    return True


def process_queue(client: Any, submit_fn: Callable, dry_run: bool = False) -> None:
    """Resume deferred jobs when capacity is available and no higher-priority job blocks them."""
    deferred = _load_deferred_jobs()
    if not deferred:
        return

    active = get_active_ray_jobs(client)
    priority_levels = get_priority_levels()
    current_min_active_level = min(
        (priority_levels.get(j["priority"], 99) for j in active),
        default=99,
    )
    address = next((job.address for job in deferred if job.address), None)
    cluster_resources = get_ray_available_resources(address) if address else {}

    to_resume: List[DeferredJob] = []
    remaining: List[DeferredJob] = []
    for job in sorted(deferred, key=lambda j: (priority_levels.get(j.priority, 99), j.deferred_at)):
        job_level = priority_levels.get(job.priority, 99)
        if current_min_active_level < job_level:
            remaining.append(job)
            continue
        requested_resources = _resolve_deferred_requested_resources(job)
        if not _has_sufficient_cluster_resources(requested_resources, cluster_resources):
            remaining.append(job)
            continue
        to_resume.append(job)
        cluster_resources = dict(cluster_resources)
        cluster_resources["CPU"] = max(
            float(cluster_resources.get("CPU", 0) or 0) - float(requested_resources.get("cpu", 0) or 0),
            0.0,
        )
        cluster_resources["GPU"] = max(
            float(cluster_resources.get("GPU", 0) or 0) - float(requested_resources.get("gpu", 0) or 0),
            0.0,
        )
        if "memory" in requested_resources:
            cluster_resources["memory"] = max(
                float(cluster_resources.get("memory", 0) or 0) - float(requested_resources.get("memory", 0) or 0),
                0.0,
            )
        custom_required = requested_resources.get("custom") or {}
        if isinstance(custom_required, dict):
            for key, value in custom_required.items():
                cluster_resources[str(key)] = max(
                    float(cluster_resources.get(str(key), 0) or 0) - float(value or 0),
                    0.0,
                )
        current_min_active_level = min(current_min_active_level, job_level)

    if not to_resume:
        if deferred:
            print(f"Keeping {len(deferred)} deferred job(s) — waiting for capacity or higher-priority jobs to clear.")
        return

    print(f"Resuming {len(to_resume)} deferred job(s).")
    for job in to_resume:
        if dry_run:
            print(f"  [dry-run] would resubmit: {job.task} ({job.priority}, reason={job.reason})")
            continue
        extra_args = list(job.extra_args)
        if job.reason == "preempted" and "--resume" not in extra_args:
            extra_args.append("--resume")
        if (
            job.reason == "preempted"
            and job.run_name
            and job.resume_run_name_arg
            and job.resume_run_name_arg not in extra_args
        ):
            extra_args.extend([job.resume_run_name_arg, job.run_name])
        elif (
            job.reason == "preempted"
            and job.original_job_id
            and "--resume-job-id" not in extra_args
        ):
            extra_args.extend(["--resume-job-id", job.original_job_id])
        resume_args = argparse.Namespace(
            manifest=job.manifest,
            repo_root=job.repo_root,
            task=job.task,
            extra_args=extra_args,
            address=job.address,
            name=job.name,
            submission_prefix=job.submission_prefix,
            dry_run=False,
            wait=False,
            poll_interval=get_queue_settings().job_status_poll_interval_s,
            log_path=None,
            summary_path=job.summary_path,
            print_logs=False,
        )
        try:
            submit_fn(resume_args, _skip_queue=True)
            print(f"  Resumed: {job.task} ({job.priority}, was {job.reason} at {job.deferred_at})")
        except Exception as exc:
            print(f"  Failed to resume {job.task}: {exc}")
            remaining.append(job)

    _save_deferred_jobs(remaining)
