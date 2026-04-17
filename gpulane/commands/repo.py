import argparse
import getpass
import json

from ..git import get_git_metadata
from ..manifest import load_repo_context
from ..queue.backend import _kafka_producer, ensure_queue_backend_ready, get_queue_backend_name
from ..ray_client import get_job_submission_client
from ..resolve import (
    build_injected_env,
    build_submission_id,
    build_submit_payload,
    resolve_entrypoint,
    resolve_metadata,
    resolve_output_dir,
    resolve_resources,
    resolve_runtime_env,
    resolve_submission_prefix,
)
from ..settings import get_queue_settings


def run_list(args: argparse.Namespace) -> int:
    repo_context = load_repo_context(args.manifest, args.repo_root)
    for task_name in sorted(repo_context.tasks):
        task_spec = repo_context.require_task_spec(task_name)
        resources = resolve_resources(task_spec)
        suffix = f" - {task_spec.description}" if task_spec.description else ""
        print(
            f"{task_name}: cpu={resources['cpu']} gpu={resources['gpu']}"
            f"{suffix}"
        )
    return 0


def run_validate(args: argparse.Namespace) -> int:
    repo_context = load_repo_context(args.manifest, args.repo_root)
    git_metadata = get_git_metadata(repo_context.repo_root)
    submitted_by = getpass.getuser()

    for task_name in sorted(repo_context.tasks):
        task_spec = repo_context.require_task_spec(task_name)
        submission_id = build_submission_id(
            repo_context.repo_name,
            task_name,
            None,
            submission_prefix=resolve_submission_prefix(repo_context, None),
        )
        output_dir = resolve_output_dir(
            repo_context, task_name, submission_id, submitted_by
        )
        resolve_entrypoint(repo_context, task_spec, [])
        resolve_resources(task_spec)
        resolve_runtime_env(task_spec)
        resolve_metadata(
            repo_context, task_name, task_spec, submitted_by, git_metadata
        )
        build_injected_env(
            repo_context=repo_context,
            task_name=task_name,
            submission_id=submission_id,
            submitted_by=submitted_by,
            git_metadata=git_metadata,
            output_dir=output_dir,
            task_cfg=task_spec,
        )
    print(f"Validated {len(repo_context.tasks)} task(s) in {repo_context.manifest_path}")
    return 0


def run_doctor(args: argparse.Namespace) -> int:
    run_validate(args)
    client = get_job_submission_client(args.address)
    version = client.get_version()
    print(f"Ray Jobs endpoint OK: {args.address} (version={version})")
    queue_settings = get_queue_settings()
    queue_backend = get_queue_backend_name()
    queue_backend_summary = ensure_queue_backend_ready()
    if queue_backend == "redis":
        print(f"Redis OK: {queue_settings.redis_url}")
    else:
        print(f"Queue backend OK: {queue_backend_summary}")
    if queue_settings.kafka_brokers:
        producer = _kafka_producer()
        print(f"Kafka OK: {queue_settings.kafka_brokers}")
        if producer is not None:
            try:
                producer.close(timeout=queue_settings.kafka_operation_timeout_s)
            except Exception:
                pass
    if not args.task:
        return 0

    repo_context = load_repo_context(args.manifest, args.repo_root)
    task_spec = repo_context.require_task_spec(args.task)

    payload = build_submit_payload(
        repo_context=repo_context,
        task_name=args.task,
        task_cfg=task_spec,
        cli_extra_args=args.extra_args,
        address=args.address,
        name_override=args.name,
        submission_prefix_override=args.submission_prefix,
    )
    print("Resolved submit payload:")
    print(json.dumps(payload, indent=2))
    return 0
