import argparse
import json
from pathlib import Path
from typing import Callable

from ..manifest import load_repo_context
from ..queue.scheduler import _handle_queue_on_submit, process_queue
from ..ray_client import get_job_submission_client, wait_for_job
from ..resolve import build_submit_payload
from ..summary import write_logs_and_summary, write_summary


def run_submit(
    args: argparse.Namespace,
    *,
    skip_queue: bool = False,
    submit_again: Callable[[argparse.Namespace, bool], int],
) -> int:
    if (args.log_path or args.print_logs) and not args.wait:
        raise SystemExit("--log-path and --print-logs require --wait")

    repo_context = load_repo_context(args.manifest, args.repo_root)
    if hasattr(repo_context, "require_task_spec"):
        task_spec = repo_context.require_task_spec(args.task)
        task_cfg = task_spec
    elif hasattr(repo_context, "require_task"):
        task_cfg = repo_context.require_task(args.task)
    else:
        if args.task not in repo_context.tasks:
            raise ValueError(
                f"Unknown task '{args.task}' in manifest {repo_context.manifest_path}"
            )
        task_cfg = repo_context.tasks[args.task]

    if not skip_queue and not getattr(args, "dry_run", False):
        if _handle_queue_on_submit(args, task_cfg):
            return 0

    payload = build_submit_payload(
        repo_context=repo_context,
        task_name=args.task,
        task_cfg=task_cfg,
        cli_extra_args=args.extra_args,
        address=args.address,
        name_override=args.name,
        submission_prefix_override=args.submission_prefix,
    )
    print(json.dumps(payload, indent=2))

    if args.dry_run:
        return 0

    output_dir = payload.get("output_dir")
    if output_dir:
        Path(output_dir).mkdir(parents=True, exist_ok=True)

    client = get_job_submission_client(args.address)
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
        args.summary_path,
        job_id=job_id,
        job_status=current_status,
        payload=payload,
        log_path=None,
    )

    print(f"Submitted job: {job_id}")
    print(f"Status command: gpulane status --address {args.address} --job-id {job_id}")
    print(f"Logs command:   gpulane logs --address {args.address} --job-id {job_id}")
    print(f"Stop command:   ray job stop --address {args.address} {job_id}")

    if not args.wait:
        return 0

    final_status = wait_for_job(client, job_id, args.poll_interval)
    print(f"Final status: {final_status}")
    write_logs_and_summary(
        client=client,
        job_id=job_id,
        final_status=final_status,
        payload=payload,
        log_path_value=args.log_path,
        summary_path_value=args.summary_path,
        print_logs=args.print_logs,
    )
    if not skip_queue:
        process_queue(client, submit_again)
    return 0 if final_status == "SUCCEEDED" else 1
