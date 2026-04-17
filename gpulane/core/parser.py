import argparse
from pathlib import Path

from ..settings import get_queue_settings, get_ray_settings


def add_repo_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--manifest",
        default=None,
        help="Explicit path to the repo-local ray_tasks.yaml manifest",
    )
    parser.add_argument(
        "--repo-root",
        default=None,
        help="Explicit repo root. Defaults to the manifest directory.",
    )


def add_init_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--path",
        default="ray_tasks.yaml",
        help="Output path for the generated manifest (default: ray_tasks.yaml)",
    )
    parser.add_argument(
        "--repo-name",
        default=None,
        help="Optional repo name to store under repo.name",
    )
    parser.add_argument(
        "--task-name",
        default="example.train",
        help="Starter task key to create in the manifest",
    )
    parser.add_argument(
        "--cwd",
        default=".",
        help="Relative working directory for the starter task",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing manifest path",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Optional command for the starter task after '--'",
    )


def add_address_argument(parser: argparse.ArgumentParser) -> None:
    ray_settings = get_ray_settings()
    parser.add_argument(
        "--address",
        default=ray_settings.default_address,
        help=f"Ray Jobs API address (default: {ray_settings.default_address})",
    )


def add_task_preview_arguments(parser: argparse.ArgumentParser, *, required: bool) -> None:
    parser.add_argument(
        "--task",
        required=required,
        default=None,
        help="Task key from ray_tasks.yaml",
    )
    parser.add_argument(
        "--name",
        default=None,
        help="Optional human-readable name override",
    )
    parser.add_argument(
        "--submission-prefix",
        default=None,
        help="Optional prefix prepended to the generated submission id",
    )
    parser.add_argument(
        "extra_args",
        nargs=argparse.REMAINDER,
        help="Arguments appended to the task command after '--'",
    )


def add_submit_arguments(parser: argparse.ArgumentParser) -> None:
    ray_settings = get_ray_settings()
    add_repo_arguments(parser)
    add_address_argument(parser)
    add_task_preview_arguments(parser, required=True)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved submission payload without submitting",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for the job to finish",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=ray_settings.job_status_poll_interval_s,
        help=(
            "Polling interval in seconds while waiting "
            f"(default: {ray_settings.job_status_poll_interval_s:g})"
        ),
    )
    parser.add_argument(
        "--log-path",
        default=None,
        help="Optional file path to store final logs when --wait is used",
    )
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Optional JSON summary path written after submit and refreshed on completion",
    )
    parser.add_argument(
        "--print-logs",
        action="store_true",
        help="Print final logs after the job completes",
    )


def add_batch_submit_arguments(parser: argparse.ArgumentParser) -> None:
    ray_settings = get_ray_settings()
    parser.add_argument(
        "--file",
        required=True,
        help="YAML file describing a batch of jobs to submit",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the resolved batch plan without submitting",
    )
    parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait for the entire batch to finish",
    )
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=ray_settings.job_status_poll_interval_s,
        help=(
            "Polling interval in seconds while waiting "
            f"(default: {ray_settings.job_status_poll_interval_s:g})"
        ),
    )


def add_watch_arguments(parser: argparse.ArgumentParser) -> None:
    ray_settings = get_ray_settings()
    add_address_argument(parser)
    parser.add_argument("--job-id", required=True, help="Existing Ray job id")
    parser.add_argument(
        "--poll-interval",
        type=float,
        default=ray_settings.job_status_poll_interval_s,
        help=(
            "Polling interval in seconds while waiting "
            f"(default: {ray_settings.job_status_poll_interval_s:g})"
        ),
    )
    parser.add_argument("--log-path", default=None, help="Optional file path for final logs")
    parser.add_argument(
        "--summary-path",
        default=None,
        help="Optional JSON summary path for the watched job",
    )
    parser.add_argument(
        "--print-logs",
        action="store_true",
        help="Print final logs after the job completes",
    )


def add_logs_arguments(parser: argparse.ArgumentParser) -> None:
    add_address_argument(parser)
    parser.add_argument("--job-id", required=True, help="Existing Ray job id")


def build_parser() -> argparse.ArgumentParser:
    queue_settings = get_queue_settings()
    ray_settings = get_ray_settings()
    parser = argparse.ArgumentParser(
        description="Shared-workstation Ray CLI for multi-repo training tasks."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List tasks in the current repo manifest")
    add_repo_arguments(list_parser)

    init_parser = subparsers.add_parser(
        "init",
        help="Generate a starter ray_tasks.yaml manifest",
    )
    add_init_arguments(init_parser)

    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate the current repo manifest and task definitions",
    )
    add_repo_arguments(validate_parser)

    doctor_parser = subparsers.add_parser(
        "doctor",
        help="Validate the manifest and connectivity to the Ray Jobs endpoint",
    )
    add_repo_arguments(doctor_parser)
    add_address_argument(doctor_parser)
    add_task_preview_arguments(doctor_parser, required=False)

    submit_parser = subparsers.add_parser("submit", help="Submit a task to the shared Ray cluster")
    add_submit_arguments(submit_parser)

    setup_parser = subparsers.add_parser(
        "setup-stack",
        help="Ensure Redis is reachable and write per-user gpulane shell env",
    )
    setup_parser.add_argument(
        "--ray-address",
        default=ray_settings.default_address,
        help=f"Ray Jobs API address (default: {ray_settings.default_address})",
    )
    setup_parser.add_argument(
        "--redis-url",
        default=queue_settings.redis_url,
        help=f"Redis URL for the shared queue backend (default: {queue_settings.redis_url})",
    )
    setup_parser.add_argument(
        "--env-file",
        default=str(Path.home() / ".gpulane" / "env.sh"),
        help="File to write shell exports to",
    )
    setup_parser.add_argument(
        "--shell-rc",
        default=str(Path.home() / ".bashrc"),
        help="Shell rc file to source the env file from",
    )
    setup_parser.add_argument(
        "--no-shell-rc",
        action="store_true",
        help="Do not modify the shell rc file",
    )
    setup_parser.add_argument(
        "--skip-redis-start",
        action="store_true",
        help="Only verify Redis; do not try to start a local redis-server",
    )

    batch_parser = subparsers.add_parser("batch", help="Batch operations for multi-job workflows")
    batch_subparsers = batch_parser.add_subparsers(dest="batch_command", required=True)
    batch_submit_parser = batch_subparsers.add_parser("submit", help="Submit a batch of jobs")
    add_batch_submit_arguments(batch_submit_parser)

    queue_parser = subparsers.add_parser("queue", help="Manage the priority job queue")
    queue_subparsers = queue_parser.add_subparsers(dest="queue_command", required=True)

    queue_subparsers.add_parser("list", help="Show deferred jobs")

    queue_process_parser = queue_subparsers.add_parser(
        "process",
        help="Resume deferred jobs if no prod jobs are active",
    )
    add_address_argument(queue_process_parser)
    queue_process_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be resumed without submitting",
    )

    queue_watch_parser = queue_subparsers.add_parser(
        "watch",
        help="Poll Ray and resume deferred jobs when prod finishes",
    )
    add_address_argument(queue_watch_parser)
    queue_watch_parser.add_argument(
        "--poll-interval",
        type=float,
        default=queue_settings.watch_poll_interval_s,
        help=f"Polling interval in seconds (default: {queue_settings.watch_poll_interval_s:g})",
    )

    watch_parser = subparsers.add_parser("watch", help="Wait for an existing Ray job id")
    add_watch_arguments(watch_parser)

    logs_parser = subparsers.add_parser("logs", help="Print logs for an existing Ray job id")
    add_logs_arguments(logs_parser)

    status_parser = subparsers.add_parser("status", help="Print status for an existing Ray job id")
    add_logs_arguments(status_parser)

    return parser
