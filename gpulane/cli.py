import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional

from .batch import (
    build_batch_index,
    build_batch_job_record,
    make_submit_args_for_spec,
    poll_batch_jobs,
    submit_batch_job,
    write_batch_index,
)
from .commands import repo as repo_commands
from .commands import init as init_commands
from .commands import jobs as job_commands
from .commands import submit as submit_commands
from .commands import batch as batch_commands
from .commands import queue as queue_commands
from .commands import setup as setup_commands
from .core.batch_config import (
    load_batch_config as core_load_batch_config,
    resolve_batch_defaults as core_resolve_batch_defaults,
    resolve_batch_job_specs as core_resolve_batch_job_specs,
)
from .core.parser import build_parser as core_build_parser
from .settings import get_queue_settings

_setup_parse_redis_host_port_impl = setup_commands._parse_redis_host_port
_setup_is_tcp_reachable_impl = setup_commands._is_tcp_reachable
_setup_write_setup_env_file_impl = setup_commands._write_setup_env_file
from .config import (
    KAFKA_OPERATION_TIMEOUT_S,
)
from .manifest import load_repo_context, find_manifest, load_yaml
from .models import BatchJobSpec, DeferredJob, RepoContext
from .queue.backend import (
    _kafka_producer,
    _redis_client,
    _redis_score,
    ensure_queue_backend_ready,
    get_queue_backend_name,
    load_deferred,
    save_deferred,
)
from .queue.scheduler import (
    _defer_job,
    _ensure_watcher_running,
    _handle_queue_on_submit,
    _preempt_job,
    _watcher_pid_file,
    process_queue,
)
from .ray_client import (
    get_active_ray_jobs,
    get_job_submission_client,
    get_ray_available_resources,
    wait_for_job,
    watch_job,
)
from .resolve import (
    build_injected_env,
    build_submission_id,
    build_submit_payload,
    resolve_entrypoint,
    resolve_metadata,
    resolve_output_dir,
    resolve_priority,
    resolve_resources,
    resolve_runtime_env,
    resolve_submission_prefix,
    resolve_task_cwd,
    resolve_task_env,
)
from .summary import write_json, write_logs_and_summary, write_summary


def load_batch_config(path_value: str) -> Dict[str, Any]:
    return core_load_batch_config(path_value)


def resolve_batch_defaults(batch_cfg: Dict[str, Any], args: argparse.Namespace) -> Dict[str, Any]:
    return core_resolve_batch_defaults(batch_cfg, args)


def resolve_batch_job_specs(batch_cfg: Dict[str, Any], defaults: Dict[str, Any]) -> List[BatchJobSpec]:
    return core_resolve_batch_job_specs(batch_cfg, defaults)


def run_queue_list(args: argparse.Namespace) -> int:
    queue_commands.load_deferred = load_deferred
    return queue_commands.run_queue_list(args)


def run_queue_process(args: argparse.Namespace) -> int:
    queue_commands.get_job_submission_client = get_job_submission_client
    queue_commands.process_queue = process_queue
    return queue_commands.run_queue_process(args, submit_fn=run_submit)


def run_queue_watch(args: argparse.Namespace) -> int:
    queue_commands.get_job_submission_client = get_job_submission_client
    queue_commands._redis_client = _redis_client
    queue_commands.load_deferred = load_deferred
    queue_commands.process_queue = process_queue
    queue_commands._watcher_pid_file = _watcher_pid_file
    return queue_commands.run_queue_watch(args, submit_fn=run_submit)


def build_parser() -> argparse.ArgumentParser:
    return core_build_parser()


def run_list(args: argparse.Namespace) -> int:
    repo_commands.load_repo_context = load_repo_context
    repo_commands.resolve_resources = resolve_resources
    return repo_commands.run_list(args)


def run_init(args: argparse.Namespace) -> int:
    return init_commands.run_init(args)


def run_validate(args: argparse.Namespace) -> int:
    repo_commands.load_repo_context = load_repo_context
    repo_commands.build_submission_id = build_submission_id
    repo_commands.resolve_submission_prefix = resolve_submission_prefix
    repo_commands.resolve_output_dir = resolve_output_dir
    repo_commands.resolve_entrypoint = resolve_entrypoint
    repo_commands.resolve_resources = resolve_resources
    repo_commands.resolve_runtime_env = resolve_runtime_env
    repo_commands.resolve_metadata = resolve_metadata
    repo_commands.build_injected_env = build_injected_env
    return repo_commands.run_validate(args)


def _ensure_queue_backend_ready() -> str:
    if get_queue_backend_name() == "redis":
        _redis_client()
        return f"redis ({get_queue_settings().redis_url})"
    return ensure_queue_backend_ready()


def run_doctor(args: argparse.Namespace) -> int:
    repo_commands.load_repo_context = load_repo_context
    repo_commands.get_job_submission_client = get_job_submission_client
    repo_commands._kafka_producer = _kafka_producer
    repo_commands.ensure_queue_backend_ready = _ensure_queue_backend_ready
    repo_commands.get_queue_backend_name = get_queue_backend_name
    repo_commands.build_submit_payload = build_submit_payload
    repo_commands.KAFKA_OPERATION_TIMEOUT_S = KAFKA_OPERATION_TIMEOUT_S
    return repo_commands.run_doctor(args)


def run_submit(args: argparse.Namespace, _skip_queue: bool = False) -> int:
    submit_commands.load_repo_context = load_repo_context
    submit_commands._handle_queue_on_submit = _handle_queue_on_submit
    submit_commands.build_submit_payload = build_submit_payload
    submit_commands.get_job_submission_client = get_job_submission_client
    submit_commands.write_summary = write_summary
    submit_commands.wait_for_job = wait_for_job
    submit_commands.write_logs_and_summary = write_logs_and_summary
    submit_commands.process_queue = process_queue
    return submit_commands.run_submit(
        args,
        skip_queue=_skip_queue,
        submit_again=run_submit,
    )


def run_batch_submit(args: argparse.Namespace) -> int:
    batch_commands.load_batch_config = load_batch_config
    batch_commands.resolve_batch_defaults = resolve_batch_defaults
    batch_commands.resolve_batch_job_specs = resolve_batch_job_specs
    batch_commands.submit_batch_job = submit_batch_job
    batch_commands.build_batch_job_record = build_batch_job_record
    batch_commands.build_batch_index = build_batch_index
    batch_commands.write_batch_index = write_batch_index
    batch_commands.poll_batch_jobs = poll_batch_jobs
    return batch_commands.run_batch_submit(args)


def run_watch(args: argparse.Namespace) -> int:
    job_commands.get_job_submission_client = get_job_submission_client
    job_commands.watch_job = watch_job
    job_commands.write_logs_and_summary = write_logs_and_summary
    return job_commands.run_watch(args)


def run_logs(args: argparse.Namespace) -> int:
    job_commands.get_job_submission_client = get_job_submission_client
    return job_commands.run_logs(args)


def run_status(args: argparse.Namespace) -> int:
    job_commands.get_job_submission_client = get_job_submission_client
    return job_commands.run_status(args)


def _parse_redis_host_port(redis_url: str) -> tuple[str, int]:
    return _setup_parse_redis_host_port_impl(redis_url)


def _is_tcp_reachable(host: str, port: int, timeout_s: float = 1.0) -> bool:
    return _setup_is_tcp_reachable_impl(host, port, timeout_s)


def _write_setup_env_file(
    *,
    env_file: Path,
    ray_address: str,
    redis_url: str,
    shell_rc: Optional[Path],
) -> None:
    _setup_write_setup_env_file_impl(
        env_file=env_file,
        ray_address=ray_address,
        redis_url=redis_url,
        shell_rc=shell_rc,
    )


def run_setup_stack(args: argparse.Namespace) -> int:
    setup_commands._parse_redis_host_port = _parse_redis_host_port
    setup_commands._is_tcp_reachable = _is_tcp_reachable
    setup_commands._write_setup_env_file = _write_setup_env_file
    return setup_commands.run_setup_stack(args)


def main(argv: Optional[List[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "list":
        return run_list(args)
    if args.command == "init":
        return run_init(args)
    if args.command == "validate":
        return run_validate(args)
    if args.command == "doctor":
        return run_doctor(args)
    if args.command == "submit":
        return run_submit(args)
    if args.command == "setup-stack":
        return run_setup_stack(args)
    if args.command == "batch":
        if args.batch_command == "submit":
            return run_batch_submit(args)
        parser.error(f"Unknown batch command: {args.batch_command}")
        return 2
    if args.command == "queue":
        if args.queue_command == "list":
            return run_queue_list(args)
        if args.queue_command == "process":
            return run_queue_process(args)
        if args.queue_command == "watch":
            return run_queue_watch(args)
        parser.error(f"Unknown queue command: {args.queue_command}")
        return 2
    if args.command == "watch":
        return run_watch(args)
    if args.command == "logs":
        return run_logs(args)
    if args.command == "status":
        return run_status(args)
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
