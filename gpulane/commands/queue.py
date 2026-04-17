import argparse
import os
import time

from ..queue.backend import (
    _redis_client,
    get_queue_backend_name,
    get_queue_events_channel,
    load_deferred,
)
from ..queue.scheduler import _watcher_pid_file, process_queue
from ..ray_client import get_job_submission_client


def run_queue_list(args: argparse.Namespace) -> int:
    deferred = load_deferred()
    if not deferred:
        print("Queue is empty.")
        return 0
    print(f"{'#':<4} {'priority':<16} {'task':<30} {'reason':<12} deferred_at")
    for i, job in enumerate(deferred):
        print(
            f"{i:<4} {job.priority:<16} {job.task:<30} {job.reason:<12} {job.deferred_at}"
        )
    return 0


def run_queue_process(
    args: argparse.Namespace,
    *,
    submit_fn,
) -> int:
    client = get_job_submission_client(args.address)
    process_queue(client, submit_fn, dry_run=getattr(args, "dry_run", False))
    return 0


def run_queue_watch(
    args: argparse.Namespace,
    *,
    submit_fn,
) -> int:
    client = get_job_submission_client(args.address)
    poll_interval = args.poll_interval

    pid_file = _watcher_pid_file()
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text(str(os.getpid()))

    try:
        if get_queue_backend_name() == "redis":
            r = _redis_client()
            pubsub = r.pubsub()
            pubsub.subscribe(get_queue_events_channel())
            while True:
                if not load_deferred():
                    break
                process_queue(client, submit_fn)
                pubsub.get_message(ignore_subscribe_messages=True, timeout=poll_interval)
        else:
            while True:
                if not load_deferred():
                    break
                process_queue(client, submit_fn)
                time.sleep(poll_interval)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            if pid_file.exists() and pid_file.read_text().strip() == str(os.getpid()):
                pid_file.unlink()
        except OSError:
            pass

    return 0
