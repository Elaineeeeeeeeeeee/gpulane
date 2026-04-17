import argparse

from ..ray_client import get_job_submission_client, watch_job
from ..summary import write_logs_and_summary


def run_watch(args: argparse.Namespace) -> int:
    client = get_job_submission_client(args.address)
    final_status = watch_job(client, args.job_id, args.poll_interval)
    payload = {"address": args.address, "job_id": args.job_id}
    write_logs_and_summary(
        client=client,
        job_id=args.job_id,
        final_status=final_status,
        payload=payload,
        log_path_value=args.log_path,
        summary_path_value=args.summary_path,
        print_logs=args.print_logs,
    )
    return 0 if final_status == "SUCCEEDED" else 1


def run_logs(args: argparse.Namespace) -> int:
    client = get_job_submission_client(args.address)
    print(client.get_job_logs(args.job_id))
    return 0


def run_status(args: argparse.Namespace) -> int:
    client = get_job_submission_client(args.address)
    print(client.get_job_status(args.job_id))
    return 0
