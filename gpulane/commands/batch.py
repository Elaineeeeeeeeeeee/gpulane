import argparse
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..batch import (
    build_batch_index,
    build_batch_job_record,
    poll_batch_jobs,
    submit_batch_job,
    write_batch_index,
)
from ..config import TERMINAL_STATES
from ..resolve import timestamp


def run_batch_submit(args: argparse.Namespace) -> int:
    batch_cfg = load_batch_config(args.file)
    defaults = resolve_batch_defaults(batch_cfg, args)
    specs = resolve_batch_job_specs(batch_cfg, defaults)

    summary_dir_value = defaults.get("summary_dir")
    index_path = None
    if summary_dir_value:
        summary_dir = Path(str(summary_dir_value)).expanduser().resolve()
        index_path = summary_dir / "index.json"

    started_at = timestamp()
    jobs: List[Dict[str, Any]] = []
    active_jobs: List[Dict[str, Any]] = []
    client_by_address: Dict[str, Any] = {}
    next_index = 0

    def flush_index(finished_at: Optional[str] = None) -> None:
        write_batch_index(
            index_path,
            build_batch_index(
                batch_file=str(batch_cfg["_path"]),
                defaults=defaults,
                jobs=jobs,
                started_at=started_at,
                finished_at=finished_at,
            ),
        )

    while next_index < len(specs):
        spec = specs[next_index]
        result = submit_batch_job(
            spec,
            address=defaults["address"],
            dry_run=args.dry_run,
        )
        record = build_batch_job_record(spec, result["payload"], next_index)
        record["job_id"] = result["job_id"]
        record["status"] = result["status"]
        jobs.append(record)
        if (
            not args.dry_run
            and result["job_id"]
            and result["status"] not in TERMINAL_STATES
        ):
            active_jobs.append(record)
        next_index += 1
        flush_index()

    if args.dry_run or not args.wait:
        finished_at = timestamp()
        flush_index(finished_at=finished_at)
        return 0

    while active_jobs:
        finished = poll_batch_jobs(active_jobs, client_by_address=client_by_address)
        flush_index()
        active_jobs = [
            job for job in active_jobs if job.get("status") not in TERMINAL_STATES
        ]
        if not finished and active_jobs:
            time.sleep(args.poll_interval)

    finished_at = timestamp()
    flush_index(finished_at=finished_at)
    failures = [
        job
        for job in jobs
        if job.get("status") not in {"SUCCEEDED", "STOPPED"}
        and job.get("status") in TERMINAL_STATES
    ]
    failed = [job for job in jobs if job.get("status") == "FAILED"]
    return 1 if failed or failures else 0
