import json
from pathlib import Path
from typing import Any, Dict, Optional

from .config import TERMINAL_STATES
from .resolve import timestamp


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n")


def write_summary(
    summary_path_value: Optional[str],
    *,
    job_id: Optional[str],
    job_status: str,
    payload: Optional[Dict[str, Any]],
    log_path: Optional[Path],
) -> None:
    if not summary_path_value:
        return

    summary_path = Path(summary_path_value).expanduser().resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    payload_data = dict(payload or {})
    summary = {
        "job_id": job_id,
        "address": payload_data.get("address"),
        "task": payload_data.get("task"),
        "submission_id": payload_data.get("submission_id"),
        "job_status": job_status,
        "final_status": job_status if job_status in TERMINAL_STATES else None,
        "log_path": str(log_path) if log_path else None,
        "updated_at": timestamp(),
        "payload": payload,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote summary to: {summary_path}")


def write_logs_and_summary(
    client: Any,
    job_id: str,
    final_status: str,
    payload: Optional[Dict[str, Any]],
    log_path_value: Optional[str],
    summary_path_value: Optional[str],
    print_logs: bool,
) -> None:
    needs_logs = (
        log_path_value is not None
        or summary_path_value is not None
        or print_logs
        or final_status != "SUCCEEDED"
    )
    if not needs_logs:
        return

    logs = client.get_job_logs(job_id)
    resolved_log_path: Optional[Path] = None

    if log_path_value:
        resolved_log_path = Path(log_path_value).expanduser().resolve()
        resolved_log_path.parent.mkdir(parents=True, exist_ok=True)
        resolved_log_path.write_text(logs)
        print(f"Wrote logs to: {resolved_log_path}")

    if summary_path_value:
        write_summary(
            summary_path_value,
            job_id=job_id,
            job_status=final_status,
            payload=payload,
            log_path=resolved_log_path,
        )

    if print_logs or final_status != "SUCCEEDED":
        print(logs)
