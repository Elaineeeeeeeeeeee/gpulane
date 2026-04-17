import json
import time
from datetime import datetime
from typing import Any, Dict, List

from .config import TERMINAL_STATES
from .resolve import timestamp
from .settings import get_default_priority_label, get_ray_settings


def _normalize_job_started_at(job: Any) -> str:
    candidates = (
        getattr(job, "start_time", None),
        getattr(job, "start_timestamp", None),
        getattr(job, "created_at", None),
        getattr(job, "creation_time", None),
        getattr(job, "submission_time", None),
    )
    for value in candidates:
        if value in (None, ""):
            continue
        if isinstance(value, (int, float)):
            try:
                return datetime.fromtimestamp(float(value) / (1000.0 if float(value) > 1e12 else 1.0)).isoformat(
                    timespec="seconds"
                )
            except Exception:
                continue
        try:
            return str(value)
        except Exception:
            continue
    return ""


def get_job_submission_client(address: str) -> Any:
    try:
        from ray.job_submission import JobSubmissionClient
    except ImportError as exc:
        raise SystemExit(
            'Ray Jobs SDK is required. Install it with: pip install -U "ray[default]"'
        ) from exc

    return JobSubmissionClient(address)


def wait_for_job(client: Any, job_id: str, poll_interval: float) -> str:
    while True:
        status = str(client.get_job_status(job_id))
        if status in TERMINAL_STATES:
            return status
        time.sleep(poll_interval)


def watch_job(client: Any, job_id: str, poll_interval: float) -> str:
    while True:
        status = str(client.get_job_status(job_id))
        if status in TERMINAL_STATES:
            print(f"[{timestamp()}] DONE: {status}")
            return status
        print(f"[{timestamp()}] still running: {status}")
        time.sleep(poll_interval)


def get_active_ray_jobs(client: Any) -> List[Dict[str, Any]]:
    """Returns non-terminal Ray jobs with their priority metadata."""
    try:
        result = []
        for job in client.list_jobs():
            status = str(getattr(job, "status", "") or "")
            if status in TERMINAL_STATES:
                continue
            metadata = dict(getattr(job, "metadata", {}) or {})
            job_id = str(
                getattr(job, "submission_id", None) or getattr(job, "job_id", "") or ""
            )
            requested_resources = {}
            try:
                requested_resources = json.loads(
                    metadata.get("requested_resources_json", "{}")
                )
            except Exception:
                requested_resources = {}
            result.append(
                {
                    "job_id": job_id,
                    "status": status,
                    "priority": metadata.get("priority", get_default_priority_label()),
                    "metadata": metadata,
                }
            )
            result[-1]["requested_resources"] = requested_resources
            result[-1]["started_at"] = _normalize_job_started_at(job)
        return result
    except Exception:
        return []


def get_ray_available_resources(address: str) -> Dict[str, float]:
    """Query Ray dashboard for currently available cluster resources.
    Returns empty dict on failure (caller should treat as insufficient)."""
    try:
        import urllib.request

        url = f"{address}/api/cluster_status"
        with urllib.request.urlopen(url, timeout=get_ray_settings().http_timeout_s) as resp:
            data = json.loads(resp.read())
        # usage format: {resource: [used, total]}
        usage = (
            data.get("data", {})
                .get("clusterStatus", {})
                .get("loadMetricsReport", {})
                .get("usage", {})
        )
        return {k: v[1] - v[0] for k, v in usage.items() if isinstance(v, list) and len(v) == 2}
    except Exception:
        return {}
