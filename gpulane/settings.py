import os
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

from .config import (
    DEFAULT_RAY_ADDRESS,
    DEFAULT_REDIS_URL,
    PRIORITY_LEVELS,
    PRIORITY_LEVELS_ENV_VAR,
    REDIS_EVENTS_CHANNEL,
    REDIS_QUEUE_KEY,
    STATE_DIR,
)


@dataclass(frozen=True)
class QueueSettings:
    backend: str
    redis_url: str
    redis_queue_key: str
    redis_events_channel: str
    kafka_brokers: str | None
    kafka_operation_timeout_s: float
    redis_connect_timeout_s: float
    local_queue_state_path: Path
    state_dir: Path
    watch_poll_interval_s: float
    preempt_stop_poll_interval_s: float
    preempt_stop_max_wait_s: float
    preempt_resource_wait_timeout_s: float
    preempt_resource_wait_poll_interval_s: float
    job_status_poll_interval_s: float
    default_ray_address: str
    default_redis_url: str


@dataclass(frozen=True)
class RaySettings:
    default_address: str
    http_timeout_s: float
    job_status_poll_interval_s: float


def get_priority_levels() -> Dict[str, int]:
    raw_levels = os.environ.get(PRIORITY_LEVELS_ENV_VAR, "")
    if not raw_levels.strip():
        return dict(PRIORITY_LEVELS)

    labels = [item.strip() for item in raw_levels.split(",")]
    labels = [item for item in labels if item]
    if not labels:
        raise ValueError(
            f"{PRIORITY_LEVELS_ENV_VAR} must contain a comma-separated ordered list"
        )

    seen = set()
    ordered_labels = []
    for label in labels:
        if label in seen:
            raise ValueError(
                f"{PRIORITY_LEVELS_ENV_VAR} contains a duplicate label: {label}"
            )
        seen.add(label)
        ordered_labels.append(label)
    return {label: index for index, label in enumerate(ordered_labels)}


def get_default_priority_label() -> str:
    levels = get_priority_levels()
    if "normal" in levels:
        return "normal"
    return next(iter(levels))


def get_queue_settings() -> QueueSettings:
    default_local_queue_state_path = STATE_DIR / "queue.json"
    return QueueSettings(
        backend=os.environ.get("GPULANE_QUEUE_BACKEND", "redis").strip().lower() or "redis",
        redis_url=os.environ.get("GPULANE_REDIS_URL", DEFAULT_REDIS_URL),
        redis_queue_key=REDIS_QUEUE_KEY,
        redis_events_channel=REDIS_EVENTS_CHANNEL,
        kafka_brokers=os.environ.get("GPULANE_KAFKA_BROKERS"),
        kafka_operation_timeout_s=float(
            os.environ.get("GPULANE_KAFKA_OPERATION_TIMEOUT_S", "2")
        ),
        redis_connect_timeout_s=float(
            os.environ.get("GPULANE_REDIS_CONNECT_TIMEOUT_S", "2")
        ),
        local_queue_state_path=Path(
            os.environ.get(
                "GPULANE_LOCAL_QUEUE_STATE_PATH",
                str(default_local_queue_state_path),
            )
        ).expanduser(),
        state_dir=STATE_DIR,
        watch_poll_interval_s=float(
            os.environ.get("GPULANE_QUEUE_WATCH_POLL_INTERVAL_S", "30")
        ),
        preempt_stop_poll_interval_s=float(
            os.environ.get("GPULANE_PREEMPT_STOP_POLL_INTERVAL_S", "2")
        ),
        preempt_stop_max_wait_s=float(
            os.environ.get("GPULANE_PREEMPT_STOP_MAX_WAIT_S", "120")
        ),
        preempt_resource_wait_timeout_s=float(
            os.environ.get("GPULANE_PREEMPT_RESOURCE_WAIT_TIMEOUT_S", "60")
        ),
        preempt_resource_wait_poll_interval_s=float(
            os.environ.get("GPULANE_PREEMPT_RESOURCE_WAIT_POLL_INTERVAL_S", "1")
        ),
        job_status_poll_interval_s=float(
            os.environ.get("GPULANE_JOB_STATUS_POLL_INTERVAL_S", "5")
        ),
        default_ray_address=DEFAULT_RAY_ADDRESS,
        default_redis_url=DEFAULT_REDIS_URL,
    )


def get_ray_settings() -> RaySettings:
    return RaySettings(
        default_address=os.environ.get("GPULANE_RAY_ADDRESS", DEFAULT_RAY_ADDRESS),
        http_timeout_s=float(os.environ.get("GPULANE_RAY_HTTP_TIMEOUT_S", "5")),
        job_status_poll_interval_s=float(
            os.environ.get("GPULANE_JOB_STATUS_POLL_INTERVAL_S", "5")
        ),
    )
