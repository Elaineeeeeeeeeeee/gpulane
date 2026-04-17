import os
from pathlib import Path
from typing import Dict

TERMINAL_STATES = {"SUCCEEDED", "STOPPED", "FAILED"}
DEFAULT_RAY_ADDRESS = os.environ.get("GPULANE_RAY_ADDRESS", "http://127.0.0.1:8265")
PROTECTED_ENV_VARS = {"CUDA_VISIBLE_DEVICES"}
PRIORITY_LEVELS: Dict[str, int] = {"high": 0, "normal": 1, "low": 2}
PRIORITY_LEVELS_ENV_VAR = "GPULANE_PRIORITY_LEVELS"
DEFAULT_REDIS_URL = "redis://127.0.0.1:6379"
REDIS_URL = os.environ.get("GPULANE_REDIS_URL", DEFAULT_REDIS_URL)
QUEUE_BACKEND = os.environ.get("GPULANE_QUEUE_BACKEND", "redis").strip().lower() or "redis"
KAFKA_BROKERS = os.environ.get("GPULANE_KAFKA_BROKERS")
REDIS_QUEUE_KEY = "gpulane:deferred"
REDIS_EVENTS_CHANNEL = "gpulane:events"
KAFKA_CANCEL_TOPIC = "gpulane.cancel"
KAFKA_EVENTS_TOPIC = "gpulane.events"
STATE_DIR = Path.home() / ".gpulane"
LOCAL_QUEUE_STATE_PATH = Path(
    os.environ.get("GPULANE_LOCAL_QUEUE_STATE_PATH", str(STATE_DIR / "queue.json"))
).expanduser()

# Centralized timing/network settings so operators can tune behavior in one place.
RAY_HTTP_TIMEOUT_S = float(os.environ.get("GPULANE_RAY_HTTP_TIMEOUT_S", "5"))
REDIS_CONNECT_TIMEOUT_S = float(os.environ.get("GPULANE_REDIS_CONNECT_TIMEOUT_S", "2"))
KAFKA_OPERATION_TIMEOUT_S = float(os.environ.get("GPULANE_KAFKA_OPERATION_TIMEOUT_S", "2"))
JOB_STATUS_POLL_INTERVAL_S = float(os.environ.get("GPULANE_JOB_STATUS_POLL_INTERVAL_S", "5"))
QUEUE_WATCH_POLL_INTERVAL_S = float(os.environ.get("GPULANE_QUEUE_WATCH_POLL_INTERVAL_S", "30"))
PREEMPT_STOP_POLL_INTERVAL_S = float(os.environ.get("GPULANE_PREEMPT_STOP_POLL_INTERVAL_S", "2"))
PREEMPT_STOP_MAX_WAIT_S = float(os.environ.get("GPULANE_PREEMPT_STOP_MAX_WAIT_S", "120"))
PREEMPT_RESOURCE_WAIT_TIMEOUT_S = float(os.environ.get("GPULANE_PREEMPT_RESOURCE_WAIT_TIMEOUT_S", "60"))
PREEMPT_RESOURCE_WAIT_POLL_INTERVAL_S = float(os.environ.get("GPULANE_PREEMPT_RESOURCE_WAIT_POLL_INTERVAL_S", "1"))
