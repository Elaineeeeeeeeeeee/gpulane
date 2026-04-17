import json
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import Any, List

from ..config import KAFKA_CANCEL_TOPIC, KAFKA_EVENTS_TOPIC
from ..models import DeferredJob
from ..settings import get_priority_levels, get_queue_settings


def _runtime_queue_settings() -> dict[str, Any]:
    settings = get_queue_settings()
    return {
        "backend": settings.backend,
        "redis_url": settings.redis_url,
        "redis_connect_timeout_s": settings.redis_connect_timeout_s,
        "kafka_brokers": settings.kafka_brokers,
        "kafka_operation_timeout_s": settings.kafka_operation_timeout_s,
        "local_queue_state_path": settings.local_queue_state_path,
        "redis_queue_key": settings.redis_queue_key,
        "redis_events_channel": settings.redis_events_channel,
    }


def get_queue_backend_name() -> str:
    return _runtime_queue_settings()["backend"]


def uses_redis_queue_backend() -> bool:
    return get_queue_backend_name() == "redis"


def get_queue_events_channel() -> str:
    return _runtime_queue_settings()["redis_events_channel"]


def _local_queue_path() -> Path:
    return _runtime_queue_settings()["local_queue_state_path"]


def _redis_client() -> Any:
    settings = _runtime_queue_settings()
    try:
        import redis as redis_lib

        r = redis_lib.from_url(
            settings["redis_url"],
            socket_connect_timeout=settings["redis_connect_timeout_s"],
        )
        r.ping()
        return r
    except Exception as exc:
        raise RuntimeError(f"Redis is unavailable: {settings['redis_url']}") from exc


def _kafka_producer() -> Any:
    settings = _runtime_queue_settings()
    if not settings["kafka_brokers"]:
        return None
    try:
        from kafka import KafkaProducer

        producer = KafkaProducer(
            bootstrap_servers=settings["kafka_brokers"].split(","),
            value_serializer=lambda v: json.dumps(v).encode(),
        )
        if not producer.bootstrap_connected():
            producer.close(timeout=settings["kafka_operation_timeout_s"])
            raise RuntimeError(
                "GPULANE_KAFKA_BROKERS is set but Kafka is unavailable: "
                f"{settings['kafka_brokers']}"
            )
        return producer
    except Exception as exc:
        if isinstance(exc, RuntimeError):
            raise
        raise RuntimeError(
            "GPULANE_KAFKA_BROKERS is set but Kafka is unavailable: "
            f"{settings['kafka_brokers']}"
        ) from exc


def _redis_score(priority: str, deferred_at: str) -> float:
    level = get_priority_levels().get(priority, 99)
    try:
        ts = datetime.fromisoformat(deferred_at).timestamp()
    except Exception:
        ts = 0.0
    return level * 1e10 + ts


def _publish_event(event_type: str, data: dict) -> None:
    payload = {"type": event_type, **data}
    settings = _runtime_queue_settings()
    if uses_redis_queue_backend():
        r = _redis_client()
        try:
            r.publish(settings["redis_events_channel"], json.dumps(payload))
        except Exception:
            pass
    producer = _kafka_producer()
    if producer is not None:
        try:
            producer.send(KAFKA_EVENTS_TOPIC, value=payload)
            producer.flush(timeout=settings["kafka_operation_timeout_s"])
        except Exception:
            pass


def publish_cancel_request(job_id: str, task: Any, priority: str) -> None:
    settings = _runtime_queue_settings()
    producer = _kafka_producer()
    if producer is None:
        return
    try:
        producer.send(
            KAFKA_CANCEL_TOPIC,
            value={"job_id": job_id, "task": task, "priority": priority},
        )
        producer.flush(timeout=settings["kafka_operation_timeout_s"])
    except Exception:
        pass


def _load_deferred_local() -> List[DeferredJob]:
    path = _local_queue_path()
    if not path.exists():
        return []
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    jobs: List[DeferredJob] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        try:
            jobs.append(DeferredJob(**item))
        except Exception:
            continue
    return jobs


def _save_deferred_local(jobs: List[DeferredJob]) -> None:
    path = _local_queue_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(jobs, key=lambda job: _redis_score(job.priority, job.deferred_at))
    path.write_text(
        json.dumps([asdict(job) for job in ordered], indent=2),
        encoding="utf-8",
    )


def _queue_backend_summary() -> str:
    settings = _runtime_queue_settings()
    if uses_redis_queue_backend():
        return f"redis ({settings['redis_url']})"
    return f"local ({_local_queue_path()})"


def ensure_queue_backend_ready() -> str:
    if uses_redis_queue_backend():
        _redis_client()
    else:
        path = _local_queue_path()
        path.parent.mkdir(parents=True, exist_ok=True)
    return _queue_backend_summary()


def load_deferred() -> List[DeferredJob]:
    if not uses_redis_queue_backend():
        return _load_deferred_local()

    r = _redis_client()
    try:
        key = _runtime_queue_settings()["redis_queue_key"]
        return [DeferredJob(**json.loads(item)) for item in r.zrange(key, 0, -1)]
    except Exception:
        return []


def save_deferred(jobs: List[DeferredJob]) -> None:
    if not uses_redis_queue_backend():
        _save_deferred_local(jobs)
        return

    r = _redis_client()
    pipe = r.pipeline()
    key = _runtime_queue_settings()["redis_queue_key"]
    pipe.delete(key)
    for job in jobs:
        pipe.zadd(
            key,
            {json.dumps(asdict(job)): _redis_score(job.priority, job.deferred_at)},
        )
    pipe.execute()
