from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class RepoContext:
    manifest_path: Path
    repo_root: Path
    repo_name: str
    repo_config: Dict[str, Any]
    tasks: Dict[str, Dict[str, Any]]

    def require_task(self, task_name: str) -> Dict[str, Any]:
        if task_name not in self.tasks:
            raise ValueError(f"Unknown task '{task_name}' in manifest {self.manifest_path}")
        return self.tasks[task_name]

    def require_task_spec(self, task_name: str) -> "TaskSpec":
        return TaskSpec(name=task_name, config=self.require_task(task_name))


@dataclass
class DeferredJob:
    priority: str
    address: str
    task: str
    manifest: Optional[str]
    repo_root: Optional[str]
    extra_args: List[str]
    name: Optional[str]
    submission_prefix: Optional[str]
    summary_path: Optional[str]
    deferred_at: str
    reason: str  # "preempted" | "queued"
    original_job_id: Optional[str] = None
    run_name: Optional[str] = None
    resume_run_name_arg: Optional[str] = None
    requested_resources: Optional[Dict[str, float]] = None


@dataclass(frozen=True)
class BatchJobSpec:
    task: str
    name: Optional[str]
    submission_prefix: Optional[str]
    extra_args: List[str]
    overrides: Dict[str, Any]
    manifest: Optional[str]
    repo_root: Optional[str]
    summary_path: Optional[str]


@dataclass(frozen=True)
class TaskSpec:
    name: str
    config: Dict[str, Any]

    @property
    def description(self) -> str:
        return str(self.config.get("description", "")).strip()

    @property
    def kind(self) -> str:
        return str(self.config.get("kind", "job"))


@dataclass(frozen=True)
class ResourceSpec:
    cpu: float
    gpu: float
    memory: Optional[int] = None
    custom: Optional[Dict[str, float]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "cpu": self.cpu,
            "gpu": self.gpu,
        }
        if self.memory is not None:
            payload["memory"] = self.memory
        if self.custom:
            payload["custom"] = self.custom
        return payload


@dataclass(frozen=True)
class SubmissionPayload:
    address: str
    submission_id: str
    repo_name: str
    repo_root: str
    manifest_path: str
    task: str
    kind: str
    cwd: str
    entrypoint: str
    resources: Dict[str, Any]
    entrypoint_resources: Dict[str, Any]
    runtime_env: Dict[str, Any]
    metadata: Dict[str, str]
    output_dir: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "address": self.address,
            "submission_id": self.submission_id,
            "repo_name": self.repo_name,
            "repo_root": self.repo_root,
            "manifest_path": self.manifest_path,
            "task": self.task,
            "kind": self.kind,
            "cwd": self.cwd,
            "entrypoint": self.entrypoint,
            "resources": self.resources,
            "entrypoint_resources": self.entrypoint_resources,
            "runtime_env": self.runtime_env,
            "metadata": self.metadata,
            "output_dir": self.output_dir,
        }
