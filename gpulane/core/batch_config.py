import argparse
from pathlib import Path
from typing import Any, Dict, List

from ..manifest import load_yaml, normalize_args, normalize_mapping
from ..models import BatchJobSpec
from ..resolve import slugify
from ..settings import get_ray_settings


def load_batch_config(path_value: str) -> Dict[str, Any]:
    path = Path(path_value).expanduser().resolve()
    if not path.exists():
        raise FileNotFoundError(f"Batch file does not exist: {path}")
    data = load_yaml(path)
    jobs = data.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise ValueError("Batch file must define a non-empty 'jobs' list")
    data["_path"] = str(path)
    return data


def resolve_batch_defaults(
    batch_cfg: Dict[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    ray_settings = get_ray_settings()
    defaults = normalize_mapping(batch_cfg.get("defaults"), "defaults")
    return {
        "manifest": defaults.get("manifest"),
        "repo_root": defaults.get("repo_root"),
        "address": str(defaults.get("address", ray_settings.default_address)),
        "submission_prefix": defaults.get("submission_prefix"),
        "summary_dir": defaults.get("summary_dir"),
        "overrides": normalize_mapping(defaults.get("overrides"), "defaults.overrides"),
    }


def resolve_batch_job_specs(
    batch_cfg: Dict[str, Any],
    defaults: Dict[str, Any],
) -> List[BatchJobSpec]:
    summary_dir_value = defaults.get("summary_dir")
    summary_dir = (
        Path(str(summary_dir_value)).expanduser().resolve()
        if summary_dir_value
        else None
    )
    specs: List[BatchJobSpec] = []
    for index, raw_job in enumerate(batch_cfg["jobs"]):
        if not isinstance(raw_job, dict):
            raise ValueError(f"Batch job at index {index} must be a mapping")
        task = raw_job.get("task")
        if not task:
            raise ValueError(f"Batch job at index {index} is missing 'task'")
        name = raw_job.get("name")
        submission_prefix = raw_job.get(
            "submission_prefix", defaults.get("submission_prefix")
        )
        manifest = raw_job.get("manifest", defaults.get("manifest"))
        repo_root = raw_job.get("repo_root", defaults.get("repo_root"))
        extra_args = normalize_args(raw_job.get("extra_args"), f"jobs[{index}].extra_args")
        summary_path = raw_job.get("summary_path")
        if summary_path is None and summary_dir is not None:
            summary_name = slugify(str(name or task))
            summary_path = str(
                (summary_dir / f"{index:03d}-{summary_name}.json").resolve()
            )
        specs.append(
            BatchJobSpec(
                task=str(task),
                name=str(name) if name is not None else None,
                submission_prefix=(
                    str(submission_prefix)
                    if submission_prefix is not None
                    else None
                ),
                extra_args=extra_args,
                overrides=normalize_mapping(
                    {
                        **normalize_mapping(
                            defaults.get("overrides"), "defaults.overrides"
                        ),
                        **normalize_mapping(
                            raw_job.get("overrides"),
                            f"jobs[{index}].overrides",
                        ),
                    },
                    f"jobs[{index}].overrides",
                ),
                manifest=str(manifest) if manifest is not None else None,
                repo_root=str(repo_root) if repo_root is not None else None,
                summary_path=str(summary_path) if summary_path is not None else None,
            )
        )
    return specs
