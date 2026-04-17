import argparse
from pathlib import Path
from typing import Dict, List

import yaml

from ..settings import get_default_priority_label


def _command_list(command: List[str]) -> List[str]:
    if command[:1] == ["--"]:
        command = command[1:]
    if command:
        return [str(part) for part in command]
    return ["python", "train.py"]


def _build_manifest_template(
    *,
    repo_name: str,
    task_name: str,
    cwd: str,
    command: List[str],
) -> Dict[str, object]:
    return {
        "version": 1,
        "repo": {
            "name": repo_name,
        },
        "tasks": {
            task_name: {
                "description": "Describe what this task runs.",
                "kind": "job",
                "priority": get_default_priority_label(),
                "cwd": cwd,
                "command": _command_list(command),
                "resources": {
                    "cpu": 1,
                    "gpu": 0,
                },
            }
        },
    }


def run_init(args: argparse.Namespace) -> int:
    manifest_path = Path(args.path).expanduser().resolve()
    if manifest_path.exists() and not args.force:
        raise FileExistsError(
            f"Refusing to overwrite existing manifest: {manifest_path}. "
            "Use --force to replace it."
        )

    repo_root = manifest_path.parent
    repo_name = str(args.repo_name or repo_root.name)
    manifest = _build_manifest_template(
        repo_name=repo_name,
        task_name=str(args.task_name),
        cwd=str(args.cwd),
        command=list(args.command or []),
    )

    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with open(manifest_path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle, sort_keys=False)

    print(f"Wrote starter manifest to: {manifest_path}")
    print("Next steps:")
    print(f"  1. Edit {manifest_path.name} to match your repo layout.")
    print(f"  2. Run: gpulane validate --manifest {manifest_path}")
    return 0
