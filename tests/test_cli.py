import io
import json
import os
import subprocess
import tempfile
import textwrap
import unittest
from contextlib import ExitStack
from pathlib import Path
from unittest import mock

from gpulane import cli
import gpulane.cli as cli_module
from gpulane.queue.scheduler import process_queue


class TrainctlTest(unittest.TestCase):
    def make_repo(self) -> Path:
        root = Path(tempfile.mkdtemp(prefix="workstation-ray-test-"))
        (root / "nested" / "job").mkdir(parents=True)
        (root / "nested" / "job" / "train.py").write_text("print('ok')\n")
        subprocess.run(["git", "init"], cwd=root, check=True, capture_output=True)
        subprocess.run(
            ["git", "config", "user.email", "codex@example.com"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        subprocess.run(
            ["git", "config", "user.name", "Codex"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        (root / "README.md").write_text("repo\n")
        subprocess.run(["git", "add", "."], cwd=root, check=True, capture_output=True)
        subprocess.run(
            ["git", "commit", "-m", "init"],
            cwd=root,
            check=True,
            capture_output=True,
        )
        return root

    def add_origin_remote(self, repo_root: Path, url: str = "https://github.com/octocat/example.git") -> None:
        subprocess.run(
            ["git", "remote", "add", "origin", url],
            cwd=repo_root,
            check=True,
            capture_output=True,
        )

    def test_find_manifest_by_walking_up(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text("version: 1\ntasks: {}\n")
        found = cli.find_manifest(repo_root / "nested" / "job")
        self.assertEqual(found, manifest_path.resolve())

    def test_init_writes_starter_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "ray_tasks.yaml"
            args = type(
                "Args",
                (),
                {
                    "path": str(manifest_path),
                    "repo_name": "sample-repo",
                    "task_name": "sample.job",
                    "cwd": "nested/job",
                    "force": False,
                    "command": ["--", "python", "train.py"],
                },
            )()

            result = cli.run_init(args)

            self.assertEqual(result, 0)
            manifest = cli.load_yaml(manifest_path)
            self.assertEqual(manifest["repo"]["name"], "sample-repo")
            self.assertEqual(manifest["tasks"]["sample.job"]["cwd"], "nested/job")
            self.assertEqual(
                manifest["tasks"]["sample.job"]["command"],
                ["python", "train.py"],
            )

    def test_init_refuses_to_overwrite_without_force(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            manifest_path = Path(tmp) / "ray_tasks.yaml"
            manifest_path.write_text("version: 1\ntasks: {}\n")
            args = type(
                "Args",
                (),
                {
                    "path": str(manifest_path),
                    "repo_name": None,
                    "task_name": "sample.job",
                    "cwd": ".",
                    "force": False,
                    "command": [],
                },
            )()

            with self.assertRaises(FileExistsError):
                cli.run_init(args)

    def test_watch_parser_uses_job_poll_interval_not_queue_watch_interval(self) -> None:
        parser = cli.build_parser()
        args = parser.parse_args(["watch", "--job-id", "ray-job-123"])
        self.assertEqual(args.poll_interval, 5.0)

    def test_priority_levels_accept_any_ordered_list(self) -> None:
        from gpulane.queue.backend import _redis_score
        from gpulane.resolve import resolve_priority
        from gpulane.settings import get_default_priority_label, get_priority_levels

        with mock.patch.dict(
            os.environ,
            {"GPULANE_PRIORITY_LEVELS": "urgent,standard,background,bulk"},
            clear=False,
        ):
            self.assertEqual(
                get_priority_levels(),
                {
                    "urgent": 0,
                    "standard": 1,
                    "background": 2,
                    "bulk": 3,
                },
            )
            self.assertEqual(get_default_priority_label(), "urgent")
            self.assertEqual(resolve_priority({"priority": "background"}), "background")
            self.assertLess(
                _redis_score("urgent", "2026-04-17T10:00:00"),
                _redis_score("bulk", "2026-04-17T10:00:00"),
            )

    def test_priority_levels_reject_duplicate_labels(self) -> None:
        from gpulane.settings import get_priority_levels

        with mock.patch.dict(
            os.environ,
            {"GPULANE_PRIORITY_LEVELS": "high,normal,high"},
            clear=False,
        ):
            with self.assertRaisesRegex(ValueError, "duplicate label"):
                get_priority_levels()

    def test_validate_rejects_cuda_visible_devices_override(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  bad.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    env:
                      CUDA_VISIBLE_DEVICES: "0"
                """
            ).strip()
            + "\n"
        )

        with self.assertRaises(ValueError):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_validate_rejects_runtime_env_cuda_visible_devices_override(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  bad.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    runtime_env:
                      env_vars:
                        CUDA_VISIBLE_DEVICES: "0"
                """
            ).strip()
            + "\n"
        )

        with self.assertRaises(ValueError):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_validate_rejects_unsupported_manifest_version(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 2
                tasks: {}
                """
            ).strip()
            + "\n"
        )

        with self.assertRaisesRegex(ValueError, "Unsupported manifest version"):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_validate_rejects_unknown_task_field(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  bad.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    launch_mode: interactive
                """
            ).strip()
            + "\n"
        )

        with self.assertRaisesRegex(ValueError, "Unknown field\\(s\\) in tasks.bad.job"):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_validate_rejects_non_numeric_resource_values(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  bad.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    resources:
                      gpu: many
                """
            ).strip()
            + "\n"
        )

        with self.assertRaisesRegex(ValueError, "tasks.bad.job.resources.gpu"):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_doctor_with_task_prints_payload_preview(self) -> None:
        repo_root = self.make_repo()
        self.add_origin_remote(repo_root)
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  name: sample-repo
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        args = type(
            "Args",
            (),
            {
                "manifest": str(manifest_path),
                "repo_root": None,
                "address": "http://127.0.0.1:8265",
                "task": "sample.job",
                "name": "preview",
                "submission_prefix": None,
                "extra_args": ["--", "--epochs", "10"],
            },
        )()

        client = mock.Mock()
        client.get_version.return_value = "2.45.0"

        stdout = io.StringIO()
        with (
            mock.patch.dict(os.environ, {"GPULANE_REDIS_URL": "redis://localhost:6379"}, clear=False),
            mock.patch.object(cli, "get_job_submission_client", return_value=client),
            mock.patch.object(cli, "_redis_client"),
            mock.patch("sys.stdout", stdout),
        ):
            result = cli.run_doctor(args)

        self.assertEqual(result, 0)
        output = stdout.getvalue()
        self.assertIn("Validated 1 task(s)", output)
        self.assertIn("Ray Jobs endpoint OK: http://127.0.0.1:8265 (version=2.45.0)", output)
        self.assertIn("Resolved submit payload:", output)
        self.assertIn('"task": "sample.job"', output)
        self.assertIn("python train.py --epochs 10", output)

    def test_doctor_checks_redis_and_kafka_when_configured(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        args = type(
            "Args",
            (),
            {
                "manifest": str(manifest_path),
                "repo_root": None,
                "address": "http://127.0.0.1:8265",
                "task": None,
                "name": None,
                "submission_prefix": None,
                "extra_args": [],
            },
        )()

        client = mock.Mock()
        client.get_version.return_value = "2.45.0"
        producer = mock.Mock()

        stdout = io.StringIO()
        with (
            mock.patch.dict(
                os.environ,
                {
                    "GPULANE_REDIS_URL": "redis://localhost:6379",
                    "GPULANE_KAFKA_BROKERS": "localhost:9092",
                },
                clear=False,
            ),
            mock.patch.object(cli, "get_job_submission_client", return_value=client),
            mock.patch.object(cli, "_redis_client") as redis_mock,
            mock.patch.object(cli, "_kafka_producer", return_value=producer) as kafka_mock,
            mock.patch("sys.stdout", stdout),
        ):
            result = cli.run_doctor(args)

        self.assertEqual(result, 0)
        redis_mock.assert_called_once_with()
        kafka_mock.assert_called_once_with()
        producer.close.assert_called_once_with(timeout=2)
        output = stdout.getvalue()
        self.assertIn("Redis OK: redis://localhost:6379", output)
        self.assertIn("Kafka OK: localhost:9092", output)

    def test_doctor_accepts_local_queue_backend_without_redis(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text("version: 1\ntasks: {}\n")

        args = type(
            "Args",
            (),
            {
                "manifest": str(manifest_path),
                "repo_root": None,
                "address": "http://127.0.0.1:8265",
                "task": None,
                "name": None,
                "submission_prefix": None,
                "extra_args": [],
            },
        )()

        client = mock.Mock()
        client.get_version.return_value = "2.45.0"

        stdout = io.StringIO()
        with (
            mock.patch.object(cli, "get_job_submission_client", return_value=client),
            mock.patch.object(cli, "get_queue_backend_name", return_value="local"),
            mock.patch.object(
                cli,
                "ensure_queue_backend_ready",
                return_value="local (/tmp/gpulane-queue.json)",
            ),
            mock.patch("sys.stdout", stdout),
        ):
            result = cli.run_doctor(args)

        self.assertEqual(result, 0)
        output = stdout.getvalue()
        self.assertIn("Queue backend OK: local (/tmp/gpulane-queue.json)", output)

    def test_setup_stack_writes_env_file_without_shell_rc(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            env_file = Path(tmp) / "env.sh"
            args = type(
                "Args",
                (),
                {
                    "ray_address": "http://127.0.0.1:8265",
                    "redis_url": "redis://127.0.0.1:6379",
                    "env_file": str(env_file),
                    "shell_rc": str(Path(tmp) / "bashrc"),
                    "no_shell_rc": True,
                    "skip_redis_start": False,
                },
            )()

            with mock.patch.object(cli, "_is_tcp_reachable", return_value=True):
                result = cli.run_setup_stack(args)

            self.assertEqual(result, 0)
            text = env_file.read_text(encoding="utf-8")
            self.assertIn("GPULANE_RAY_ADDRESS", text)
            self.assertIn("GPULANE_REDIS_URL", text)

    def test_doctor_fails_when_configured_redis_is_unavailable(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text("version: 1\ntasks: {}\n")

        args = type(
            "Args",
            (),
            {
                "manifest": str(manifest_path),
                "repo_root": None,
                "address": "http://127.0.0.1:8265",
                "task": None,
                "name": None,
                "submission_prefix": None,
                "extra_args": [],
            },
        )()

        client = mock.Mock()
        client.get_version.return_value = "2.45.0"

        with (
            mock.patch.dict(os.environ, {"GPULANE_REDIS_URL": "redis://localhost:6379"}, clear=False),
            mock.patch.object(cli, "get_job_submission_client", return_value=client),
            mock.patch.object(cli, "_redis_client", side_effect=RuntimeError("redis down")),
            self.assertRaises(RuntimeError),
        ):
            cli.run_doctor(args)

    def test_build_submit_payload_injects_metadata_and_output_dir(self) -> None:
        repo_root = self.make_repo()
        self.add_origin_remote(repo_root)
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  name: sample-repo
                  output_root: outputs
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    default_args: ["--epochs", "1"]
                    resources:
                      cpu: 2
                      gpu: 0
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=["--lr", "0.001"],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        self.assertEqual(payload["repo_name"], "sample-repo")
        self.assertEqual(payload["resources"]["cpu"], 2.0)
        self.assertEqual(payload["entrypoint_resources"]["cpu"], 2.0)
        self.assertIn("python train.py --epochs 1 --lr 0.001", payload["entrypoint"])
        self.assertEqual(payload["metadata"]["task"], "sample.job")
        self.assertEqual(payload["metadata"]["repo_name"], "sample-repo")
        self.assertIn("GPULANE_JOB_ID", payload["runtime_env"]["env_vars"])
        self.assertNotIn("WORKSTATION_RAY_JOB_ID", payload["runtime_env"]["env_vars"])
        self.assertIn(
            os.path.join("sample-job", payload["submission_id"]),
            payload["output_dir"],
        )

    def test_build_submit_payload_supports_distinct_entrypoint_resources(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    resources:
                      cpu: 16
                      gpu: 2
                    entrypoint_resources:
                      cpu: 1
                      gpu: 0
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=[],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        self.assertEqual(payload["resources"], {"cpu": 16.0, "gpu": 2.0})
        self.assertEqual(payload["entrypoint_resources"], {"cpu": 1.0, "gpu": 0.0})
        self.assertEqual(
            json.loads(payload["metadata"]["requested_resources_json"]),
            {"cpu": 16.0, "gpu": 2.0},
        )
        self.assertEqual(
            json.loads(payload["metadata"]["entrypoint_resources_json"]),
            {"cpu": 1.0, "gpu": 0.0},
        )

    def test_build_submit_payload_injects_run_name_for_resumeable_tasks(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    resume_run_name_arg: --experiment-name
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=["--lr", "0.001"],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        run_name = payload["submission_id"]
        self.assertIn(f"--experiment-name {run_name}", payload["entrypoint"])
        self.assertEqual(payload["metadata"]["run_name"], run_name)
        self.assertEqual(payload["metadata"]["resume_run_name_arg"], "--experiment-name")

    def test_build_submit_payload_uses_repo_submission_prefix(self) -> None:
        repo_root = self.make_repo()
        self.add_origin_remote(repo_root)
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  name: sample-repo
                  submission_prefix: octocat
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=[],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        self.assertRegex(
            payload["submission_id"],
            r"^octocat-sample-repo-sample-job-\d{8}-\d{6}$",
        )

    def test_build_submit_payload_cli_prefix_overrides_repo_prefix(self) -> None:
        repo_root = self.make_repo()
        self.add_origin_remote(repo_root)
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  name: sample-repo
                  submission_prefix: octocat
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=[],
            address="http://127.0.0.1:8265",
            name_override=None,
            submission_prefix_override="hubot",
        )

        self.assertRegex(
            payload["submission_id"],
            r"^hubot-sample-repo-sample-job-\d{8}-\d{6}$",
        )

    def test_build_submit_payload_uses_github_owner_from_origin_by_default(self) -> None:
        repo_root = self.make_repo()
        self.add_origin_remote(repo_root, "git@github.com:autobot/sample-repo.git")
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  name: sample-repo
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=[],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        self.assertRegex(
            payload["submission_id"],
            r"^autobot-sample-repo-sample-job-\d{8}-\d{6}$",
        )

    def test_build_submit_payload_strips_remainder_separator(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        repo_context = cli.load_repo_context(str(manifest_path), None)
        payload = cli.build_submit_payload(
            repo_context=repo_context,
            task_name="sample.job",
            task_cfg=repo_context.tasks["sample.job"],
            cli_extra_args=["--", "--epochs", "10"],
            address="http://127.0.0.1:8265",
            name_override=None,
        )

        self.assertEqual(
            payload["entrypoint"],
            f"cd {repo_root / 'nested' / 'job'} && python train.py --epochs 10",
        )

    def test_validate_rejects_cwd_outside_repo_root(self) -> None:
        repo_root = self.make_repo()
        outside_dir = repo_root.parent / f"{repo_root.name}-outside"
        outside_dir.mkdir()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                f"""
                version: 1
                tasks:
                  bad.job:
                    cwd: ../{outside_dir.name}
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        with self.assertRaises(ValueError):
            cli.run_validate(
                type(
                    "Args",
                    (),
                    {"manifest": str(manifest_path), "repo_root": None},
                )()
            )

    def test_submit_writes_initial_summary_without_wait(self) -> None:
        summary_dir = Path(tempfile.mkdtemp(prefix="gpulane-summary-test-"))
        summary_path = summary_dir / "job-summary.json"

        repo_context = type(
            "RepoContextStub",
            (),
            {
                "manifest_path": Path("/tmp/repo/ray_tasks.yaml"),
                "tasks": {"sample.job": {}},
            },
        )()
        payload = {
            "address": "http://127.0.0.1:8265",
            "submission_id": "sample-job-20260403-120000",
            "task": "sample.job",
            "entrypoint": "cd /tmp/repo && python train.py",
            "resources": {"cpu": 1.0, "gpu": 0.0},
            "entrypoint_resources": {"cpu": 1.0, "gpu": 0.0},
            "runtime_env": {"env_vars": {}},
            "metadata": {"task": "sample.job"},
        }

        args = type(
            "Args",
            (),
            {
                "manifest": None,
                "repo_root": None,
                "task": "sample.job",
                "extra_args": [],
                "address": "http://127.0.0.1:8265",
                "name": None,
                "submission_prefix": None,
                "dry_run": False,
                "wait": False,
                "poll_interval": 5.0,
                "log_path": None,
                "summary_path": str(summary_path),
                "print_logs": False,
            },
        )()

        client = mock.Mock()
        client.submit_job.return_value = "ray-job-123"
        client.get_job_status.return_value = "RUNNING"

        with (
            mock.patch.object(cli, "load_repo_context", return_value=repo_context),
            mock.patch.object(cli, "build_submit_payload", return_value=payload),
            mock.patch.object(cli, "get_job_submission_client", return_value=client),
            mock.patch.object(cli, "_handle_queue_on_submit", return_value=False),
        ):
            result = cli.run_submit(args)

        self.assertEqual(result, 0)
        summary = json.loads(summary_path.read_text())
        self.assertEqual(summary["job_id"], "ray-job-123")
        self.assertEqual(summary["address"], "http://127.0.0.1:8265")
        self.assertEqual(summary["task"], "sample.job")
        self.assertEqual(summary["submission_id"], "sample-job-20260403-120000")
        self.assertEqual(summary["job_status"], "RUNNING")
        self.assertIsNone(summary["final_status"])
        self.assertIsNone(summary["log_path"])
        self.assertEqual(summary["payload"], payload)
        self.assertIn("updated_at", summary)
        client.get_job_status.assert_called_once_with("ray-job-123")

    def test_submit_dry_run_does_not_create_output_dir(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                repo:
                  output_root: outputs
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                """
            ).strip()
            + "\n"
        )

        args = type(
            "Args",
            (),
            {
                "manifest": str(manifest_path),
                "repo_root": None,
                "task": "sample.job",
                "extra_args": [],
                "address": "http://127.0.0.1:8265",
                "name": None,
                "submission_prefix": None,
                "dry_run": True,
                "wait": False,
                "poll_interval": 5.0,
                "log_path": None,
                "summary_path": None,
                "print_logs": False,
            },
        )()

        result = cli.run_submit(args)

        self.assertEqual(result, 0)
        self.assertFalse((repo_root / "outputs").exists())

    def test_resolve_batch_job_specs_uses_default_summary_dir(self) -> None:
        batch_cfg = {
            "defaults": {
                "overrides": {"training.learning_rate": 0.001},
            },
            "jobs": [
                {
                    "task": "sample.job",
                    "name": "Trial A",
                    "extra_args": ["--", "--epochs", "1"],
                    "overrides": {"training.epochs": 10},
                },
                {"task": "sample.job"},
            ]
        }
        defaults = {
            "manifest": "/tmp/repo/ray_tasks.yaml",
            "repo_root": "/tmp/repo",
            "address": "http://127.0.0.1:8265",
            "submission_prefix": "octocat",
            "summary_dir": "/tmp/gpulane-batch",
            "overrides": {"training.learning_rate": 0.001},
        }

        specs = cli.resolve_batch_job_specs(batch_cfg, defaults)

        self.assertEqual(len(specs), 2)
        self.assertEqual(specs[0].task, "sample.job")
        self.assertEqual(specs[0].name, "Trial A")
        self.assertEqual(specs[0].submission_prefix, "octocat")
        self.assertEqual(specs[0].extra_args, ["--", "--epochs", "1"])
        self.assertEqual(
            specs[0].overrides,
            {"training.learning_rate": 0.001, "training.epochs": 10},
        )
        self.assertEqual(specs[1].overrides, {"training.learning_rate": 0.001})
        self.assertTrue(specs[0].summary_path.endswith("000-trial-a.json"))
        self.assertTrue(specs[1].summary_path.endswith("001-sample-job.json"))

    def test_run_batch_submit_submits_all_specs_and_waits_for_active_jobs(self) -> None:
        batch_dir = Path(tempfile.mkdtemp(prefix="gpulane-batch-test-"))
        batch_path = batch_dir / "batch.yaml"
        index_path = batch_dir / "summaries" / "index.json"
        batch_path.write_text(
            textwrap.dedent(
                f"""
                version: 1
                defaults:
                  address: http://127.0.0.1:8265
                  summary_dir: {batch_dir / 'summaries'}
                jobs:
                  - task: sample.job
                    name: first
                  - task: sample.job
                    name: second
                """
            ).strip()
            + "\n"
        )

        args = type(
            "Args",
            (),
            {
                "file": str(batch_path),
                "dry_run": False,
                "wait": True,
                "poll_interval": 0.0,
            },
        )()

        submit_results = [
            {
                "job_id": "job-1",
                "status": "RUNNING",
                "payload": {"address": "http://127.0.0.1:8265", "submission_id": "sub-1"},
            },
            {
                "job_id": "job-2",
                "status": "RUNNING",
                "payload": {"address": "http://127.0.0.1:8265", "submission_id": "sub-2"},
            },
        ]
        poll_responses = [True, True]

        def fake_submit(spec, *, address, dry_run):
            self.assertEqual(address, "http://127.0.0.1:8265")
            self.assertFalse(dry_run)
            return submit_results.pop(0)

        def fake_poll(active_jobs, *, client_by_address):
            active_jobs[0]["status"] = "SUCCEEDED"
            return poll_responses.pop(0)

        with (
            mock.patch.object(cli, "submit_batch_job", side_effect=fake_submit) as submit_mock,
            mock.patch.object(cli, "poll_batch_jobs", side_effect=fake_poll) as poll_mock,
        ):
            result = cli.run_batch_submit(args)

        self.assertEqual(result, 0)
        self.assertEqual(submit_mock.call_count, 2)
        self.assertEqual(poll_mock.call_count, 2)
        index = json.loads(index_path.read_text())
        self.assertEqual(index["counts"]["succeeded"], 2)
        self.assertEqual(len(index["jobs"]), 2)

    def test_submit_batch_job_defers_instead_of_submitting_to_ray(self) -> None:
        repo_context = type(
            "RepoContextStub",
            (),
            {
                "manifest_path": Path("/tmp/repo/ray_tasks.yaml"),
                "tasks": {"sample.job": {}},
            },
        )()
        spec = cli.BatchJobSpec(
            task="sample.job",
            name="trial-a",
            submission_prefix=None,
            extra_args=[],
            overrides={},
            manifest="/tmp/repo/ray_tasks.yaml",
            repo_root="/tmp/repo",
            summary_path=None,
        )
        payload = {
            "address": "http://127.0.0.1:8265",
            "submission_id": "sample-job-20260410-170000",
            "task": "sample.job",
            "entrypoint": "cd /tmp/repo && python train.py",
            "resources": {"cpu": 1.0, "gpu": 1.0},
            "entrypoint_resources": {"cpu": 1.0, "gpu": 1.0},
            "runtime_env": {"env_vars": {}},
            "metadata": {"task": "sample.job"},
        }

        with (
            mock.patch.object(cli, "get_job_submission_client") as client_mock,
            mock.patch("gpulane.batch.load_repo_context", return_value=repo_context),
            mock.patch("gpulane.batch.build_submit_payload", return_value=payload),
            mock.patch("gpulane.batch._handle_queue_on_submit", return_value=True),
        ):
            result = cli.submit_batch_job(
                spec,
                address="http://127.0.0.1:8265",
                dry_run=False,
            )

        client_mock.assert_not_called()
        self.assertIsNone(result["job_id"])
        self.assertEqual(result["status"], "DEFERRED")
        self.assertEqual(result["payload"], payload)

    def test_submit_batch_job_materializes_overridden_config(self) -> None:
        repo_root = self.make_repo()
        manifest_path = repo_root / "ray_tasks.yaml"
        config_path = repo_root / "nested" / "job" / "config.yaml"
        config_path.write_text(
            textwrap.dedent(
                """
                training:
                  learning_rate: 0.01
                  epochs: 5
                """
            ).strip()
            + "\n"
        )
        manifest_path.write_text(
            textwrap.dedent(
                """
                version: 1
                tasks:
                  sample.job:
                    cwd: nested/job
                    command: ["python", "train.py"]
                    default_args: ["--config", "config.yaml"]
                """
            ).strip()
            + "\n"
        )
        spec = cli.BatchJobSpec(
            task="sample.job",
            name="trial-a",
            submission_prefix=None,
            extra_args=[],
            overrides={"training.learning_rate": 0.001},
            manifest=str(manifest_path),
            repo_root=None,
            summary_path=None,
        )
        captured = {}

        def fake_build_submit_payload(**kwargs):
            captured["cli_extra_args"] = list(kwargs["cli_extra_args"])
            return {
                "address": kwargs["address"],
                "submission_id": "sample-job-20260410-170000",
                "task": kwargs["task_name"],
                "entrypoint": "cd /tmp/repo && python train.py",
                "resources": {"cpu": 1.0, "gpu": 0.0},
                "entrypoint_resources": {"cpu": 1.0, "gpu": 0.0},
                "runtime_env": {"env_vars": {}},
                "metadata": {"task": kwargs["task_name"]},
            }

        with mock.patch("gpulane.batch.build_submit_payload", side_effect=fake_build_submit_payload):
            result = cli.submit_batch_job(spec, address="http://127.0.0.1:8265", dry_run=True)

        self.assertEqual(result["status"], "DRY_RUN")
        generated_config = Path(captured["cli_extra_args"][1])
        self.assertTrue(generated_config.exists())
        overridden = cli.load_yaml(generated_config)
        self.assertEqual(overridden["training"]["learning_rate"], 0.001)
        self.assertEqual(overridden["training"]["epochs"], 5)


class PriorityQueueTest(unittest.TestCase):
    """Tests for the priority-based preemption and queue management logic."""

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _make_active_job(
        self,
        priority: str,
        job_id: str = "job-001",
        gpu: float = 1.0,
        started_at: str = "",
    ) -> dict:
        return {"job_id": job_id, "status": "RUNNING", "priority": priority, "metadata": {
            "priority": priority, "task": f"task-{priority}", "manifest_path": None,
            "repo_root": None, "extra_args_json": "[]",
            "requested_resources_json": json.dumps({"gpu": gpu}),
        }, "requested_resources": {"gpu": gpu}, "started_at": started_at}

    def _make_task_cfg(self, priority: str, gpu: float = 1.0) -> dict:
        return {"priority": priority, "resources": {"gpu": gpu}}

    def _make_args(self, task: str = "my-task", address: str = "http://127.0.0.1:8265") -> object:
        return type("Args", (), {
            "task": task, "address": address, "manifest": None, "repo_root": None,
            "extra_args": [], "name": None, "submission_prefix": None, "summary_path": None,
        })()

    def _no_redis_kafka(self):
        """Patch out Redis and Kafka so tests run without those services."""
        stack = ExitStack()
        stack.enter_context(
            mock.patch.multiple(
                "gpulane.queue.backend",
                _redis_client=mock.Mock(return_value=None),
                _kafka_producer=mock.Mock(return_value=None),
            )
        )
        stack.enter_context(
            mock.patch.multiple(
                "gpulane.queue.scheduler",
                _kafka_producer=mock.Mock(return_value=None),
            )
        )
        return stack

    def test_redis_client_raises_when_configured_service_is_unavailable(self):
        import gpulane.queue.backend as backend_module

        fake_redis = mock.Mock()
        fake_redis.from_url.side_effect = OSError("connection refused")
        with (
            mock.patch.dict(os.environ, {"GPULANE_REDIS_URL": "redis://localhost:6379"}, clear=False),
            mock.patch.dict("sys.modules", {"redis": fake_redis}),
            self.assertRaises(RuntimeError),
        ):
            backend_module._redis_client()

    def test_kafka_producer_raises_when_configured_service_is_unavailable(self):
        import gpulane.queue.backend as backend_module

        fake_producer_instance = mock.Mock()
        fake_producer_instance.bootstrap_connected.return_value = False
        fake_kafka_module = mock.Mock(KafkaProducer=mock.Mock(return_value=fake_producer_instance))

        with (
            mock.patch.dict(os.environ, {"GPULANE_KAFKA_BROKERS": "localhost:9092"}, clear=False),
            mock.patch.dict("sys.modules", {"kafka": fake_kafka_module}),
            self.assertRaises(RuntimeError),
        ):
            backend_module._kafka_producer()

        fake_producer_instance.close.assert_called_once_with(timeout=2)

    def test_local_queue_backend_round_trips_jobs_in_priority_order(self):
        import gpulane.queue.backend as backend_module

        now = "2026-04-07 10:00:00"
        jobs = [
            cli.DeferredJob(
                "low",
                "http://x",
                "exp",
                None,
                None,
                [],
                None,
                None,
                None,
                now,
                "queued",
                requested_resources={"gpu": 1.0},
            ),
            cli.DeferredJob(
                "normal",
                "http://x",
                "label",
                None,
                None,
                [],
                None,
                None,
                None,
                now,
                "queued",
                requested_resources={"gpu": 1.0},
            ),
        ]

        with tempfile.TemporaryDirectory() as tmp:
            queue_path = Path(tmp) / "queue.json"
            with (
                mock.patch.dict(
                    os.environ,
                    {
                        "GPULANE_QUEUE_BACKEND": "local",
                        "GPULANE_LOCAL_QUEUE_STATE_PATH": str(queue_path),
                    },
                    clear=False,
                ),
            ):
                cli.save_deferred(jobs)
                loaded = cli.load_deferred()

        self.assertEqual([job.task for job in loaded], ["label", "exp"])

    # ------------------------------------------------------------------
    # _handle_queue_on_submit
    # ------------------------------------------------------------------

    def test_gpu_available_submits_directly_regardless_of_priority(self):
        """When free GPUs >= required, submit immediately — no deferral, no preemption."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("low", gpu=2.0)
        active = [self._make_active_job("high")]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 4.0}), \
             mock.patch.object(scheduler_module, "_defer_job") as defer_mock, \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock:

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        defer_mock.assert_not_called()
        preempt_mock.assert_not_called()

    def test_gpu_full_higher_priority_running_defers_self(self):
        """GPU full + higher-priority job running → current job defers."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("normal", gpu=2.0)
        active = [self._make_active_job("high", "high-001")]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_defer_job") as defer_mock, \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertTrue(result)  # deferred, caller must not submit
        defer_mock.assert_called_once()
        self.assertEqual(defer_mock.call_args[0][1], "normal")
        self.assertEqual(defer_mock.call_args[1]["reason"], "queued")
        preempt_mock.assert_not_called()

    def test_gpu_full_lower_priority_running_preempts_only_enough_jobs(self):
        """GPU full + lower-priority jobs running → preempt only enough jobs to cover the deficit."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [
            self._make_active_job("low", "low-001", gpu=1.0),
            self._make_active_job("low", "low-002", gpu=1.0),
            self._make_active_job("low", "low-003", gpu=1.0),
            self._make_active_job("low", "low-004", gpu=1.0),
        ]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=1.0), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)  # not deferred — caller submits self
        preempt_mock.assert_called_once()
        self.assertEqual(preempt_mock.call_args[0][1]["job_id"], "low-004")

    def test_gpu_full_missing_gpu_metadata_does_not_preempt_every_lower_priority_job(self):
        """If running jobs lack requested_resources, preemption should re-check cluster capacity after each stop."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [
            {
                **self._make_active_job("low", "low-001", gpu=1.0, started_at="2026-04-09T10:44:27"),
                "requested_resources": {},
            },
            {
                **self._make_active_job("low", "low-002", gpu=1.0, started_at="2026-04-09T10:44:28"),
                "requested_resources": {},
            },
            {
                **self._make_active_job("low", "low-003", gpu=1.0, started_at="2026-04-09T10:44:30"),
                "requested_resources": {},
            },
            {
                **self._make_active_job("low", "low-004", gpu=1.0, started_at="2026-04-09T10:44:31"),
                "requested_resources": {},
            },
        ]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=1.0), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        preempt_mock.assert_called_once()
        self.assertEqual(preempt_mock.call_args[0][1]["job_id"], "low-004")

    def test_gpu_release_wait_prevents_extra_preemption_after_minimum_batch(self):
        """After preempting the minimum required batch, wait for resource release before expanding to more victims."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [
            self._make_active_job("low", "low-001", gpu=1.0, started_at="2026-04-09T11:40:27"),
            self._make_active_job("low", "low-002", gpu=1.0, started_at="2026-04-09T11:40:28"),
            self._make_active_job("low", "low-003", gpu=1.0, started_at="2026-04-09T11:40:29"),
            self._make_active_job("low", "low-004", gpu=1.0, started_at="2026-04-09T11:40:30"),
        ]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=1.0) as wait_mock, \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        preempt_mock.assert_called_once()
        wait_mock.assert_called_once_with("http://127.0.0.1:8265", required_gpu=1.0)

    def test_gpu_release_wait_can_trigger_second_batch_after_timeout(self):
        """If capacity is still insufficient after waiting, a second minimum batch may be preempted."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [
            self._make_active_job("low", "low-001", gpu=1.0, started_at="2026-04-09T11:40:27"),
            self._make_active_job("low", "low-002", gpu=1.0, started_at="2026-04-09T11:40:28"),
            self._make_active_job("low", "low-003", gpu=1.0, started_at="2026-04-09T11:40:29"),
        ]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", side_effect=[0.0, 1.0]) as wait_mock, \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        self.assertEqual(preempt_mock.call_count, 2)
        self.assertEqual(
            [call.args[1]["job_id"] for call in preempt_mock.call_args_list],
            ["low-003", "low-002"],
        )
        self.assertEqual(wait_mock.call_count, 2)

    def test_high_preempts_normal(self):
        """A higher-priority tier preempts a lower one."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=2.0)
        active = [self._make_active_job("normal", "normal-001")]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=2.0), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        preempt_mock.assert_called_once()
        self.assertEqual(preempt_mock.call_args[0][1]["job_id"], "normal-001")

    def test_gpu_full_resource_check_failure_falls_back_to_preempt(self):
        """If resource query fails (returns {}), treat as GPU full and apply priority logic."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [self._make_active_job("low", "low-001")]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=1.0), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            cli._handle_queue_on_submit(self._make_args(), task_cfg)

        preempt_mock.assert_called_once()

    def test_available_gpu_is_not_double_counted(self):
        """The Ray-reported available GPU count is already net free capacity and must not be reduced again."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("low", gpu=4.0)
        active = [self._make_active_job("high", "high-001", gpu=2.0)]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 4.0}), \
             mock.patch.object(scheduler_module, "_defer_job") as defer_mock, \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        defer_mock.assert_not_called()
        preempt_mock.assert_not_called()

    def test_preempt_prefers_lower_priority_before_higher_priority_and_later_start_within_priority(self):
        """Victim selection should prefer the lowest-priority running jobs, and among ties, the latest-started one."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("high", gpu=1.0)
        active = [
            self._make_active_job("normal", "normal-001", gpu=1.0, started_at="2026-04-08T10:00:00"),
            self._make_active_job("low", "low-older", gpu=1.0, started_at="2026-04-08T10:01:00"),
            self._make_active_job("low", "low-newer", gpu=1.0, started_at="2026-04-08T10:02:00"),
        ]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_wait_for_available_gpu", return_value=1.0), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertFalse(result)
        preempt_mock.assert_called_once()
        self.assertEqual(preempt_mock.call_args[0][1]["job_id"], "low-newer")

    def test_gpu_full_same_priority_running_defers_self(self):
        """GPU full + same-priority active jobs should defer rather than bypass the queue."""
        import gpulane.queue.scheduler as scheduler_module
        task_cfg = self._make_task_cfg("low", gpu=1.0)
        active = [self._make_active_job("low", "low-001", gpu=1.0)]

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "get_job_submission_client"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=active), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 0.0}), \
             mock.patch.object(scheduler_module, "_defer_job") as defer_mock, \
             mock.patch.object(scheduler_module, "_preempt_job") as preempt_mock, \
             mock.patch.object(scheduler_module, "_ensure_watcher_running"):

            result = cli._handle_queue_on_submit(self._make_args(), task_cfg)

        self.assertTrue(result)
        defer_mock.assert_called_once()
        preempt_mock.assert_not_called()

    # ------------------------------------------------------------------
    # _preempt_job: stop confirmation
    # ------------------------------------------------------------------

    def test_preempt_job_waits_for_stopped_state(self):
        """_preempt_job polls until the job reaches a terminal state before returning."""
        import gpulane.queue.scheduler as scheduler_module
        client = mock.Mock()
        client.get_job_status.side_effect = ["RUNNING", "RUNNING", "STOPPED"]

        job_info = {
            "job_id": "low-001",
            "priority": "low",
            "metadata": {"priority": "low", "task": "exp", "manifest_path": None,
                         "repo_root": None, "extra_args_json": "[]"},
        }

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "_defer_job"), \
             mock.patch.object(scheduler_module, "_publish_event"), \
             mock.patch("time.sleep"):
            cli._preempt_job(client, job_info, "http://127.0.0.1:8265")

        client.stop_job.assert_called_once_with("low-001")
        self.assertEqual(client.get_job_status.call_count, 3)

    def test_preempt_job_preserves_actual_priority(self):
        """Preempted job is stored with its actual priority, not hardcoded to the default."""
        import gpulane.queue.scheduler as scheduler_module
        client = mock.Mock()
        client.get_job_status.return_value = "STOPPED"

        job_info = {
            "job_id": "label-001",
            "priority": "normal",
            "metadata": {"priority": "normal", "task": "label", "manifest_path": None,
                         "repo_root": None, "extra_args_json": "[]"},
        }

        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "_defer_job") as defer_mock, \
             mock.patch.object(scheduler_module, "_publish_event"), \
             mock.patch("time.sleep"):
            cli._preempt_job(client, job_info, "http://127.0.0.1:8265")

        self.assertEqual(defer_mock.call_args[0][1], "normal")

    # ------------------------------------------------------------------
    # process_queue: resume ordering
    # ------------------------------------------------------------------

    def test_process_queue_resumes_highest_priority_first(self):
        """When multiple jobs are deferred, the highest-priority one is resumed first."""
        now = "2026-04-07 10:00:00"
        deferred = [
            cli.DeferredJob("low", "http://x", "exp", None, None, [], None, None, None, now, "preempted", requested_resources={"gpu": 1.0}),
            cli.DeferredJob("normal", "http://x", "label", None, None, [], None, None, None, now, "preempted", requested_resources={"gpu": 1.0}),
        ]
        client = mock.Mock()
        resumed = []

        def fake_submit(args, _skip_queue=False):
            resumed.append(args.task)
            return 0

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 1.0}):

            process_queue(client, fake_submit)

        self.assertEqual(resumed, ["label"])  # normal first; low blocked by updated min_active_level

    def test_process_queue_blocks_lower_priority_after_resuming_higher(self):
        """After resuming normal, low must stay deferred (min_active_level updated)."""
        now = "2026-04-07 10:00:00"
        deferred = [
            cli.DeferredJob("normal", "http://x", "label", None, None, [], None, None, None, now, "preempted", requested_resources={"gpu": 1.0}),
            cli.DeferredJob("low", "http://x", "exp", None, None, [], None, None, None, now, "preempted", requested_resources={"gpu": 1.0}),
        ]
        client = mock.Mock()
        remaining_saved = []

        def fake_save(jobs, *_):
            remaining_saved.extend(jobs)

        def fake_submit(args, _skip_queue=False):
            return 0

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred", side_effect=fake_save), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 1.0}):

            process_queue(client, fake_submit)

        remaining_priorities = [j.priority for j in remaining_saved]
        self.assertIn("low", remaining_priorities)
        self.assertNotIn("normal", remaining_priorities)

    def test_process_queue_appends_resume_for_preempted_jobs(self):
        """Preempted jobs should be resubmitted with --resume exactly once."""
        now = "2026-04-07 10:00:00"
        deferred = [
            cli.DeferredJob("low", "http://x", "exp", None, None, [], None, None, None, now, "preempted", original_job_id="job-123", requested_resources={"gpu": 1.0}),
        ]
        client = mock.Mock()
        seen_extra_args = []

        def fake_submit(args, _skip_queue=False):
            seen_extra_args.append(list(args.extra_args))
            return 0

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 1.0}):

            process_queue(client, fake_submit)

        self.assertEqual(seen_extra_args, [["--resume", "--resume-job-id", "job-123"]])

    def test_process_queue_prefers_run_name_for_preempted_jobs(self):
        now = "2026-04-07 10:00:00"
        deferred = [
            cli.DeferredJob(
                "low",
                "http://x",
                "exp",
                None,
                None,
                [],
                None,
                None,
                None,
                now,
                "preempted",
                original_job_id="job-123",
                run_name="stable-run-001",
                resume_run_name_arg="--experiment-name",
                requested_resources={"gpu": 1.0},
            ),
        ]
        client = mock.Mock()
        seen_extra_args = []

        def fake_submit(args, _skip_queue=False):
            seen_extra_args.append(list(args.extra_args))
            return 0

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 1.0}):

            process_queue(client, fake_submit)

        self.assertEqual(seen_extra_args, [["--resume", "--experiment-name", "stable-run-001"]])

    def test_process_queue_does_nothing_when_higher_priority_still_active(self):
        """Deferred normal must not resume while high is still running."""
        now = "2026-04-07 10:00:00"
        deferred = [
            cli.DeferredJob("normal", "http://x", "label", None, None, [], None, None, None, now, "queued", requested_resources={"gpu": 1.0}),
        ]
        client = mock.Mock()
        submit_mock = mock.Mock(return_value=0)

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[
                 self._make_active_job("high", "high-001")
             ]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 3.0}):

            process_queue(client, submit_mock)

        submit_mock.assert_not_called()

    def test_process_queue_resumes_same_priority_job_when_gpu_frees_up(self):
        """A preempted job should resume once there is capacity, even if active jobs are the same priority."""
        now = "2026-04-08 16:40:10"
        deferred = [
            cli.DeferredJob("low", "http://x", "exp", None, None, [], None, None, None, now, "preempted", original_job_id="job-123", requested_resources={"gpu": 1.0}),
        ]
        client = mock.Mock()
        resumed = []

        def fake_submit(args, _skip_queue=False):
            resumed.append((args.task, list(args.extra_args)))
            return 0

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred"), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[
                 self._make_active_job("low", "low-1", gpu=1.0),
                 self._make_active_job("low", "low-2", gpu=1.0),
                 self._make_active_job("low", "low-3", gpu=1.0),
             ]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"GPU": 1.0}):

            process_queue(client, fake_submit)

        self.assertEqual(resumed, [("exp", ["--resume", "--resume-job-id", "job-123"])])

    def test_process_queue_does_not_resume_when_cpu_is_insufficient(self):
        """A deferred job must stay queued when GPU is free but CPU is still insufficient."""
        now = "2026-04-09 12:10:00"
        deferred = [
            cli.DeferredJob(
                "low",
                "http://x",
                "exp",
                None,
                None,
                [],
                None,
                None,
                None,
                now,
                "preempted",
                original_job_id="job-123",
                requested_resources={"cpu": 8.0, "gpu": 1.0},
            ),
        ]
        client = mock.Mock()
        submit_mock = mock.Mock(return_value=0)
        saved_jobs = []

        def fake_save(jobs, *_):
            saved_jobs.extend(jobs)

        import gpulane.queue.scheduler as scheduler_module
        with self._no_redis_kafka(), \
             mock.patch.object(scheduler_module, "load_deferred", return_value=deferred), \
             mock.patch.object(scheduler_module, "save_deferred", side_effect=fake_save), \
             mock.patch.object(scheduler_module, "get_active_ray_jobs", return_value=[]), \
             mock.patch.object(scheduler_module, "get_ray_available_resources", return_value={"CPU": 4.0, "GPU": 1.0}):

            process_queue(client, submit_mock)

        submit_mock.assert_not_called()
        self.assertEqual(saved_jobs, [])

    def test_save_load_deferred_uses_redis_only(self):
        import gpulane.queue.backend as backend_module

        fake_pipeline = mock.Mock()
        fake_redis = mock.Mock()
        fake_redis.pipeline.return_value = fake_pipeline
        fake_redis.zrange.return_value = [
            json.dumps(
                {
                    "priority": "low",
                    "address": "http://127.0.0.1:8265",
                    "task": "exp",
                    "manifest": "/a/b.yaml",
                    "repo_root": "/a",
                    "extra_args": ["--lr", "0.01"],
                    "name": "my-run",
                    "submission_prefix": "elaine",
                    "summary_path": "/out/s.json",
                    "deferred_at": "2026-04-07 10:00:00",
                    "reason": "preempted",
                    "original_job_id": "ray-job-abc",
                }
            )
        ]
        job = cli.DeferredJob(
            priority="low", address="http://127.0.0.1:8265",
            task="exp", manifest="/a/b.yaml", repo_root="/a",
            extra_args=["--lr", "0.01"], name="my-run",
            submission_prefix="elaine", summary_path="/out/s.json",
            deferred_at="2026-04-07 10:00:00", reason="preempted",
            original_job_id="ray-job-abc",
        )

        with mock.patch.object(backend_module, "_redis_client", return_value=fake_redis):
            cli.save_deferred([job])
            loaded = cli.load_deferred()

        fake_pipeline.delete.assert_called_once()
        fake_pipeline.zadd.assert_called_once()
        fake_pipeline.execute.assert_called_once()
        self.assertEqual(len(loaded), 1)
        self.assertEqual(loaded[0].priority, "low")
        self.assertEqual(loaded[0].extra_args, ["--lr", "0.01"])
        self.assertEqual(loaded[0].original_job_id, "ray-job-abc")

    # ------------------------------------------------------------------
    # _redis_score: ordering correctness
    # ------------------------------------------------------------------

    def test_redis_score_priority_order(self):
        """high must have a lower score than normal and low."""
        ts = "2026-04-07 10:00:00"
        s_high = cli._redis_score("high", ts)
        s_normal = cli._redis_score("normal", ts)
        s_low = cli._redis_score("low", ts)

        self.assertLess(s_high, s_normal)
        self.assertLess(s_normal, s_low)

    def test_redis_score_fifo_within_same_priority(self):
        """Two low-priority jobs: the earlier one must have a lower score."""
        s_early = cli._redis_score("low", "2026-04-07 09:00:00")
        s_late = cli._redis_score("low", "2026-04-07 10:00:00")
        self.assertLess(s_early, s_late)


if __name__ == "__main__":
    unittest.main()
