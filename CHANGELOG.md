# Changelog

All notable changes to `gpulane` will be documented in this file.

The format follows Keep a Changelog principles and uses semantic versioning.

## [Unreleased]

### Added

- Public-facing project documentation, examples, and architecture notes.
- Baseline CI, issue templates, pull request template, and contributor docs.
- Explicit manifest schema validation for `ray_tasks.yaml`.
- Internal `commands/` and `core/` modules to reduce `cli.py` coupling.
- Domain dataclasses for `TaskSpec`, `ResourceSpec`, and `SubmissionPayload`.

### Changed

- Manifest loading now validates root, repo, task, and resource schema before command execution.
- Submission prefix detection now prefers GitHub login and remote owner metadata before local git user name.
- Batch override files now fall back to a writable temporary directory when `~/.gpulane` is unavailable.

## [0.1.1] - 2026-04-16

### Added

- Initial public packaging metadata and installable console entrypoint.
