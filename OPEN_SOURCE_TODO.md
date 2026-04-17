# Open Source TODO

This file tracks the staged work needed to turn `gpulane` into a public, contributor-friendly open source project.

## Status legend

- `[ ]` pending
- `[~]` in progress
- `[x]` done

## Phase 1: Repository trust and packaging

- [x] Add an in-repo roadmap for open source hardening
- [x] Add `LICENSE`
- [x] Expand package metadata in `pyproject.toml`
- [x] Add `CONTRIBUTING.md`
- [x] Add `SECURITY.md`
- [x] Add GitHub issue templates
- [x] Add a pull request template
- [x] Add baseline CI for tests and packaging checks

## Phase 2: Product positioning and onboarding

- [x] Rewrite `README.md` as a public project landing page
- [x] Add a concise quickstart that works for first-time users
- [x] Document supported and unsupported deployment models
- [x] Add an architecture section with queueing and preemption flow
- [x] Add a comparison section against raw Ray Jobs and heavier schedulers
- [x] Add `examples/` with minimal manifests and batch files

## Phase 3: CLI and architecture refactor

- [x] Split parser wiring from command execution in `gpulane/cli.py`
- [x] Introduce `gpulane/commands/` for subcommand handlers
- [x] Introduce a `core` layer for manifest resolution and submission payloads
- [x] Extract queue and scheduler policy from command handlers
- [x] Reduce direct global config coupling
- [x] Define clearer domain models for task specs and submission payloads

## Phase 4: Backend abstraction and developer ergonomics

- [x] Separate Ray, Redis, and Kafka integrations behind backend interfaces
- [x] Support a local or no-op queue backend for easier evaluation
- [x] Add schema-driven validation for `ray_tasks.yaml`
- [x] Add a `gpulane init` or equivalent manifest bootstrap command
- [x] Add more targeted unit tests around queue policy and payload generation

## Phase 5: Growth and adoption

- [x] Add a changelog and release process
- [x] Add a non-publish package verification path
- [x] Publish example workflows and demo-ready docs
- [x] Add benchmark or case-study documentation scaffolding
- [ ] Collect and document real deployment patterns from users

## Current tranche

The current implementation tranche focuses on Phase 1:

1. Create this roadmap.
2. Add missing open source project files.
3. Add basic CI so the repository looks alive and reliable to outside users.
4. Leave license selection explicit rather than guessing the maintainer's legal intent.

## Notes

- Keep changes incremental and low-risk while the repo already has local uncommitted work.
- Prefer changes that improve public trust and contributor onboarding before deeper refactors.
- Current command extraction status:
  - repo commands moved to `gpulane/commands/repo.py`
  - job inspection commands moved to `gpulane/commands/jobs.py`
  - submit flow moved to `gpulane/commands/submit.py`
  - batch submit flow moved to `gpulane/commands/batch.py`
  - queue commands moved to `gpulane/commands/queue.py`
  - setup-stack flow moved to `gpulane/commands/setup.py`
  - batch config parsing moved to `gpulane/core/batch_config.py`
  - parser construction moved to `gpulane/core/parser.py`
  - submission payload assembly moved to `gpulane/core/submission.py`
  - manifest loading and schema validation moved to `gpulane/core/manifest_loader.py` and `gpulane/core/validation.py`
  - public docs were consolidated into `docs/USAGE.md`, `docs/ARCHITECTURE.md`, and `docs/RELEASING.md`
  - runtime queue and Ray configuration now flow through `gpulane/settings.py`
  - injected runtime metadata now uses `GPULANE_*` environment variables rather than `WORKSTATION_*`
  - `gpulane/cli.py` still provides compatibility wrappers for tests and queue callbacks
