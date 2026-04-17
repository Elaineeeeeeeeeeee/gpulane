# Architecture

`gpulane` is a thin layer on top of Ray Jobs for teams sharing a single GPU workstation or a small shared host.

## Core Components

- `ray_tasks.yaml`: repo-local task registry
- `gpulane` CLI: validation, resolution, submission, queue operations
- Ray Jobs API: remote job submission and status/log access
- queue backend: Redis for wake-ups or local file storage for lightweight evaluation
- Kafka: optional event publication for observability

## Submission Flow

1. The user runs `gpulane submit` or `gpulane batch submit`.
2. `gpulane` discovers and validates the repo-local manifest.
3. The CLI resolves the task entrypoint, resources, metadata, environment variables, and output path.
4. If queueing is enabled and GPUs are constrained, the scheduler policy decides whether to:
   - submit immediately
   - preempt lower-priority jobs
   - defer the current job into the queue
5. The final payload is submitted through the Ray Jobs API.

## Queue And Preemption Model

Priority is encoded as an ordered set of policy levels.

The default order is:

- `high`
- `normal`
- `low`

The order can be replaced with any comma-separated list through `GPULANE_PRIORITY_LEVELS`.

Lower numeric priority values are treated as more important. When GPUs are full:

- higher-priority jobs can preempt lower-priority jobs
- jobs of the same priority do not preempt one another
- the lowest-priority tier never preempts higher-priority work

Deferred jobs are stored in either Redis or local queue state and reconsidered by the queue watcher when:

- a job is preempted
- resources are freed
- a new event wakes the watcher via Redis pub/sub, or polling detects capacity changes

## Current Tradeoffs

This design is intentionally narrow:

- it favors inspectability over abstraction
- it assumes one shared Ray cluster per host
- it does not provide strong tenant isolation
- it does not aim to replace large-scale cluster schedulers

## Remaining Work

- reduce compatibility wrappers in `cli.py` as tests move to newer boundaries
- add stronger public-facing demo assets such as GIFs or screen recordings
- document real deployment patterns once outside users adopt the tool
