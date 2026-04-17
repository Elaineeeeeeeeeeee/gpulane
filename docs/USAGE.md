# Usage Guide

This guide collects the practical parts of the project in one place: when to use `gpulane`, how to try it quickly, and how it compares to adjacent tools.

## Best Fit

`gpulane` is a good fit when:

- one shared workstation or small VM owns the available GPUs
- multiple repos or users need one submission contract
- the team wants something lighter than Slurm or Kubernetes

It is not a good fit when:

- you need multi-node scheduling across many machines
- you need strong tenant isolation or quota management
- your organization already operates a mature scheduler

## Queue Backends

### Local queue backend

Fastest path for evaluation or solo use:

```bash
export GPULANE_QUEUE_BACKEND=local
```

This stores deferred jobs in `~/.gpulane/queue.json` and uses polling for queue wake-ups.

### Redis-backed queue backend

Better fit for a shared host with multiple users:

```bash
gpulane setup-stack
```

This gives you:

- deferred scheduling
- preemption support
- Redis pub/sub wake-ups for the queue watcher

## Common Workflows

### First-time evaluation

```bash
export GPULANE_QUEUE_BACKEND=local
gpulane init --task-name example.train -- -- python train.py
gpulane validate --manifest ray_tasks.yaml
gpulane doctor --manifest ray_tasks.yaml
```

### Shared workstation flow

```bash
gpulane setup-stack
gpulane doctor --manifest ray_tasks.yaml --task train.default
gpulane submit --manifest ray_tasks.yaml --task train.default
```

### Batch sweep

```bash
gpulane batch submit --file examples/batch/batch.yaml --wait
```

### Queue recovery after preemption

When a higher-priority job interrupts a lower-priority one:

- the lower-priority job is deferred
- `gpulane queue watch` or `gpulane queue process` resumes it later

## FAQ

### Why not just use `ray job submit`?

Raw Ray Jobs may be enough when one repo owns one machine and everyone is comfortable writing submission commands directly.

`gpulane` adds:

- repo-local manifests
- validation of task definitions
- standard metadata and summaries
- priority-aware queueing and preemption

The default labels are `high`, `normal`, and `low`, but that is only the default. You can define any ordered set with:

```bash
export GPULANE_PRIORITY_LEVELS=urgent,standard,background,bulk
```

### Do I need Redis?

No. Redis is optional. Use the local queue backend for first-time evaluation.

### Is this a cluster scheduler?

No. It is a lightweight control layer for one shared workstation or a small shared host.

### Can I verify packaging without uploading to PyPI?

Yes:

```bash
make release-check
```

## Comparisons

### `gpulane` vs raw Ray Jobs

Use raw Ray Jobs when:

- one repo owns its own submit flow
- there is no need for shared scheduling rules

Use `gpulane` when:

- several repos share one machine
- you want one manifest contract
- you need explicit priority and queue behavior

### `gpulane` vs Slurm

Use Slurm when:

- you operate a real cluster
- you need quotas, partitions, and mature scheduling guarantees

Use `gpulane` when:

- the real problem is "one shared GPU box is chaotic"
- you want something inspectable and lightweight

### `gpulane` vs Kubernetes-native job platforms

Kubernetes-native systems are a better fit when you need:

- container orchestration at scale
- autoscaling fleets
- strong isolation boundaries

`gpulane` is the lower-ceremony option when you do not want to become a platform team just to share one GPU machine.

## Sharing The Project

If you want to record a short demo, keep it simple:

1. `gpulane init`
2. `gpulane validate`
3. `gpulane doctor`
4. `gpulane submit --dry-run`

If you later want to document a real adoption story, capture:

- the environment
- how many repos share the host
- which queue backend is used
- what operational pain it replaced
