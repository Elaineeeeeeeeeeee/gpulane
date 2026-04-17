# gpulane

`gpulane` is a lightweight control plane for running ML jobs across a shared GPU workstation with Ray Jobs, manifest-based task definitions, and priority-aware queueing.

It is aimed at small ML teams that are not ready to run Kubernetes or Slurm, but still need something stricter than ad hoc shell scripts.

## Why This Exists

Shared GPU machines usually break down in the same ways:

- teams launch jobs from different repos with no common submission contract
- GPU allocation gets hard-coded in scripts instead of declared as resources
- urgent production work competes with background research with no clear priority rules
- there is no consistent metadata, output layout, or job summary record

`gpulane` adds a thin control layer on top of Ray Jobs:

- repo-local `ray_tasks.yaml` manifests
- validated task definitions
- consistent submission metadata
- optional deferred queue with Redis or local file storage
- priority-based preemption for shared GPU capacity
- batch submission with config overrides

## Good Fit

- one shared workstation or small EC2 box with CPUs and GPUs
- multiple repos owned by one team or adjacent teams
- users already comfortable with Python and Ray Jobs
- environments where "simple and inspectable" matters more than full cluster orchestration

## Not A Good Fit

- multi-node production scheduling across many machines
- strict multi-tenant isolation requirements
- organizations that already operate Kubernetes, Slurm, or a mature internal scheduler

## Quick Demo

```bash
gpulane init --task-name train.default -- -- python train.py
gpulane list --manifest ray_tasks.yaml
gpulane doctor --manifest ray_tasks.yaml --task train.default
gpulane submit --manifest ray_tasks.yaml --task train.default --wait
```

## Install

```bash
pip install "gpulane @ git+https://github.com/Elaineeeeeeeeeeee/gpulane.git@main"
gpulane --help
```

PyPI packaging is planned, but releases are not published there yet.

## Quickstart

### 1. Start Ray once per machine

```bash
ray start --head --node-ip-address=127.0.0.1 --dashboard-host=127.0.0.1 --dashboard-port=8265
```

Dashboard: `http://127.0.0.1:8265`

### 2. Pick a queue backend

Fastest evaluation path:

```bash
export GPULANE_QUEUE_BACKEND=local
```

Shared host with Redis wake-ups:

```bash
gpulane setup-stack
```

### 3. Generate and validate a manifest

```bash
gpulane init --task-name train.default --cwd training -- -- python train.py
gpulane validate --manifest ray_tasks.yaml
gpulane doctor --manifest ray_tasks.yaml --task train.default
```

Starter manifest:

```yaml
version: 1

repo:
  name: sample-repo

tasks:
  train.default:
    kind: train
    priority: high
    description: Run the default training entrypoint.
    cwd: training
    command:
      - python
      - train.py
    default_args:
      - --config
      - configs/train.yaml
    resources:
      cpu: 8
      gpu: 1
```

### 4. Submit a run

```bash
gpulane submit \
  --manifest ray_tasks.yaml \
  --task train.default \
  --name train-lr1 \
  --summary-path /tmp/gpulane/train-lr1.json \
  -- --epochs 10 --lr0 0.001
```

## Priority Model

Default order:

```text
high > normal > low
```

Priority is not fixed to three labels. You can define any ordered set:

```bash
export GPULANE_PRIORITY_LEVELS=urgent,standard,background,bulk
```

When GPUs are full, `gpulane` either submits immediately, preempts only enough lower-priority jobs, or defers the current job for later resumption.

## Compared With Raw Ray Jobs

Raw Ray Jobs already provides remote job submission. `gpulane` adds:

- a repo-local manifest instead of hand-written per-project submit scripts
- consistent resource declarations
- validation of unsafe environment overrides
- standard metadata and output layout
- optional deferred queueing and preemption rules
- batch execution with config materialization

If you only need `ray job submit`, this repo is probably too much. If several repos are sharing one GPU box, it becomes more useful.

## Docs

- [docs/USAGE.md](docs/USAGE.md)
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
- [docs/RELEASING.md](docs/RELEASING.md)
- [examples/minimal/ray_tasks.yaml](examples/minimal/ray_tasks.yaml)
- [examples/batch/batch.yaml](examples/batch/batch.yaml)
- [CHANGELOG.md](CHANGELOG.md)
- [LICENSE](LICENSE)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
