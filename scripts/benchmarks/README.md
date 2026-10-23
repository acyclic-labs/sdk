# Benchmark preparation

This directory provides an offline paired-job planner and a Harbor installed
agent for the shipping GraphCoder prompt mode. It never starts a benchmark.
Every generated plan is marked BLOCKED until its explicit readiness requirements
are satisfied. Fixture tests make no model calls and require no credentials.

Run all Linux preparation on bb2. Build GraphCoder from the exact qualified main
SHA using `cargo build --locked --release -p graphcoder`. Windows and macOS live
acceptance must qualify that same SHA before benchmark execution. Do not build
or run benchmark tasks on a developer PC.

Use Harbor 0.24.0 and Python 3.12 or later. Install `requirements.lock` into a
dedicated environment with pinned uv 0.13.5 (`uv venv`, then
`uv pip sync --require-hashes --python ENV/bin/python scripts/benchmarks/requirements.lock`).
The bb2 system Python lacks ensurepip; `python3 -m venv` with pip bootstrapping
is not a prerequisite. Keep tools, caches and environments in the lane target
directory. Buildbox target directories are disposable per job: do not assume
an environment from an earlier job still exists. The self-contained bb2 setup
downloads a SHA256-pinned public uv wheel, creates the environment without
ensurepip and installs only the hashed dependency lock:

```sh
python3 -m scripts.benchmarks.bootstrap --directory "$CARGO_TARGET_DIR/benchmark-tools"
bash scripts/qualify-benchmarks.sh "$CARGO_TARGET_DIR/benchmark-tools/environment/bin/python"
```

Setup network/install time is separate from the offline qualification suite.

Inventory an already available public dataset checkout at its immutable Git
commit. The inventory reads Git tree metadata only; it does not fetch tasks:

```sh
python3 -m scripts.benchmarks.plan inventory \
  --repository /path/to/terminal-bench-2 \
  --dataset terminal-bench@2.0 \
  --git-url https://github.com/laude-institute/terminal-bench-2.git \
  --commit FULL_40_DIGIT_DATASET_COMMIT > /lane/tb2-manifest.json
```

Generate a plan with an explicitly supplied, unauthenticated Responses endpoint:

```sh
python3 -m scripts.benchmarks.plan plan \
  --stage tb2-20 --manifest /lane/tb2-manifest.json \
  --binary /lane/target/release/graphcoder --binary-sha256 FULL_BINARY_SHA256 \
  --source-sha FULL_QUALIFIED_MAIN_COMMIT \
  --model openai/MODEL --base-url http://EXPLICIT_ENDPOINT/v1 \
  --context-tokens MODEL_CONTEXT_CAPACITY --output-tokens MODEL_OUTPUT_CAPACITY \
  --codex-version EXACT_CODEX_VERSION > /lane/tb2-20-plan.json
```

The planner has no Harbor dependency. It rejects floating commits, incomplete
task inventories, duplicate names and noncanonical paths. The full Terminal-Bench
2.0 inventory contains 89 tasks; the first stage selects 20 by a stable hash of
task identity. `tb2-full` uses all 89. `swe-verified` requires an independently
pinned SWE-bench Verified inventory of all 500 tasks. Both agent job configs
contain the same explicit task list, dataset commit and model endpoint, one
attempt per task, concurrency one and zero retries. The selection seed is
`predictor-tb2-v1`; no model results affect selection.

Each plan contains `graphcoder` and `codex` Harbor job objects. Preserve the plan
alongside any subsequently authorized job configurations and resulting Harbor
`lock.json` and per-trial verifier results. A plan is not a run receipt. No
comparison score or missing-trial interpretation is produced here.

The GraphCoder adapter verifies the Linux binary digest and captures those
exact bytes before any upload, checks the digest again inside the environment,
then runs the prompt through the task's native working directory. It uses
fixture HOME/CODEX_HOME/settings, clears inherited process environment apart
from the task PATH, and uploads only the binary and a public provider catalogue.
It rejects configured agent environment variables and does not resolve a host
login. Its stdout JSONL, stderr and artifact/source receipt remain in Harbor's
agent logs. Nonzero exits propagate without a logging pipeline masking them.

The environment must already admit native FUSE mounts: a usable `/dev/fuse`,
`fusermount3`, the binary's runtime libraries, mount authority, and a task working
directory other than `/`. Host Docker/FUSE availability alone does not prove
container readiness. The adapter never changes container privileges or task
images. Native mount admission inside the actual task images still needs a
credential-free qualification before a run. Loopback endpoint addresses refer
to the task container, not the bb2 host.

Readiness also requires green-wide-main and three-platform receipts for the
binary source, separately authorized credential-free model access for **both**
agents, and matched effective reasoning/token budgets. The Codex job uses
Harbor's built-in comparator; this planner does not instantiate it or invoke
its authentication discovery. Neither host login nor API-key material may be
placed on bb2. Model-access and effective-budget gaps remain explicit blockers.

Interface references: [Harbor custom agents](https://docs.harborframework.com/agents/custom-agents),
[pinned Harbor source](https://github.com/laude-institute/harbor/tree/v0.24.0),
[Terminal-Bench 2](https://github.com/laude-institute/terminal-bench-2),
[SWE-bench Verified](https://www.swebench.com/verified).
