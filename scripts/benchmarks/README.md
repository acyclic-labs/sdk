# Benchmark preparation

This directory provides an offline paired-job planner and a Harbor installed
agent for the shipping GraphCoder prompt mode. It never starts a benchmark.
Every generated plan is marked BLOCKED until its explicit readiness requirements
are satisfied. Fixture tests make no model calls and require no credentials.

Use Python 3.12 or later on Linux. The existing bootstrap installs the pinned
Harbor dependencies from `requirements.lock` into an explicit tools directory:

```sh
python3 -m scripts.benchmarks.bootstrap --directory /path/to/benchmark-tools
bash scripts/qualify-benchmarks.sh /path/to/benchmark-tools/environment/bin/python
```

Setup network/install time is separate from the offline qualification suite.
On Linux also run `bazel test //rust/crates/harness-providers:main --test_filter=benchmark_catalog::`.
It generates the actual Python catalogue, loads/resolves it through GraphCoder's
production registry with empty explicit credentials, and rejects the old
incomplete feature declaration. It never sends a request.

Build the Linux GraphCoder artifact from the selected source commit with
`bazel build -c opt --remote_download_toplevel //plugin:graphcoder`. Preserve its source commit and
SHA256 digest with the plan; the artifact alone does not satisfy run readiness.

Inventory an already available public dataset checkout at its immutable Git
commit. The inventory reads Git tree metadata only; it does not fetch tasks:

```sh
python3 -m scripts.benchmarks.plan inventory \
  --repository /path/to/terminal-bench-2 \
  --dataset terminal-bench@2.0 \
  --git-url https://github.com/laude-institute/terminal-bench-2.git \
  --commit FULL_40_DIGIT_DATASET_COMMIT > /path/to/tb2-manifest.json
```

Generate a plan with an explicitly supplied, unauthenticated Responses endpoint:

```sh
python3 -m scripts.benchmarks.plan plan \
  --stage tb2-20 --manifest /path/to/tb2-manifest.json \
  --binary /path/to/graphcoder --binary-sha256 FULL_BINARY_SHA256 \
  --source-sha FULL_QUALIFIED_MAIN_COMMIT \
  --model openai/MODEL --base-url http://EXPLICIT_ENDPOINT/v1 \
  --context-tokens MODEL_CONTEXT_CAPACITY --output-tokens MODEL_OUTPUT_CAPACITY \
  --codex-version EXACT_CODEX_VERSION > /path/to/tb2-20-plan.json
```

The planner has no Harbor dependency. It rejects floating commits, incomplete
task inventories, duplicate names and noncanonical paths. The full Terminal-Bench
2.0 inventory contains 89 tasks; the first stage selects 20 by a stable hash of
task identity. `tb2-full` uses all 89. `swe-verified` requires an independently
pinned SWE-bench Verified inventory of all 500 tasks. Both agent job configs
contain the same explicit task list, dataset commit and model endpoint, one
attempt per task, concurrency one and zero retries. The selection seed is
`predictor-tb2-v1`; no model results affect selection.
The pinned Codex adapter projects model names to their last slash-delimited
segment. Paired plans therefore require exactly `provider/model`; nested model
names are rejected because the two adapters would select different models.

Each plan contains `graphcoder` and `codex` Harbor job objects. Preserve the plan
alongside any subsequently authorized job configurations and resulting Harbor
`lock.json` and per-trial verifier results. A plan is not a run receipt. No
comparison score or missing-trial interpretation is produced here.
An execution receipt must retain failed attempts and distinguish process
completion from the task's required effects. A zero exit status alone does not
establish task success.

The GraphCoder adapter verifies the Linux binary digest and captures those
exact bytes before any upload, checks the digest again inside the environment,
and requires the uploaded executable's `--version` to report `graphcoder <version>`.
The artifact's filename is unrestricted. It then runs the prompt through the
task's native working directory. It uses
fixture HOME/CODEX_HOME/settings, clears inherited process environment apart
from the task PATH, and uploads only the binary and a public provider catalogue.
It rejects configured agent environment variables and does not resolve a host
login. Its stdout JSONL, stderr and artifact/source receipt remain in Harbor's
agent logs. Nonzero exits propagate without a logging pipeline masking them.
Successful setup grants execution only in that exact environment object; a
different container or a failed reinstall cannot reuse that authority.
Task skills are copied from Harbor's environment-side skills directory into
the task workspace's `.agents/skills`, including auxiliary and hidden files.
The harness discovers them through the ordinary volume path. Copy failures
reject setup and revoke execution authority; task MCP servers remain unsupported.

The environment needs the binary's runtime libraries and a task working directory
other than `/`. The CLI defaults to `--workspace-mounting auto`: it selects native
projection when the host probe admits it, otherwise reports the reason and uses
physical directories with capture at each tool boundary. Native Linux projection
additionally needs a usable `/dev/fuse`, `fusermount3` and mount authority.
Host Docker/FUSE availability alone does not prove container readiness.
Setup runs `graphcoder doctor` with the run's exact flags:
it checks read-only prerequisites without opening state, creating signing authority
or starting MCP children. Doctor and execution share the same explicit private-file
custody under the trial state directory. Storage and credential-service access
are admitted only when execution opens the harness. The adapter
never changes container privileges or task
images. Native mount admission inside the actual task images still needs a
credential-free qualification before a run. Loopback endpoint addresses refer
to the task container, not the Linux host.

Readiness also requires green-wide-main and three-platform receipts for the
binary source, separately authorized credential-free model access for **both**
agents, and matched effective reasoning/token budgets. The Codex job uses
Harbor's built-in comparator; this planner does not instantiate it or invoke
its authentication discovery. Neither host login nor API-key material may be
placed on Linux. Model-access and effective-budget gaps remain explicit blockers.

Interface references: [Harbor custom agents](https://docs.harborframework.com/agents/custom-agents),
[pinned Harbor source](https://github.com/laude-institute/harbor/tree/v0.24.0),
[Terminal-Bench 2](https://github.com/laude-institute/terminal-bench-2),
[SWE-bench Verified](https://www.swebench.com/verified).

GraphCoder's Harbor kwargs accept `adapter="anthropic"`, `"gemini"`, `"chat"`
or `"responses"`. If omitted, a recognized proxy path segment takes precedence
over the model provider; otherwise the provider selects the adapter, with
Responses as the fallback. Thus `/anthropic`, `/google`, `/deepseek/v1` and
`/fireworks/v1` select Anthropic, Gemini, Chat and Chat respectively. Plain HTTP
proxy endpoints are supported on loopback hosts (`localhost`, `127.0.0.1`,
or `[::1]`); other proxy hosts require HTTPS. No credentials enter the task.
Native token counters come from the Rust catalog's `token_counters.json`:
Anthropic, Gemini and Responses use their own count endpoints; Chat has none.
For catalogs without a counter, Harbor supplies `--no-compaction` to both
doctor and execution. This selects the harness's existing disabled-compaction
policy: ordinary turns have no input-count preflight, and the provider enforces
its context window. Use a GraphCoder artifact built with this flag.
The offline Rust catalog test covers all four adapters and malformed negatives.

## Feature on/off evaluation

`features.json` lists each optional feature, its placement (`in-turn`, or
`off-turn` for separate calls such as context management) and its two arms.
An arm is admitted flags (`ARM_FLAGS` in `contracts.py`) plus non-secret home
files. `plan features` pairs GraphCoder with itself: the same tasks, artifact,
model, endpoint and budgets, with the arm as the only difference. A pair whose
effective arms are identical under the selected catalog is refused. Every run
exports committed per-call usage to `agent/usage.jsonl`, so both arms carry
their own token counts, including after a failure.

```sh
python3 -m scripts.benchmarks.plan features \
  --stage tb2-20 --manifest /path/to/tb2-manifest.json \
  --features scripts/benchmarks/features.json \
  --binary /path/to/graphcoder --binary-sha256 FULL_BINARY_SHA256 \
  --source-sha FULL_QUALIFIED_MAIN_COMMIT \
  --model provider/CHEAP_MODEL --base-url http://EXPLICIT_ENDPOINT/v1 \
  --context-tokens MODEL_CONTEXT_CAPACITY --output-tokens MODEL_OUTPUT_CAPACITY \
  > /path/to/features-plan.json
python3 -m scripts.benchmarks.feature_eval jobs --plan /path/to/features-plan.json --out /path/to/configs
for config in /path/to/configs/*.json; do harbor run -c "$config" -o /path/to/jobs; done
python3 -m scripts.benchmarks.feature_eval score --plan /path/to/features-plan.json --jobs-dir /path/to/jobs
```

The scorer refuses arms that differ outside their feature arm, and trial sets
that are not exactly the plan's tasks. Trials are matched by the task path in
their own `result.json` config: Harbor's `task_name` is task.toml's package
name when the task declares one, not its directory. A trial that raised, or whose verifier
reward is not 1, is a failure and stays in the denominator. Token totals are
input plus output over completed calls. Cached input is reported but never
decides. An unfinished call or a missing count makes the verdict INCOMPLETE,
never zero. The decision rule, computed from on-minus-off deltas:

- **ON** when successes do not drop and tokens do not rise.
- **JUSTIFY** when an in-turn feature buys strictly more successes with more
  tokens. A written justification is then required to keep it.
- **OFF** otherwise. An off-turn call is never kept on a justification: without
  a net gain it stays off.

`test_feature_eval.py` is the mock tier. It plans the registry, runs every arm's
real command line against a scripted stand-in agent, lays the results out as
Harbor trial directories, and scores them. No model, Harbor, Docker, network or
credential is involved.
