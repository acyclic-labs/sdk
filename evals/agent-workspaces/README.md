# Acyclic agent-workspaces behavioral evaluation

This directory evaluates the plugin through real `codex exec --json` sessions. It does not mock
subagents or grade their prose. The behavioral grader consumes Codex JSONL events, the fixture
filesystem, the plugin's persisted adapter state, and before/after checksums of the fixture's
system Git metadata.

There are two deliberately separate suites:

- `protocol_check.py` is deterministic. It builds the production control and packaged `acyclic`
  executables, pins the JSON-RPC handshake, public tool schemas, and child prompt, exercises the
  authenticated `acyclic git` IPC path (including invalid capabilities), proves bare `git` still
  resolves to system Git, verifies unclean restart against the same `PLUGIN_DATA`, and runs the
  control crate's locked Rust tests.
- `run_eval.py` measures model reliability. Every attempt uses a fresh Git fixture and plugin-data
  directory, invokes a pinned Codex CLI/model/reasoning configuration in a disposable Codex home,
  and retains the raw JSONL
  needed to explain its grade. Repeated model runs are independent samples; they are not expected
  to be bit-for-bit deterministic.

## Prerequisites

- `codex-cli 0.154.0`, authenticated in a dedicated evaluation Codex home
- Rust 1.94/Cargo, Git, and Python 3.11+
- a host supported by `acyclic-fs` native mounts

Run the behavioral suite inside a dedicated OS account or container with no personal files and a
short-lived, scoped evaluation credential. The runner intentionally has no default source home and
requires `--acknowledge-readable-eval-credential`: local Codex sandboxes do not provide a general
read denylist, so model commands may be able to read the copied credential. Never use a personal
Codex home. The runner creates a temporary home, copies only `auth.json`, and always removes that
home even when artifacts are retained. Never copy the credential into a report bundle.
Before retaining a failed fixture or plugin state, the runner scans every regular file for raw,
JSON-escaped, URL-encoded, and base64 credential markers. Any match fails the attempt and forces
that state to be deleted; persisted JSON evidence is redacted independently.
Sessions are not ephemeral because native subagents and lifecycle hooks need the transcript store.
The local eval marketplace and plugin wrapper exist only inside disposable fixtures.
Model commands run under `workspace-write` with approvals disabled and receive an allowlisted host
environment, but the isolated runtime boundary—not the Codex sandbox—is the credential boundary.

## Run

From the repository root:

```text
python evals/agent-workspaces/protocol_check.py --out artifacts/agent-workspaces/protocol
python evals/agent-workspaces/run_eval.py --source-codex-home <dedicated-home> --acknowledge-readable-eval-credential --runs 3 --out artifacts/agent-workspaces/behavior
python -m unittest evals/agent-workspaces/test_harness.py
```

Useful development forms:

```text
python evals/agent-workspaces/run_eval.py --source-codex-home <dedicated-home> --acknowledge-readable-eval-credential --case debug-evidence-discard --runs 1 --keep all
python evals/agent-workspaces/run_eval.py --source-codex-home <dedicated-home> --acknowledge-readable-eval-credential --case speculative-dependent-reconcile-restart --runs 1 --keep all
python evals/agent-workspaces/grade.py artifacts/agent-workspaces/behavior/*/events.jsonl
```

`cases.json` pins the CLI version, model, reasoning effort, per-case assertions, and thresholds.
Override a pin only with the corresponding explicit CLI option; the override is recorded in the
report and makes comparisons with the baseline configuration non-equivalent.

## Reports and thresholds

`report.json` is the machine-readable result. Each attempt records:

- normalized JSONL event counts and required tool observations;
- exact file content/absence/sentinel assertions;
- plugin-state structural assertions;
- the pre/post SHA-256 tree digest of `.git`;
- SHA-256 checksums for the prompt, fixture seed, event stream, final response, and retained state.

Deterministic protocol checks must all pass. Behavioral cases pass when at least 80% of their
attempts pass, and the complete suite passes at 90% or better overall. A skipped attempt never
counts as a pass. With fewer than three runs, the report is useful for debugging but is marked
`insufficient-samples` and cannot satisfy the release threshold.

`SHA256SUMS` covers every retained, non-secret artifact. `fixture-before.json` and
`fixture-after.json` are canonical manifests, so a result can be audited without trusting prose.

The default retention policy is `failures`: passing worktrees and plugin state are removed after
their manifests and event evidence are saved; failing attempts keep the fixture and plugin-data.
Use `--keep none` for sensitive repositories and `--keep all` for local diagnosis. Raw event
streams may contain model output and absolute paths, so treat retained bundles as confidential.

## Case contract

Prompts live in `prompts/`; invariant definitions live in `cases.json`. The three central usage
patterns are disposable debugging evidence (`agent_changes` then `agent_discard`), speculative
dependent work (publish, reconcile, and publish the same child again across a control restart),
and parallel hypotheses (inspect both, selectively `agent_merge` one and `agent_discard` the
other). Additional cases cover recursive publication, idempotency, typed conflicts, path escapes,
and rejected transport. Children use the authenticated `acyclic git` facade for local Git-shaped
operations; ordinary `git` is deliberately system Git and is never intercepted. The interruption
is injected by `launch_control.py` after a successful `_hook_post_tool` response; subsequent hooks
must restart the same production binary using the same durable plugin state.

The agent-facing contract is intentionally narrow: native `spawn_agent` plus only
`agent_changes`, `agent_merge`, and `agent_discard`. Command evidence counts only completed command
events with the expected exit code; prompt text and final claims cannot satisfy it.

The final response schema helps debugging only. Passing depends on events and host state, never on
the model claiming success.
