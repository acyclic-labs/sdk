# Harness supervision

Supervision belongs to the shared harness. The GraphCoder preset admits an
unattended goal through the ordinary authenticated file and conversation client:

```sh
acyclic run --goal GOAL.md --policy supervision.json
acyclic supervise --json
```

Connection options precede the command. The same commands accept `--endpoint`
and the normal process credential for a remote owner. A local supervisor attaches
to the existing workspace service; it requires no model setup and does not start
an owner. With no `--agent`, it observes the independent sessions admitted by its
credential and their descendants. A tree-scoped credential observes its own tree.
`--agent PATH` selects that admitted subtree.

## Original inputs and decisions

The version-one policy selects thresholds, validation commands, actions and
capacity. For example:

```json
{
  "version": 1,
  "thresholds": {
    "repeat_failure": 2,
    "repeat_failure_pause": 3,
    "validation_loop": 3,
    "loop_output": 2
  },
  "validation_commands": ["cargo test --locked -p selected-package"],
  "no_progress_turns": 2,
  "quiet_ms": 60000,
  "reservation": {"tokens": null, "usd_micros": null},
  "prices": {},
  "actions": {"no_progress": "pause", "stuck": "stop"}
}
```

A null occurrence threshold disables that query. Null reservation dimensions are
unbounded. Validation commands are owner selected; commands match exact original bytes, including quotes and whitespace. An action is `alert`, `pause` or `stop`.
Alerts continue execution. The default repeated-failure action escalates from an
alert to a pause; validation loops, output loops and exhausted capacity pause.

One original HOME publication admits `GOAL.md`, `control/supervision.json` and
`runs/<operation>.json`. Its retained receipt selects the original goal and policy.
Later file edits cannot replace that policy or reset its accounting. Reusing
`--operation UUID` recovers the original admission and preserves accepted goal
progress; changed inputs under that identity are refused.

## Signals and files

| Signal | Original evidence |
| --- | --- |
| `repeat_failure` | Identical invocation with byte-identical command arguments and terminal error |
| `validation_loop` | Repeated admission of a selected check within one turn |
| `no_progress` | Consecutive spending turns without accepted authored progress |
| `stuck` | An unsettled tool's output quiet deadline, delivered by an actor alarm |
| `loop_output` | An identical complete model output in distinct goal turns |
| `spend` | Measured reservation exhaustion or unavailable required usage/cost |

The private file query exposes `status/supervision/<signal>.json` on the normal
local and remote file client. Absence means that signal has not fired. Query
values carry their original stream position and descriptor; they create no
authored filesystem generation. An authored override suppresses its default;
deleting the override reveals the query again. Watches wait on original source
events, including when a signal has never existed.

Accounting uses verified terminal provider usage. Missing measurements remain
unknown. USD reservations require exact provider/model prices pinned in the
original policy; endpoint names do not supply prices. Fractional microdollars
carry across original calls. Same-byte file writes and recovered no-op receipts
do not count as authored progress.

## Owner controls

The private root consumes ordinary filesystem publications:

| File | Effect |
| --- | --- |
| `control/pause` | Presence pauses; deletion resumes |
| `control/stop` | Presence archives the agent through the shared lifecycle |
| `control/steer.md` | Each new UTF-8 suffix becomes ordinary inbox input |

Steering retains the admitted immutable generation. A rewrite or truncation of
its admitted prefix is rejected and emits `control_rejected`; owner pause and
stop still take effect. Comparison reuses immutable chunks and bounded range
reads. Cumulative steering is unbounded; each individual suffix uses the owner's
ordinary input capacity. Rejection never replaces the admitted prefix. Clearing
a pause retains spending totals, and every new dispatch checks that retained
reservation before paid work.

The first request of a new turn captures one original inbox cut and includes
steering already committed there. A request whose input was already frozen
replays its exact original cut; later steering waits for its next boundary.

## Feed and exits

`supervise --json` writes one direct version-one event per line:

```json
{"version":1,"agent":"<original agent path>","run":"<operation UUID>","source":{"stream":"<original producer>","sequence":0},"event":{"kind":"started","input":0}}
```

Sequences are zero based. Events include `started`, `turn_done`, `signal`,
`paused`, `resumed`, `stopped` and `control_rejected`. Explicit owner controls
carry `run: null`. Source positions identify original publications; observer
clocks never replace them. Reopening a feed can replay records, so consumers
retain original positions when deduplicating.

The final `run` result is `{version:1,operation,result}`. Exit zero means the goal
is done. Signal exits are 20 repeat failure, 21 validation loop, 22 no progress,
23 stuck, 24 output loop and 25 spend. Explicit pause and stop use 26 and 27;
an ended goal without done uses 28.

## Compatible model configuration

Morph and other compatible endpoints use the ordinary provider catalog and
per-role model configuration. The endpoint adapter, model capabilities and
capacity belong to that catalog. No provider or deployment name is selected by
supervision code. Root selection can come from settings:

```toml
model_provider = "morph"
model = "<configured model id>"
[compaction]
kind = "disabled"
```

Role templates can select another configured provider/model with their `model`
frontmatter. Automatic compaction requires a verified counter; the explicit
disabled policy supports endpoints without one. Unattended owners read API keys
only from the process environment, using the provider's normal credential slot
(for example, `MORPH_API_KEY` for catalog id `morph`). Keys do not enter guest
environments, agent files or streams. Standard transient HTTP backpressure
retains the shared transport's Retry-After handling.
