`pi-assistant-error.jsonl` contains unchanged assistant `message_end` and
`agent_settled` lines captured
from Pi 1.1.0 on Linux with `--mode json --print --no-session`, the local
Responses provider fixture, and no authentication. The real CLI exited 0
despite this error and dispatched no model request. The capture contains no
credentials or filesystem paths.

SHA-256: `02b1bdc90f4ace0499a89dbcc3c8dc407eb805f9d16f4c51e71b64b90aba1fd7`.

`codex-completed.jsonl` and `claude-code-completed.jsonl` select unchanged
reply lines from `plugin/tests/hosts/fixtures/native-completion.json` in this
repository. That capture records installed native hosts against the loopback
mock, with no live model call. Codex 0.164.0 source
`0610981c4cdb69ff10d4a17d563ff13c2ef7bb68` retains its `turn.started`,
`item.completed` and `turn.completed` lines; Claude Code 2.1.278 source
`943c9d40e41c6a022a241ab19dab5431d7639aa8` retains its `result` line.
No event fields were rewritten. Other events were omitted.

SHA-256:

- Codex: `570f3b075daf21ce326fb22b28df4f512f1d21436d9152ec339ff0a147e9a160`.
- Claude Code: `a4c4a86ed094e949983389230343a95ba9129df847235366541a9b6cda75d7ae`.

The refusal controls mutate the captured Claude success flag and Codex terminal
type in memory; those derived negatives are not captured native output.
