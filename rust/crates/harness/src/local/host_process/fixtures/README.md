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

The native session lifecycle controls replay the original full stdout from
that capture, and this Pi refusal, into the settlement log. They read the
committed bytes before and after replacing the workspace owner. This checks
log retention and resume identity; it does not execute the installed hosts or
restart the harness process.

SHA-256:

- Codex: `570f3b075daf21ce326fb22b28df4f512f1d21436d9152ec339ff0a147e9a160`.
- Claude Code: `a4c4a86ed094e949983389230343a95ba9129df847235366541a9b6cda75d7ae`.

The refusal controls mutate the captured Claude success flag and Codex terminal
type in memory. The framing-loss control corrupts the first byte of a copy of
the captured Codex start line, then replays the original reply. These derived
negatives are not captured native output.

The tool-refusal controls share
`plugin/tests/hosts/fixtures/native-claude-workflow-refusal.json` and
`native-codex-tool-terminals.json`, including their original provenance.
The Claude projection retains three real tool errors, their call identities
and reasons, followed by the original successful parent terminal and its
usage, session and terminal metadata. Original stdout SHA-256:
`81b56ddc318ef025923163679285c1038cb1f68ca8d22bac8df78473bb5e3ea1`.
The Codex projection retains failed command items and their original exit
codes before a successful turn. Original stdout SHA-256:
`f4747f9fdc481e61d6db161f18b6fdeda2716994d43adc8ad169ac0a7f225688`.

The `claude-tool-refusal` and `codex-tool-refusal` native-reply fuzz seeds
serialize those same selected event values as JSONL; no wire values are
invented or replaced. The Claude process control sends those records through
the original pipe, framing, settlement and event descriptor paths, with an
external stderr writer and a zero process exit. Its shell replays the capture;
it does not run the installed model CLI or qualify a process restart.
