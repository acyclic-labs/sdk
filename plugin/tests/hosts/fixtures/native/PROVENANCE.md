These session initialization lines are captured from the locked native host
binaries on bb2 with exclusive fixture homes and no inherited authentication.
`provenance.json` records the commands, versions and output hashes. Fields and
session identifiers are retained exactly as emitted.

Claude Code 2.1.278 and Pi 1.1.0 were captured by job
`1792954462041-1154467-ad56f307` on 2026-10-25. Claude Code explicitly selected
`claude-haiku-5-5`; its fixture run exited for absent authentication. Pi selected
the same model and emitted its session event before refusing absent credentials.
Both ran within Codex's native workspace-write sandbox with networking disabled.

Codex 0.164.0 was captured by job `1792954940625-1173634-ebafc5b6` on
2026-10-25. It used the same native sandbox, a nonexistent localhost provider
and an empty fixture CODEX_HOME. The canonical owned-process scope stopped the
process tree immediately after `thread.started`. No real model was called.

These are protocol fixtures for explicit native process stand-ins. They do not
qualify managed model sessions; the coordinator runs that acceptance matrix.

Attribution: Codex <noreply@openai.com>.
