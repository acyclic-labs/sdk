# Codex 0.155.1 fixtures

These are recorded from the real `codex exec --json` (0.155.1) talking to `tools/fake_responses_server.py`.
No OpenAI API was used. `../../tools/record-run.sh` re-records them, and the steps are in `DESIGN.md` under "Upgrading Codex".

| File | What it pins |
| --- | --- |
| `<run>.stdout.jsonl` | Exact event stream for the run |
| `<run>.exit` | Exit code and wall time |
| `request-shell.json` | One request Codex sends to the provider (trimmed) |
| `request-counts.json` | Requests Codex makes before giving up, by failure mode |
| `probe-config.toml` | Config accepted by `--strict-config`; `config.rs` may only render keys found here |
| `help-exec*.txt` | CLI flags of this version |
| `synthetic-crash.stdout.jsonl` | **Not recorded.** The first lines of `ok` with no turn event, standing in for a killed process |

Runs: `ok` (unknown model), `ok-gpt55` (catalog model), `shell`, `patch`, `reasoning`, `resume` (resumes the `ok` thread),
`402` / `500` / `response-failed` (retried), `quota429` / `quotafailed` / `400` (not retried), `missing-envkey`,
`mcp-404` (optional MCP server down), `mcp-required-404` (required MCP server down: empty stdout, error on stderr).
