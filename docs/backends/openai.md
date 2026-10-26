Use the existing OpenAI `Registry` catalog entry for the Responses adapter. API
keys are explicit credentials; native Codex CLI login uses the existing
`ApiKeyOrCodexLogin` policy and account-bound refresh path. The registry refuses
to forward Codex login credentials to a custom endpoint. Configure exact model
capacity before resolving a model outside the shipped catalog.

The common inference suite drives the same public `ModelProvider` API for both
credential modes. Existing provider tests cover Responses reasoning continuity,
tool pairing, refresh and native accounting. Those tests do not substitute for
a captured-real cloud conformance family, which is still required.

Live steps (coordinator only): capture text, echo(value1), paired result, context
overflow, invalid auth and a genuine rate refusal through the configured Responses
route. Preserve raw SSE/status, redacted request metadata, exact usage and origin
in a complete inference manifest, then run `inference-qualify` unchanged. Keep
Codex account and bearer data out of the capture. No paid account is used by lanes.
