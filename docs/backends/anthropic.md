Use the existing Anthropic catalog entry and explicit API-key credentials. Its
Messages adapter uses the shared request pipeline, bounded retries/deadlines,
stream parsing, usage merge and paired tool-result projection. Register exact
model capacity for a selection outside the shipped catalog. A verified token
counter is required for automatic context admission and compaction.

Live steps (coordinator only): every live request must select `claude-haiku-5-5`.
Capture the common text, echo(value1), paired result, context-overflow, invalid-auth
and genuine rate-limit cases from Messages, preserving original response bytes,
status, sanitized request metadata, exact usage and capture provenance. Supply a
complete manifest to `inference-qualify` unchanged. Include signed thinking blocks
when enabled so the same tool-continuation vector verifies their preservation.

Existing provider tests cover Messages parsing, signed reasoning, tools and native
accounting. A complete captured-real Anthropic family has not yet been provided;
no cloud conformance pass is claimed from those tests or the llama.cpp recording.
