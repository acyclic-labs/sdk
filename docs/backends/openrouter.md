Qualification: **unqualified** against OpenRouter. The shared Chat adapter passes
the complete real llama.cpp suite; no OpenRouter cloud pass is claimed.

Enable `endpoint-presets`, register `endpoints::openrouter()`, and supply an
explicit OpenRouter key through `Credentials`. Model names retain their vendor
prefix, e.g. `openrouter/vendor/model`; register that full upstream name and its
exact capacity. OpenRouter uses the shared Chat adapter, including streamed usage
and standard tool-call/result projection. Streaming support depends on the routed
upstream model. Configure a verified token counter for automatic compaction.

Live steps (coordinator only): capture the suite's text, echo-tool, paired-result,
context-overflow, invalid-auth and rate-limit responses from
`https://openrouter.ai/api/v1/chat/completions`, retaining status, headers, request
and raw body; declare exact usage counters and SHA-256 hashes in the common
manifest, then run `inference-qualify` unchanged. No cloud conformance pass is
claimed without this captured-real family.

Reference: https://openrouter.ai/docs/api-reference/streaming
