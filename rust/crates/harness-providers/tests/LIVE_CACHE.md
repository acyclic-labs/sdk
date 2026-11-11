# Prompt-cache acceptance

The deterministic `prefix_cache` shared-host test covers Anthropic, Responses,
Gemini, routed Chat, and Chat endpoints with automatic caching. It checks native
cache requests, byte-stable extensions across tools, notices, forks, paging and
compaction, and cache reads recovered from the original private journal. Valid
JSON mutations of model, system and tool-prefix fields must fail the R44 oracle.

The separate `live-cache` acceptance uses only `claude-haiku-5-5`, `gpt-6-luna`,
and `gemini-3.8-flash`. Each receives three requests: a fresh prefix, that prefix
with a new suffix, then a changed prefix. Reported input usage verifies that the
prefix exceeds the cache minimum. The second call must report positive cache
reads; the changed prefix must not. Missing fields stay missing, including a
Gemini cache-miss field, and are never converted into an estimated zero.

The transport observes original usage JSON through the shared SSE decoder before
the provider adapter. The stock executor runs each call with a fresh in-memory
journal. Acceptance compares durable `tokens.cached` to the original provider
field, including absence. At most nine generation calls run, with no tokenizer
calls, redelivery, sleeps or readiness polling. Each requests only `OK`, with an
output ceiling of 512 tokens.

Set `ACYCLIC_LIVE_CACHE_PROVIDER` to `anthropic`, `openai`, or `gemini` to run only
that provider's three calls. This lets an interrupted acceptance resume with an
unfinished provider without repeating a completed paid probe. Other values are
refused before any request. Without the variable, all three providers run.

Inject Infisical `graphcoder` API keys directly into the process environment as
`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, and `GOOGLE_API_KEY`. Explicit API credentials
disable keychain and subscription-login discovery. Never export keys to files,
print the environment, enable HTTP tracing, or put credentials in prompts. The
test reports model names, call counts and cache counters, suppressing raw errors.

When the native test host has no Infisical login, `ACYCLIC_LIVE_CACHE_RELAY` may
name an explicit `http://127.0.0.1:PORT/PREFIX` reached through an SSH reverse
forward. The trusted local relay keeps Infisical keys in its own process, injects
only the upstream authentication headers, and forwards request and response
bodies unchanged. The test uses placeholder credentials in this mode. The relay
must allow only these three providers and models and at most three calls each;
it must not log headers, bodies or raw upstream errors. No key enters the native
test host or its journal.

Compile on an approved native build host, then explicitly run with injected keys:

```sh
cargo test --locked -p acyclic-harness-providers --test live-cache --no-run
# Use infisical run to inject keys directly into this process:
cargo test --locked -p acyclic-harness-providers --test live-cache -- \
  --ignored --exact real_provider_cache_reads_match_recorded_usage --nocapture
```

Provider contracts:
- [Anthropic breakpoints and usage](https://platform.claude.com/docs/en/build-with-claude/prompt-caching)
- [OpenAI cache routing and usage](https://developers.openai.com/api/docs/guides/prompt-caching)
- [Gemini automatic caching](https://ai.google.dev/gemini-api/docs/caching)
- [Fireworks automatic caching](https://docs.fireworks.ai/guides/prompt-caching)
