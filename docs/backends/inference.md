The inference provider seam is `acyclic_harness_core::model::ModelProvider`.
`acyclic-harness-providers` supplies the shared registry, immutable model selection,
request projection, transport, retry/deadline handling and stream decoders.
`acyclic-inference` separately exposes remote immutable Contexts and recoverable
Runs; it is not the streaming model provider used by a harness turn.

`acyclic-conformance::inference` exercises streaming, immediate response release
on cancellation, exact usage, paired tools/results, typed rate-limit/context/auth
refusals and deterministic replay. The assertions accept any `ModelProvider` and
contain no provider-specific branches. Each complete fixture family has all eight
cases; missing real captures fail qualification.

Run on bb2 from the lane worktree:

```powershell
pwsh -NoProfile -File Q:/lanes/bs.ps1 cargo test -p acyclic-conformance --features inference
```

`inference-qualify PATH/manifest.json [...]` checks unmodified captured HTTP bodies,
their hashes and provenance through the production registry and adapters. The
fixture manifest declares exact token expectations rather than inferring a passing
result from the output under test. The llama.cpp fixture is real; cloud provider
qualification requires separate captured-real families.

A manifest records the provider ID, original `ProviderSpec` without credentials,
model/options, UTC capture time, origin, the actual overflowing `context_input`,
and all six real response bodies (`text`,
`tool`, `result`, `rate`, `context`, `auth`). Each body has its original HTTP status
and SHA-256 digest. Optional request URL and JSON-pointer assertions bind the
replay to its original route and paired-result request. Replays supply an isolated
public test credential and never consult CLI login, environment or keychain data.

Usage pointers are relative to public completion metadata: Chat uses
`/usage/prompt_tokens` and `/usage/completion_tokens`; Responses and Messages use
`/usage/input_tokens` and `/usage/output_tokens`. Record exact expected counters
from the original wire response. `usage` describes the text step; `tool_usage`
contains separate exact counters for the tool and result steps. Preserve original response bytes with Git's
`-text` attribute. The common runner refuses incomplete families and changed hashes.

Enable `acyclic-harness-providers/endpoint-presets` for `endpoints::{openrouter,
bedrock,vertex,local}`. All use `Adapter::Chat`. Register exact model capacities
in `ProviderSpec.models` and explicit credentials in `Credentials::explicit`.
Resolution and streaming are available without a counting endpoint; automatic
context admission/compaction still refuses without a verified token counter.

Model selection is a public harness component. A host composes a `Registry` with
its protected credential source and `HttpTransport`, registers `ProviderSpec`
values, then calls `resolve_with_options("provider/model", options)`. The returned
`ResolvedModel` implements the canonical `ModelProvider`; its `model()` supplies
the immutable model identity used in prepared requests. Register model names
exactly, including any slashes in routed model names. Resolution performs no
network I/O. Hosts must supply already authorized content and credential access;
selecting a model does not confer filesystem or turn authority.

GraphCoder's `--catalog`, `--model` and settings select these same public
components. A catalog can set `default_model`; the GraphCoder preset supplies
defaults and the CLI renders events. Managed Swarm can compose the registry
directly instead of retaining a private gateway, request interpreter or transport.
This configuration path does not qualify an untested cloud route.

For a client driving a remote harness, resolve the connection through
`acyclic_harness_client::ConnectionOptions::from_env` (or
`resolve_connection_options` with supplied environment values on WASM). Explicit
connection values take precedence over `ACYCLIC_ENDPOINT` and `ACYCLIC_API_KEY`;
the endpoint otherwise defaults to `https://swarm.cloud.acyclic.dev`.
The host separately composes its upstream model registry using `ProviderSpec`
and `Credentials`. These provider inputs do not read the managed connection
variables. A remote client supplies its account credential to the harness
connection; the host supplies provider credentials to its shared model pipeline.

Provider presets are configuration only: `endpoints.rs` has 74 physical lines
including comments and whitespace, shared by all four added routes. The existing
wire decoders are Chat 53, Anthropic 112, Responses 138 and Gemini 97 lines.
Request projection, retries, deadlines, authentication selection, protocol budgets,
usage and cancellation remain in the shared provider core. No backend adds its
own retry, timer, token cache or authentication policy.
