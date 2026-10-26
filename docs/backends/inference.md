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

Enable `acyclic-harness-providers/endpoint-presets` for `endpoints::{openrouter,
bedrock,vertex,local}`. All use `Adapter::Chat`. Register exact model capacities
in `ProviderSpec.models` and explicit credentials in `Credentials::explicit`.
Resolution and streaming are available without a counting endpoint; automatic
context admission/compaction still refuses without a verified token counter.
