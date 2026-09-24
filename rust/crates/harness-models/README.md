# acyclic-harness-models

Model adapters that implement the Harness `ModelProvider` over third-party
inference APIs. They run wherever the agent runs (a customer machine or a
managed sandbox), so they are public here rather than in a private service.

```sh
cargo add acyclic-harness-models
```

`OpenAiCompatibleProvider` streams the OpenAI-compatible chat completions API
with tool calling. `ProviderConfig::openrouter` selects the `OpenRouter` dialect:
app attribution headers (`HTTP-Referer`, `X-Title`), a per-request `user`
attribution id, inline usage accounting whose `cost` lands in the
`ModelEvent::Completed` metadata as a typed `CompletionMetadata`, and a typed
`ProviderError` that separates an exhausted credit or key budget (stop the
work) from a rate limit (retry). Provider-specific types never enter the
Harness core. See the [adapter API](https://docs.rs/acyclic-harness-models/latest/acyclic_harness_models/)
and [Harness guide](https://docs.rs/acyclic-harness/latest/acyclic_harness/).
