# acyclic-inference

Customer-side Inference SDK for immutable Context revisions, recoverable Runs, streamed events, and explicit warm retention. Context content operations do not imply a deployed inference service.

The unreleased additive idle KV contract uses
`context.retain(Retention::idle_kv(profile, idle_timeout_ms))` and
`warm.renew_idle(idle_timeout_ms)`. Discover opaque policy profiles from
`ModelCapability.idle_kv_profiles`. This paid KV pin has no capacity, throughput
or latency guarantee. Only verified actual Run reuse of the pinned revision or
descendant prefix advances last-use; fork/edit/admission/inspect/recovery do not.
Renewal changes timeout from the last verified-use baseline (initial verified
pin time before first use), without resetting that baseline. Released or expired
pins require a new retain identity. Clone the admission/renewal builder to retry
the same operation; inspect reports the current lifecycle. The canonical
`WarmView.idle_kv` separates initial pin time from optional actual-use time and
its authoritative Run identity. Cloud implementation and package publication
remain separate from this source contract.

```sh
cargo add acyclic-inference
```

Connect over authenticated HTTPS with `Inference::connect(endpoint, api_key, ca_pem)`. Supply a trusted PEM CA when the service uses a private CA; transport validation remains enabled. Create or attach a Context, then use the typed operation builders to fork, edit, generate, and retain. Save the Run identity after admission so an interrupted caller can recover it. Warm commitments have their own inspect, renew, and release lifecycle.

See the [Rust API](https://docs.rs/acyclic-inference/latest/acyclic_inference/), [repository example](https://github.com/acyclic-labs/sdk/blob/main/README.md), and [customer protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference). Provider availability, model access, and billing are deployment-specific.
