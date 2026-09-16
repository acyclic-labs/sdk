# acyclic-inference

Customer-side Inference SDK for immutable Context revisions, recoverable Runs, streamed events, and explicit warm retention. Context content operations do not imply a deployed inference service.

```sh
cargo add acyclic-inference
```

Connect over authenticated HTTPS with `Inference::connect(endpoint, api_key, ca_pem)`. Supply a trusted PEM CA when the service uses a private CA; transport validation remains enabled. Create or attach a Context, then use the typed operation builders to fork, edit, generate, and retain. Save the Run identity after admission so an interrupted caller can recover it. Warm commitments have their own inspect, renew, and release lifecycle.

See the [Rust API](https://docs.rs/acyclic-inference/latest/acyclic_inference/), [repository example](https://github.com/acyclic-labs/sdk/blob/main/README.md), and [customer protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/inference). Provider availability, model access, and billing are deployment-specific.
