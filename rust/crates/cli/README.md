# `acyclic`

Extensible command-line interface for Acyclic SDK tools.

`acyclic git` is an optional Git-shaped façade for a managed Acyclic workspace:

```sh
acyclic git status
acyclic git commit -m "checkpoint"
```

It sends the argv after `acyclic git` to the authenticated local control plane.
The CLI never opens plugin storage and never intercepts or replaces ordinary
system `git`. Without a managed-workspace capability, `acyclic git` fails
closed.

The lifecycle integration provides exactly three environment values during a
managed child command:

- `ACYCLIC_CONTEXT_VERSION=1`
- `ACYCLIC_CONTROL_ENDPOINT=unix://...` or `npipe://./pipe/...`
- `ACYCLIC_WORKSPACE_TOKEN=<opaque base64url capability>`

The lifecycle owner must create the endpoint with per-user/per-session access
controls and an unguessable name, validate the bearer against the active route
and epoch, and revoke both endpoint and token when the managed session ends.

The previous recursive Harness demonstration remains available explicitly:

```sh
cargo install acyclic-cli
acyclic harness-demo
```

For application integration, use [`acyclic-harness`](https://docs.rs/acyclic-harness/latest/acyclic_harness/) or the [`acyclic-sdk`](https://docs.rs/acyclic-sdk/latest/acyclic_sdk/) umbrella crate.
