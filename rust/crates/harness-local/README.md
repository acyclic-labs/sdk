# acyclic-harness-local

The durable native host behind the [`acyclic-harness`](../harness) facade. It
owns the local filesystem, checkout and agent tree, and serves that tree to
in-process and remote clients through one `server::serve`.

`LocalHarnessConfig` selects the checkout or project volume, the model binding,
the state directory and the pinned configuration layers. `LocalHarness::prepare`
initializes the saved home before credentials bind; `LocalHarness::open` opens
durable state and assembles the host; `LocalHarness::doctor` performs read-only
configuration, workspace, mount and custody checks without creating state.

`server::serve` is the one entry point for a managed deployment and a
self-deployed host: both supply the same `ServerConfig` and speak the same
stream session protocol. `local::Connection::connect` reaches either, the
endpoint being the only difference. A plain HTTP listener serves only a loopback
address; a non-loopback address requires a TLS identity.

The facade re-exports this crate's `local`, `server` and `test_support` modules,
so applications normally depend on `acyclic-harness` and use
`acyclic_harness::local::LocalHarness`. Depend on this crate directly only when
composing the native host without the facade.

See the [harness README](../harness/README.md) for the shared runtime contracts
and the [repository docs](../../../docs) for composition guides.
