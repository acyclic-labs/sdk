# Diplomat 0.16.1 typed Actors prototype

This isolated crate exercises the maintained Diplomat C++ backend against the
actual `acyclic-actors` semantic domain. It does not add annotations to the
Actors production crate. Every opaque value stores a canonical
`acyclic_actors::domain` value; constructors call domain constructors and
accessors read those values directly. No C++ request, response, or error
record mirrors the Rust fields.

The generated C++ surface covers all eight caller request families: create,
update, inspect, add subscription, remove subscription, resume subscription,
checkpoint, and invoke. All eight response wrappers preserve the canonical
actor observation through typed accessors (presence, identity, home region,
state, subscriptions, checkpoint, revision, and code digest). Invoke
additionally preserves response absence, status, body bytes, and ordered
headers. `ActorsError` keeps a typed error kind, the canonical Rust display
message, and numeric detail for contract or unknown-enum errors.

`ActorsClient` is a Rust-owned Tokio runtime with authenticated TLS CA
configuration. Its maintained Diplomat callback boundary exposes Rust-owned
handles for all eight operations with caller-driven `poll`, typed completion
and error state, and a real cancellation token. A live C++ consumer proves
bearer authentication, TLS, all eight typed responses, callback completion,
and server-observed in-flight abort against the package static library.

The generated C++ headers, static library, consumers, and negative strong type
compile receipt are staged outside the source tree at
`Q:\cpp-actors-diplomat-package-final`. The exact run is recorded in
`qualification-receipt.txt`. Q: is used for all target and package output to
avoid consuming the system volume.

This remains a qualification prototype rather than a production SDK cutover.
Foundation-port still needs to select the production annotation location and
metadata ownership. Diplomat 0.16.1 rejects a callback parameter carrying
both `Fn` and `Send`; therefore completion dispatch is deliberately
caller-polled and no automatic cross-thread callback is claimed. This is an
explicit maintained-tool gap, rather than a claim that the whole C++ SDK is
qualified.