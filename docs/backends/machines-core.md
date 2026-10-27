# Machines core on Streams (G29)

Enable `acyclic-machines/core`. Supply the application's canonical authenticated
`StreamProvider` and one account path segment. The supplied Streams session
continues to enforce its existing scope; the account string grants no authority.

```rust,ignore
let operations = acyclic_machines::core::OperationRegistry::new(streams, "account")?;
let provider = acyclic_machines_daytona::DaytonaProvider::new(config, operations)?;
```

Admission precedes native dispatch. A keyed conditional commit publishes the point
projection and account operation log (`machines/<account>/ops`) together.
An identical successful retry returns the original result; a different intent is
a conflict. A pending or unknown operation refuses redispatch. Recovery never
infers success from a native resource's name or labels.

`watch` and `join` follow the point projection through Streams' checked cursor.
`SettledMutation` includes the operation, final `CommitId`, and exact result.
Large immutable results use bounded canonical records with a length, digest and
prefix anchor; success publishes only after those records are durable. This
preserves Streams' 64 KiB record limit and retains original outcomes independently
of later machine observations. Staged but unpublished bytes remain canonical
Streams data; retention and storage custody belong to the supplied backend.

Machine state and its state event (`machines/<account>/<id>/events`) publish in
one multi-stream commit. Cleanup ownership survives cancellation and process exit.
A created machine's native handle, initial observation/event, and ownership
publish in one conditional commit before readiness. Driver identity is part of
native admission; another backend cannot observe or clean up its resources.
Bindings live in the machine projection, without a second process-local map.
Each acknowledged deletion releases only that resource; a late create cannot be
lost by another cleanup pass replacing the ownership list. A successful settlement
never yields resources for cancellation to delete.

Adapters dispatch each native mutation once. Asynchronous native resources use
the shared bounded `settle` observer, which repeats reads, never mutations.
Accepted atomic checkpoints are owned before the first readiness read. Machine
and checkpoint cleanup use typed native addresses in the same operation record.
A native conflict does not establish snapshot ownership. An accepted Daytona
snapshot may not yet be readable by name: a 404 at that point retains deletion
debt. If a deletion reply is lost and its name subsequently disappears, cleanup
conservatively remains indeterminate; no absence poll manufactures an acknowledgment.
Checkpoint results and their original settlement replay after reopening.

Suspend/wake admission and policy transitions are shared too. Native readiness is
separate from public state: a Running VM may still be busy with a fork or snapshot.
A destroyed canonical observation cannot be replaced by a stale native read.
Fork rounds join every started sibling before reporting failure, retaining cleanup
ownership. Only an explicit definite native refusal permits dispatch backoff; a
lost reply never does. Machine/event page construction belongs to this core.
The raw backend migration is in progress; the operation authority alone does not
establish that all adapters meet the approximately 300-line budget.

Validation runs on bb2 without provider credentials. LocalStream reopen vectors
cover exact replay, stable settlement identity, indeterminate admission, cleanup
retention, atomic state/events, and a maximum-child fork result larger than one
record. MemoryStream vectors cover independent instances racing admission and
following completion. These are internal vectors, not captured provider responses.
The PR receipt records the tested source and results; these tests do not qualify
paid-provider execution or atomic snapshots.

Live Daytona runs require `DAYTONA_MACHINES_JOURNAL` naming a durable
account-specific directory. Restart with that directory and the original keys.

The current Daytona adapter targets the `/sandbox` lifecycle API. Its documented
[TypeScript waitUntilStarted method](https://www.daytona.io/docs/en/typescript-sdk/sandbox/#waituntilstarted)
uses state polling; that is the readiness mechanism currently bound here. The
[newer v2 SDK](https://www.daytona.io/docs/en/v2/typescript-sdk/sandbox/#sandboxeventsconfig)
documents a WebSocket events hub for its different API. This draft does not claim
that no native push mechanism exists, or silently substitute that API for the
selected one. Raw completion-backed adapters should await native completion;
operation watch/join always follows Streams rather than a readiness timer.
