# @acyclic-labs/harness

TypeScript bindings for the Rust Harness v2 runtime.

```sh
npm install @acyclic-labs/harness
```

[Client views](docs/client-views.md) · [Demand and recovery](docs/client-demand.md) · [Harness protocol](https://github.com/acyclic-labs/sdk/tree/main/proto/harness)

The `@acyclic-labs/harness/browser` entry exports `BrowserTaskAuthority`,
`BrowserTaskRegistry`, `BrowserTaskRuntime`, and their generated contracts.
Initialize WASM with `initializeBrowserRuntime()`, register versioned machines
or `registerStockTurn()`, and open the runtime with explicit browser provider,
session, payload, and execution limits and a signed scope. Volume initialization
and task admission are separate operations. Model composition requires the
selected provider's capacity and request-token accounting callbacks and an
explicit compaction policy.

The browser adapter runs the ordinary Rust task lifecycle over IndexedDB
Filesystem and Stream providers. Worker and tab messages carry wake hints;
the retained journals determine admission, work ownership, and completion.
Carry the bounded wake cursor between pages. Keep an unresolved or yielded
work lease and resume that exact attempt rather than claiming another lease.

Durable mail binds a sender/message identity to one recipient and immutable
payload. The receiver mailbox record and its derived location commit atomically;
recovery checks the original record, commit, and complete value before reporting
success. Mailbox observation does not acknowledge model consumption. This
receipt-backed format uses `mail/by-recipient` and `mail/by-intent` paths.
Previous unindexed mailbox records are outside this format and are not loaded;
use fresh journals when moving from that format.
