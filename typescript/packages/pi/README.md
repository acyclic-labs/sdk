# @acyclic-labs/pi

Pi bridge for Harness v2. `piProvider` receives the same typed `ModelRequest` as every Harness model adapter and accepts a custom, synchronous or asynchronous projector. The injected `resolveFile` must enforce the owner's read grant; projected bytes are checked against length and SHA-256 before dispatch. Unsupported policies or types fail explicitly. Canonical conversation storage remains ref-only and unchanged.

Pass `contextCapacity` and `countTokens` on the bridge to supply accounting for the exact selected model and prepared request. The adapter forwards these synchronous callbacks without projecting files or dispatching a model request. Token counts must bind `serializedInput` and include framing, tools, options and media. Missing callbacks reject accounting explicitly; the adapter supplies no model catalog or estimated tokenizer.

The native recursive-agent extension is installed by `acyclic install pi`;
its service/task authority is not supplied by `piProvider`. That extension keeps
Pi's native model/context/compaction/retry loop. Installation and the typed model
bridge do not qualify recursive process execution. See the SDK's
[`plugin/pi/README.md`](../../../plugin/pi/README.md) for durable allocation
discriminators, source-bound CLI controls and qualification commands.
