# @acyclic-labs/pi

Pi bridge for Harness v2. `piProvider` receives the same typed `ModelRequest` as every Harness model adapter and accepts a custom, synchronous or asynchronous projector. The injected `resolveFile` must enforce the owner's read grant; projected bytes are checked against length and SHA-256 before dispatch. Unsupported policies or types fail explicitly. Canonical conversation storage remains ref-only and unchanged by projection.

Pass `contextCapacity` and `countTokens` on the bridge to supply accounting for the exact selected model and prepared request. The Pi adapter forwards these synchronous callbacks without projecting files or dispatching a model request. Token counts must bind `serializedInput` and include framing, tools, options and media. Missing callbacks reject accounting explicitly; the adapter supplies no model catalog or estimated tokenizer.
