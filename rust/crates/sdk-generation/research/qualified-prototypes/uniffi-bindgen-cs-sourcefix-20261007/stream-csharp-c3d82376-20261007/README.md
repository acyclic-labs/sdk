# Actors.StreamBinding C# stream cohort

This is an isolated qualification prototype generated from the Rust-owned UniFFI stream producer. The generated cursor classes are produced by the maintained C# source generator; `StreamAsyncEnumerable.cs` only turns generated `Read`/`Follow` cursors into `IAsyncEnumerable<StreamRecord>` and closes each Rust cursor in `finally` with an uncancelled cleanup token.

The package carries the matching Windows and Linux native assets from the same Rust producer source and lock. It is a research artifact and is not published.