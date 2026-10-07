# Stream UniFFI shared foreign API

Date: 2026-10-07. Scope: research-only producer contract shared with Dart; no Dart handwritten recovery or transport implementation.

The maintained Rust producer exports the same opaque cursor boundary to every foreign consumer:

```text
StreamClientBridge.new()
RemoteStreamClientBridge.connect(endpoint, bearerToken, caCertificatePem)
append(path, value)
read(path, after, limit) -> RecordCursor
follow(path, from) -> RecordCursor
children(parent?, limit) -> ChildCursor
activeRecordCursors() -> u64
RecordCursor.next() -> StreamRecord?
RecordCursor.closeCursor()
ChildCursor.next() -> StreamChild?
ChildCursor.closeCursor()
```

The remote bridge uses the maintained Rust gRPC client with CA bytes and bearer metadata. `RecordCursor` and `ChildCursor` remain opaque handles over the original Rust `BoxStream`; a Dart `Stream`/`StreamController` adapter may call `next` until null and invoke `closeCursor` from its cancellation/finally path. It must not buffer, replay, reorder, translate recovery, or duplicate server cleanup. `StreamRecord.sequence` and `committedAtMicros` are `u64`; `StreamChild.path` is a Rust-owned string and record bytes remain byte arrays. Dart can consume this contract directly once its maintained UniFFI binding is generated from the same Rust producer.

The Kotlin proof uses the identical methods and observed ordered events, nonzero-cursor replay, typed errors, and server-side stream drop after cancellation. Source producer: `Q:\sdk\work\actors-stream-uniffi-kotlin-prototype-20261007\src\lib.rs`; Mac source archive: `source/mac-source-cohort-20261007.tar.gz`.
