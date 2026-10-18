# Objects v1 batch RPCs

`GrpcObjects::put_batch` and `get_batch` use the canonical `PutObjects` and
`GetObjects` streaming RPCs. Each nonempty batch admits 1–1000 ordered objects
once, authorizes every selection before work, and reports individual outcomes.
The canonical RPCs also have HTTP bindings at `objects/put-batch` and
`objects/get-batch`, relative to `/v1/objects/`. Rust `HttpObjects::put_objects`
and `get_objects`, and TypeScript `HttpObjectsV1.putBatch` and `getBatch`, expose
the canonical generated frames. Upload frames use NDJSON and return one JSON
response; download selections use JSON and return indexed NDJSON frames.

Bodies retain the single-object framing and allocation bounds. Uploads begin
with every object's header, followed by each object's indexed body chunks and
completion marker, then the batch completion marker and EOF. The complete
encoded header frame must fit the 16 MiB transport limit. Downloads identify
every selection and its decoded byte bound before the server starts work.
Their indexed headers, bodies and terminal outcomes retain input order.

Empty client batches return immediately without acquiring a credential or
opening an RPC. Retrying a publication batch uses the original per-object
mutation identities; cancellation can leave earlier items published. Each
object is atomic independently. A failed item does not change its peers'
ordered outcomes. Malformed transport framing fails unacknowledged selections;
native batch failures retain valid acknowledgements as receipts.

Native filesystem publication uses `GrpcObjects::put_batch_native` with the
caller's original meter. One batch write admission funds one RPC. The upload
keeps the original request guard, and returned receipts and their result vector
retain allocation custody until destruction. Cancellation is checked while the
transport polls upload frames; an early reply cannot acknowledge an unfinished
upload. Complete unique upload chunks transfer their vectors into protobuf
frames; shared and split chunks retain their contents through bounded copies.

The native upload encoder transfers its already encoded user-metadata maps to
the reply decoder. Returned user metadata must match those dispatched entries;
the decoder verifies their values without inserting or allocating map nodes.
It records valid accepted writes synchronously when it decodes a reply, before
waiting for transport trailers. Invalid peers, cancellation, incomplete uploads
and failed trailers retain the valid receipts in the batch failure outcomes.

Reply completion reserves codec work and string allocation credits before
sending the upload. Decoding consumes those credits without checking the live
cancellation state. Each item owns its transferred metadata and new string
capacity until validation succeeds; rejection retires that custody before the
next item is decoded. Exchange locks use the crate's ranked synchronization
shim, including in the Loom model.

HTTP header frames admit the canonical 16 MiB protobuf envelope and up to six
JSON bytes per wire byte for escaping. Indexed body frames retain their 64 KiB
payload bound. TypeScript encodes and bounds each frame before examining the
next frame.

The required qualification command is `bash scripts/qualify-objects-batch.sh`
(on the build box through `build-slot --remote`). It runs the batch properties,
the production exchange and upload-completion Loom models, the
`objects_batch_reply` libFuzzer target, and production mutation controls for
lost metadata handoff, late cancellation, retained rejected-item charges, and
lost decoded results.
The target uses `rust/crates/objects/fuzz/corpus/batch_reply` as its shared seed
corpus. Its stable qualification run is bounded to 256 mutations; the same
target is available to cargo-fuzz for longer instrumented runs.
