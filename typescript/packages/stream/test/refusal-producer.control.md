# Original gRPC session refusal

Build the Stream and Objects WASM packages and their TypeScript dependencies,
then run:

```sh
bun test typescript/packages/stream/test/native-session-pool.test.ts --test-name-pattern 'gRPC Session rejection preserves original diagnostic bytes through the public provider'
```

The original Rust Objects writer produces the opaque refusal bytes at runtime.
An authenticated TLS peer returns those original bytes in gRPC trailers. The
public `GrpcStreamProvider.heads` crosses the actual HTTP/2 transport, callback,
WASM session pool and public error projection. The assertion requires original
status, message, detail bytes and product metadata and exactly one connection
and Session RPC. These derived operands are not attributed to a captured Stream
failure. Transport framing headers are excluded by the shared Auth contract.

Apply `typescript/packages/stream/test/refusal-producer.patch` and repeat the
same command. It must fail the original-status assertion. Reverse the patch and
require the unchanged test to pass. The retained mutation removes the producer's
envelope while keeping its category and terminal classification unchanged.

The adapter decodes the original detail header once and copies each retained
product metadata value once at the owned boundary. It adds one envelope, one
metadata array and the bytes they own. It adds no request, connection, wait,
or retry. Diagnostic decoding runs only on failure; the RPC response observer
retains the existing raw arrays until completion. Status and message retain
their original strings.
The raw Connect cause is released instead of keeping a second diagnostic owner.

The peer's `grpc-status` and `grpc-message` are authoritative even when its
opaque detail bytes happen to decode as a different Google Status. The adapter
uses Connect's existing trailer parser on just those two headers; it does not
introduce another status or details decoder. This adds one two-field Headers
object and one projected ConnectError on a peer failure. Product metadata uses
the existing Buffer encoder, without a separate TextEncoder object.

Apply `typescript/packages/stream/test/refusal-status.patch` and repeat the same
command. It must fail the original-status assertion while still supplying the
envelope. Reverse the patch and require the unchanged test to pass. This second
mutation substitutes Connect's detail-derived status for the actual peer status.

The shared `@acyclic-labs/auth/grpc-refusal` decoder receives the original
HTTP/2 header pairs. One AsyncLocalStorage scope binds each RPC to its original
response events; it has no registry, scan or history. The two event callbacks
retain the existing raw arrays without encoding success responses. Session and
unary calls share the same transport and decoder. This replaces the prior
per-Session transport allocation. A failure encodes each product header value
once using Latin-1, retaining repeated values separately.

Run the additional original-metadata control:

```sh
bun test typescript/packages/stream/test/native-session-pool.test.ts --test-name-pattern 'gRPC Session and unary refusals retain repeated original metadata bytes'
```

The TLS fixture passes repeated derived operands directly to Node's real
HTTP/2 trailer writer. `refusal-metadata.patch` substitutes the old Headers
projection and must fail the repeated-value assertion. `refusal-octets.patch`
substitutes the old UTF-8 encoding and must fail the original-octet assertion.
Rebuild Auth after each mutation and restoration. The decoder fuzzer captures
its seed pairs from Node's real HTTP/2 writer/parser using the original Rust
Objects diagnostic, then varies diagnostic octets and the header/trailer split:

```sh
node typescript/packages/stream/test/grpc-refusal.fuzz.mjs rust/codec-fuzz/corpus/protobuf/acyclic.objects.v1.ErrorDetail
```

Both metadata mutations must also fail that same decoder property. The corpus
is the existing Rust ErrorDetail writer corpus, not an invented wire fixture.

The same public-provider control covers unknown and permission-denied statuses.
The public category remains unavailable or access_denied independently of the
retained diagnostic. `refusal-category.patch` restores the old unary branch
that discarded the envelope for known categories; it must fail the status-7
case's original-status assertion. This removes a conditional and retains the
already-built envelope, with no additional conversion or allocation.
