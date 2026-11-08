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
retry or success-path work. Status and message retain their original strings.
The raw Connect cause is released instead of keeping a second diagnostic owner.
