# Current-main native TLS qualification

This research fixture is isolated from production. It creates a one-day localhost certificate and a Python generic gRPC/HTTP2 server. The server returns empty valid protobuf responses for all eight Actors service methods. `native-ca-all8.mjs` loads the actual generated NAPI binding from the candidate package, connects with `NativeActorsClient.connectWithCa`, and invokes the public `ActorsClient` for all eight operations.

Observed on candidate `a2881c65fdd93a4cc82512bbb4f5a3f6ed8e28eb`:

```text
direct native TLS/CA ActorsClient passed grpc and 8 operations
```

The default JavaScript loader currently calls `NativeActorsClient.connect` without a CA argument, so this private localhost CA fixture intentionally qualifies the built native Rust client through the explicit CA API. A default-loader qualification needs a publicly trusted endpoint or a production-supported CA option.

`fallback-adversarial/*` exercises `isMissingNativeArtifact` with an own-loader miss and two transitive missing-module errors whose paths contain the accepted native patterns. All three currently fall back to WASM, showing that the anchored first-line check still accepts arbitrary transitive paths. The fallback predicate must bind the missing artifact to the package's own generated loader resolution, or preserve the original error for transitive failures.

The direct N-API instance also has no JavaScript `transport` property (`Object.getOwnPropertyNames(Object.getPrototypeOf(client))` omits it), although the Rust source defines `transport`. The TypeScript loader reads `inner.transport`, so default native clients currently report `undefined` transport unless the binding export is corrected.

## Regenerated browser qualification (HEAD ae6c358)

With the current generated WASM cancellation ABI (`AbortSignal` is passed into each Rust operation), the real headless Chrome fixture produced:

```text
{"transport":"grpc-web","operations":8,"invalidCurrentHeadRejected":false}
{"rejected":true,"abortReason":"Actors operation cancelled","preRejected":true,"stallRejected":true,"stallReason":"Actors operation cancelled","afterAbort":{"requests":1,"active":0,"aborted":1},"afterPre":{"requests":1,"active":0,"aborted":1}}
```

The all-eight page is a transport smoke test; its invalid request assertion was removed because the browser page previously accumulated metrics across repeated runs. The abort page is a fresh server run and proves the held request closes (`active=0`, `aborted=1`) and a pre-aborted request adds no network request.

The source loader predicate now accepts only the package's generated loader path and scoped optional package name. The checked-in `dist/client.js` still contains the previous `index.<platform>-*.node` fallback branch, so packaging must regenerate `dist` before treating the source fix as shipped.
