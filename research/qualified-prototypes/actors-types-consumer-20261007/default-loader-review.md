# Default loader review

Review target: `Q:/sdk/work/sdkgen-main-port-current`, inspected at HEAD
`0093da12d7d4205d56db1c25fb75d50b59de11d6`.

The Node root facade selects the platform package
`@acyclic-labs/actors-<platform>-<arch>` and falls back to the packaged WASM
bridge only when that exact optional package is absent. The current package
manifest contains no optional platform dependency, no platform companion
package, and no `./native` export, so native selection is not install-qualified.
The missing-package check is narrow (`ERR_MODULE_NOT_FOUND`/`MODULE_NOT_FOUND`
with the requested package in the message) and does not swallow transitive or
corrupt-module failures.

The browser facade is wired to the generated WASM module. The checked-in
browser conformance runner still exercises `HttpActorsClient` and the old HTTP
fixture, so it does not qualify `ActorsClient`'s browser branch. A
browser-context smoke test must instantiate the root client, assert the WASM
transport, and exercise at least one operation through the generated module.

The isolated positive and negative fixtures import `semantic` from the package
root (the current package has no `./types` subpath) and pass after the package
is built. They provide compile-time evidence for brands, bigint, required
oneof presence, and readonly client views; they do not replace a native/WASM
runtime fixture.

An isolated copied-package install with a platform companion shim exercised the
root default Node loader and all eight operation methods, proving the expected
companion module shape and result envelope. The candidate itself still ships
no real platform companion package; the shim is only loader qualification.

The generated Rust WASM bridge was also exercised through the default browser
branch by removing the Node process marker and serving a local gRPC-Web stub.
All eight operations reached the stub with `grpc-web` transport, while Rust
rejected `currentHead=false`, a 31-byte SHA digest, and an empty ActorId before
the request reached the stub. This validates the actual WASM admission path;
an interactive browser process was unavailable in the review environment, so
this is a browser-branch/runtime qualification rather than a browser UI run.

The generated semantic declarations inspected from the qualified bundle also
use mutable object properties and `Array` collections. The negative fixture
therefore includes mutation expectations so the public immutability policy is
checked rather than inferred from runtime behavior.
