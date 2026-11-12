# Cargo-derived Bazel definitions

Cargo.toml and Cargo.lock define packages and dependencies. Generated BUILD files,
MODULE.bazel and Cargo.Bazel files are disposable outputs; do not maintain them by hand.

With Cargo, Node and the pinned Bazel client installed, regenerate with:

```sh
node tools/build-graph/regenerate.mjs /path/to/generation-config.json
```

Alternatively set BAZEL_GENERATION_CONFIG and run the command without an argument.
The external JSON configuration has version 1, a nonempty `roots` array of Cargo
workspace package names, and `images` containing version 1 and `compile` and `test`
SHA-256 execution identities. Operators supply these identities for the immutable
execution environments enforced by their workers. An optional `windowsCompiler`
sets the absolute compiler path in that environment; its default is
`/usr/bin/x86_64-w64-mingw32-gcc`.

An optional `testCases` array selects existing test-binary cases. Each entry has a
Bazel target `name`, a test target `binary` label and a fully qualified `case` name.
Selections and deployment identities belong in user or CI configuration outside
this repository. Changing configuration requires regeneration.

Run `bazel test //:selected_tests` for the selected packages' tests. The generated
`//:selected_test_binaries` filegroup builds their executables without running them.
Additional case targets reuse those binaries and forward test arguments. The
supported targets are Linux x86-64 and, with `--config=windows-gnu`, Windows GNU
x86-64. GNU compilers, headers and runtimes come from the declared execution image.

The generator derives dependencies, build scripts, enabled features, test fixtures
and supported scalar development-profile settings from Cargo inputs. Unsupported
profile fields fail generation. Execution tools use Cargo build-profile defaults.
Tests require hermetic environments before their results can be shared safely.

The committed .bazelrc contains no endpoint. Supply user or CI options on the
command line or in untracked .bazelrc.local:

```text
build --remote_cache=grpcs://YOUR_ENDPOINT
```

Anonymous and fork CI clients read the cache without uploading local results.
The service must enforce authorization; client flags are not an access boundary.
Trusted clients configure remote execution and operator-issued mTLS credentials:

```text
build --remote_executor=grpcs://YOUR_ENDPOINT
build --tls_certificate=/path/to/service-ca.crt
build --tls_client_certificate=/path/to/client.crt
build --tls_client_key=/path/to/client.key
```

Use `--config=trusted` for trusted uploads. Keep keys in user or CI secret-file
storage outside the repository. Developer and CI clients use the same REAPI path.
The shared policy requests zstd compression and minimal output downloads; add
`--remote_download_toplevel` when retrieving executables. Place execution next to
the cache. An optional object store requires a same-region gateway endpoint.
After a region move, rewarm an empty local cache on demand rather than copying it.
