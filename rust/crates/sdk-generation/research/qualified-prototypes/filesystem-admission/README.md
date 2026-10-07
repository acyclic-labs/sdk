# Installed filesystem admission qualification

This prototype qualifies the runtimes that are actually present in an extracted
filesystem package. It does not rebuild Rust, regenerate WASM, or copy generated
assets into the repository. The runner takes the package-owned WASM files and a
package-owned N-API companion as inputs, then records their hashes beside the
observed admission results.

Run it only after package assembly has extracted the archive and after the
native companion has been installed:

```text
node research/qualified-prototypes/filesystem-admission/run.mjs \
  --package-root <extracted-@acyclic-labs-fs> \
  --native-binding <installed-@acyclic-labs-fs-platform>/acyclic-fs-<version>-<target>.node \
  --source-commit <checked-out-commit> \
  --archive <packed-@acyclic-labs-fs.tgz> \
  --receipt <qualification-directory>/filesystem-admission.json
```

The runner invokes `liveRebase` through the generated WASM module and through
the installed N-API binding. Each call uses the same Rust-owned boundary
parameter. A value is classified as `boundary_rejected` only when the runtime
returns the exact Rust admission error or the exact wasm-bindgen JavaScript
number type guard; downstream Rust policy errors are recorded separately so a
valid `u32` value cannot be mistaken for a boundary failure.

The required matrix is:

| Class | Values |
| --- | --- |
| Numeric edges | `0`, `1`, `4294967295` |
| Numeric rejects | `-1`, `0.5`, `1.5`, `4294967296`, `NaN`, `Infinity`, `-Infinity` |
| Non-number rejects | `null`, `"1"`, `true`, `false`, `undefined`, `1n`, `new Number(1)`, `Symbol("1")` |

Both runtimes must reject every value in the two reject rows at the boundary.
The three edge values must pass boundary admission and reach the expected Rust core outcome for the non-fork fixture: zero reaches the configured-bound error, while one and u32::MAX reach the non-fork error. Unknown errors fail the gate.

The receipt binds the result to the bytes tested. It contains the checked-out
source commit, the hash computed from the packed archive, package-relative WASM JS/WASM hashes, native
binding hash, matrix digest, and every observed result. Release assembly must
fail if any package-owned byte differs from the bytes used to create the
receipt, or if either runtime has a non-number that reaches downstream policy.

This is a qualification input and receipt format. It intentionally contains no
generated WASM, native binary, or duplicated numeric validation logic.
