# Maintained UniFFI 0.31 Kotlin template patch

This patch is applied to the pinned `uniffi_bindgen-0.31.0` source before
building the standalone Actors bindgen binary. It changes only the generated
Kotlin opaque-object constructors from public to `internal`:

* the `(UniffiWithHandle, Long)` raw-handle constructor remains available to
  the generated converter and lifting code in the same Kotlin module;
* the `NoHandle` test constructor remains available to generated same-module
  tests; and
* external Kotlin callers can no longer construct an opaque wrapper around an
  arbitrary native handle or use the fake-object constructor through the
  package API.

The generated source is never edited after bindgen runs. The patch is against
the maintained upstream 0.31.0 template and retains that template's MPL-2.0
license and notice. The patch is a qualification candidate until the upstream
generator accepts an equivalent visibility option.
