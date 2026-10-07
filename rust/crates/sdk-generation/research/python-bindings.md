# Python bindings research

## Decision

Use **PyO3 0.29.0 plus maturin 1.14.1** as the Python packaging and native
extension backend. The Python surface must be generated from the Rust contract
model and Rust domain types. PyO3 wrappers may provide Python naming,
iteration, and exception ergonomics, but they must call the canonical Rust
client and constructors. A Python file must not become a second contract or
implementation.

Keep **UniFFI 0.32.2** as a comparison and a possible backend for small,
object-oriented embedded components. It does not pass the current default for
the remote SDK because its Python generator does not provide the required
stream cancellation and packaging behavior without additional handwritten
adapter code. Its UDL and `uniffi.toml` configuration would also create a
second interface description unless proc-macro metadata is used consistently
and the generated surface is checked against the Rust contract model.

The version pins are the versions to record in the generation manifest and
lockfile. They were selected from the upstream release and package pages:
[PyO3 0.29.0](https://github.com/PyO3/pyo3/releases),
[maturin 1.14.1](https://github.com/PyO3/maturin/releases), and
[UniFFI 0.32.2](https://docs.rs/uniffi/latest/uniffi/).

## What PyO3 and maturin provide

PyO3 is a Rust-to-Python extension API, not a schema generator. Rust types
marked with `#[pyclass]`, methods marked with `#[pymethods]`, and conversion
implementations define the Python runtime surface. That is a good fit for this
repository when those declarations are emitted or checked by the Rust
generation entrypoint and each constructor delegates to a Rust domain type.
The Python object can therefore hold `ActorId`, `CodeSha256`, `PositiveU64`,
and future nominal values without turning them into unchecked strings,
integers, or dictionaries.

PyO3 0.29.0 supports CPython 3.15 and the free-threaded `abi3t` direction. Its
stable-ABI features still need an explicit minimum Python version; the default
release target should be `abi3-py310` for GIL-enabled CPython 3.10 and newer.
The 3.15 free-threaded build is a separate artifact using the `abi3t` family.
PyPy, GraalPy, and version-specific CPython wheels remain separate targets
until their import and behavior tests pass. The PyO3 feature reference
documents the distinction between version-specific extensions and `abi3`:
[PyO3 features](https://pyo3.rs/main/features).

maturin is the maintained wheel and source-distribution builder. It supports
PyO3 projects, stable-ABI wheels, manylinux repair, cross compilation, and
PEP 517 packaging. Its configuration controls wheel tags, Python source
packages, data files, and the extension module; it does not define the Rust
contract. Use a checked-in `pyproject.toml`, a locked Cargo graph, and a
generation manifest that records the maturin version and wheel hashes. The
upstream binding guide covers PyO3 detection, `abi3`, and platform wheels:
[maturin bindings](https://www.maturin.rs/bindings). Maturin 1.14 also has
PyO3 stub-generation support, but the generated `.pyi` output must be treated
as a Rust-derived artifact and checked for drift rather than edited directly;
see the [maturin changelog](https://github.com/PyO3/maturin/blob/main/Changelog.md).

PyO3 plus maturin has no ordinary browser runtime. A Python-in-browser build
would be a separate PyEmscripten/Pyodide wheel with its own ABI and package
tag. Maturin documents the current `pyemscripten_*_wasm32` and historical
Pyodide variables in its [environment variable reference](https://www.maturin.rs/environment-variables).
That artifact cannot reuse a native manylinux, macOS, or Windows wheel. The
SDK's primary browser path remains the Rust WASM binding used by the web SDK;
Python Pyodide support should be added only after a real wheel can be built,
loaded, and tested in the selected Pyodide release.

## What UniFFI provides

UniFFI generates Python, Kotlin, Swift, and Ruby bindings from Rust proc-macro
metadata or a WebIDL-like UDL. Its metadata pipeline records exported Rust
items and generates language-specific bindings, which is attractive for
shared embedded APIs. The upstream documentation describes the metadata IR
and generated binding stages in [the bindings IR guide](https://mozilla.github.io/uniffi-rs/next/internals/bindings_ir.html).
The built-in Python generator supports records, enums, objects, errors,
custom types, async functions, and callbacks. Python async functions are
driven by the foreign event loop; the generated module exposes
`uniffi_set_default_event_loop()` for calls made outside an active loop. See
[UniFFI Python configuration](https://mozilla.github.io/uniffi-rs/latest/python/configuration.html)
and [async/future support](https://mozilla.github.io/uniffi-rs/next/futures.html).

UniFFI's async support does not provide general cancellation by itself. The
upstream guide explicitly requires a library-specific cancellation channel and
mapping. That would be a second adapter protocol for the Stream and Harness
families unless the Rust-owned client exposes cancellation as part of the
exported semantic API. UniFFI also does not provide a browser binding
generator. WASM requires an external generator and target-specific scaffolding;
the [WASM guide](https://mozilla.github.io/uniffi-rs/next/wasm/configuration.html)
calls this out directly.

UniFFI is therefore viable for a deliberately small embedded component when
all exported types are Rust-owned and the generated Python package is wrapped
by a thin package layer. It is not the default remote SDK generator for this
repository until a prototype proves typed streaming, cancellation, recovery,
and package installation with the canonical Rust client.

## Source-of-truth and type fidelity

The Rust contract model remains authoritative for names, field presence,
numeric widths, enum values, wire identities, errors, capabilities, and
documentation. The Python generation entrypoint should consume that model and
the Rust semantic types, then emit:

* a PyO3 extension module whose constructors call Rust `new` functions;
* Python package modules that provide only idiomatic naming and protocol
  adapters;
* `.pyi` declarations generated from the exported Rust surface, with a
  `py.typed` marker;
* package metadata and examples tied to the exact Rust revision and wheel
  hashes.

Use frozen Python classes for nominal values that need runtime identity. A
`typing.NewType` or a bare alias would improve static checking but would not
prevent an invalid value at runtime. For values such as `PositiveU64`, the
extension must accept a Python `int`, reject negative, zero, and values outside
`u64`, and return the Rust-owned nominal class. Do not silently coerce through
`float`.

For high-level operation requests and responses, generate typed Python records
from the Rust model rather than exposing arbitrary protobuf dictionaries. The
wire codec remains in Rust so unknown enum values, presence, service details,
and numeric values survive the boundary. Python exceptions should preserve the
same stable error category, operation identity, transport code, and service
detail fields as the other SDKs.

## Repository viability

The current workspace already has the necessary Rust ownership boundaries:
`rust/crates/actors` contains the canonical domain, wire, and client layers;
`rust/crates/actors-napi` demonstrates a native Rust boundary; and the Actors
WASM crate demonstrates the browser boundary. There is currently no Python
crate, `pyproject.toml`, maturin configuration, UniFFI configuration, or
generated Python package in the repository. That means a Python target can be
added without migrating a competing Python implementation.

The first implementation should add a dedicated Rust-owned Python binding
crate and a generation manifest entry. It should depend on the canonical
Actors, Stream, and Harness crates rather than copy their request or response
types. A generated package may contain Python protocol adapters, but transport
selection, validation, retries, cancellation, and error conversion must stay
in Rust. Native wheels should be optional package artifacts selected by the
Python package installer; the public API should not expose platform feature
flags.

UniFFI can be prototyped against a small semantic fixture, but adding UDL for
the complete remote contract before that test would violate the source-of-truth
requirement. If UniFFI is used later, prefer proc-macro exports on Rust types,
check the generated metadata against the contract manifest, and keep
`uniffi.toml` limited to naming and packaging configuration. The UniFFI guide
also states that it generates bindings but does not provide an end-to-end
packaging solution, so wheel production would still need a maintained package
builder and release lane: [foreign-language binding tutorial](https://mozilla.github.io/uniffi-rs/next/tutorial/foreign_language_bindings.html).

## Qualification plan

Qualification is release-only for downstream Python wheels. Pull requests run
fast Rust contract tests, generation drift checks, and one warm native smoke
test when the Python binding files change. The full matrix runs on release or
manual dispatch and records wheel hashes, tool versions, and the Rust source
fingerprint.

| Area | Required evidence |
| --- | --- |
| Package install | Clean virtual environments install the exact wheel on CPython 3.10, 3.11, 3.12, 3.13, 3.14, and 3.15 for Linux x64, macOS arm64/x64, and Windows x64. Test `abi3` and the separate free-threaded artifact where supported. |
| Rust-owned types | ActorId, CodeSha256, PositiveU64, all generated records, enums, optional fields, bytes, and errors have runtime constructors and generated stubs. Invalid values fail before transport with the canonical error category. |
| Actors remote | All eight operations pass serialization, presence, authentication, default transport selection, service-detail preservation, unknown-enum handling, and idempotency tests against the matching fixture. |
| Stream | The Python async iterator receives ordered events, propagates terminal errors, supports explicit and task cancellation, closes resources exactly once, and recovers according to the Rust client policy after a reconnectable failure. Cancellation must be observed while the operation is in flight, not only after a response arrives. |
| Harness | Durable admission, operation identity, retries, uncertain acknowledgements, reconciliation, retained terminal events, and typed provider errors match the Rust conformance suite. A lost acknowledgement must not cause an unsafe duplicate. |
| Embedded/native | A native Python call exercises the same Rust implementation used by the other native bindings. There must be no independently translated Rust algorithm. Bytes ownership, thread/GIL behavior, and shutdown are checked under load. |
| Browser boundary | The native wheel is never used as browser evidence. If Pyodide is supported, build and install a matching PyEmscripten wheel and run the same semantic tests in Pyodide; otherwise record Python browser support as excluded while the Rust WASM web SDK remains qualified. |
| Reproducibility | A clean checkout regenerates the extension, stubs, docs inputs, examples, and wheels with the same pinned tool versions. A drift check fails when any generated Python file or package metadata is edited. |
| Documentation | Rust comments and examples generate Python API references and executable snippets tied to the wheel revision. The docs site shows the Python artifact's supported interpreter and platform matrix from the qualification receipt. |

The minimum passing prototype is Actors unary plus one Stream operation and
one Harness reconciliation scenario. It must install from a locally produced
wheel, use the generated stubs, and prove cancellation and error identity.
Only after that prototype passes should the binding be widened to the rest of
the contract.

## Recommendation for the generation pipeline

1. Extend the Rust contract manifest with a Python target that identifies the
   PyO3/maturin backend, exact ABI families, and the native and PyEmscripten
   capability sets separately.
2. Generate the PyO3 module declarations and Python stubs from the Rust model;
   keep the generated output in a clean artifact directory and reject drift.
3. Build native wheels with maturin from a locked checkout. Run Python
   qualification only on release or explicit dispatch, using local Linux,
   Windows, and macOS hosts when available to keep CI latency and cost low.
4. Keep UniFFI as a measured alternative for embedded APIs. Promote it only if
   its generated Python output passes the same nominal-type, stream,
   cancellation, recovery, Harness, and packaging gates without a separately
   authored contract.

