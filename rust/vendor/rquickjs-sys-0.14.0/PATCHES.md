This is the build source of the published `rquickjs-sys` 0.14.0 crate
(SHA-256 `cee271d0eeba64f0915b846cb7ae02e16faf3dfdffdca91731101d9d30fe3423`).
Platform bindings, WebAssembly support and required C sources are retained;
unrelated upstream examples, tests and development files are omitted.

`JS_ToCStringLen2` releases its owned `val` at the shared failure label,
matching `JS_ToCStringLenUTF16`. Refusing either the narrow or wide UTF-8
temporary allocation previously leaked the source string reference. The
successful borrowed and allocated return paths are unchanged. Failure adds
one reference release and no allocation, copy, syscall, lock or wait.

The existing `code_mode` fuzz target checks actual temporary-allocation
refusals and original native-ledger conservation through engine destruction.
`code_mode_leak_utf8_source.patch` removes this release as a retained negative
control. Its generated Unicode inputs are identified in `corpus/SOURCES.tsv`.
