This is the build source of the published `rquickjs-sys` 0.14.0 crate
(SHA-256 `cee271d0eeba64f0915b846cb7ae02e16faf3dfdffdca91731101d9d30fe3423`).
Platform bindings, WebAssembly support and required C sources are retained;
unrelated upstream examples, tests and development files are omitted.

The archive's `.cargo_vcs_info.json` identifies `sys` at commit
`d7ef5eeae702fea24c03643064de454f1c1dd4b0`. Its package license is MIT;
the bundled QuickJS license and wasi-libc dependency notices are retained.

`JS_ToCStringLen2` releases its owned `val` at the shared failure label,
matching `JS_ToCStringLenUTF16`. Refusing either the narrow or wide UTF-8
temporary allocation previously leaked the source string reference. The
successful borrowed and allocated return paths are unchanged. Failure adds
one reference release and no allocation, copy, syscall, lock or wait.

The existing `code_mode` fuzz target checks actual temporary-allocation
refusals and original native-ledger conservation through engine destruction.
`code_mode_leak_utf8_source.patch` removes this release as a retained negative
control. Its generated Unicode inputs are identified in `corpus/SOURCES.tsv`.

The build script tracks every copied C source, header and bundled binding
at its original path. Tracking only the output copies let Cargo reuse a
native library after an input changed. The existing copy loops also emit
the input dependencies; no additional copies or runtime work are needed.
