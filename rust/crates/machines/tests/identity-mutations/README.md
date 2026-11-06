# Identity contract negative controls

These patches change the actual Machines identity owner or its WASM boundary.
They are retained defects, never alternative implementations.

The native `contract::identity::resource_identity_tests::normalized_identity_preserves_retained_ids_and_domain` property checks retained UUID spellings, empty-label refusal, nil-label normalization, the protocol domain, kind separation and UUID version/variant. Apply each of `domain`, `stamp`, `retained` and `empty` separately and require that exact property to fail at runtime; restore the source before proceeding.

`wasm-kind.patch` drops the caller's kind at the real exported boundary. Build `acyclic-machines-wasm` for `wasm32-unknown-unknown`, generate its JavaScript with the workspace-pinned wasm-bindgen, and run the existing Machines test named `normalizes public identities through the Rust contract`. Its independent Node SHA-256 protocol check must reject the defect. Restore and rebuild before requiring the same test to pass. The previous test only compared two callers of the same WASM function and could agree after kind loss.

No captured compiler, provider or operating-system fixture is synthesized here. The WASM control uses the real compiled module and normal package import. Hashing necessarily visits the current input bytes; identity normalization performs no I/O or history-dependent work.
