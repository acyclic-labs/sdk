# Native registry root admission

The existing fresh-process registry property exercises the production selector
before initialization can create state. Absolute overrides still create and open
the real registry. Empty and relative overrides, including invalid HOME values
on Unix, use the native fallback only when it is absolute. Every child runs in
an isolated working directory; fallback locations outside the fixture are only
inspected, never initialized.

Run the two original properties with:

```sh
cargo nextest run --locked -p acyclic-fs --all-features --lib -E 'test(=native_mount::registry::tests::explicit_state_directory_is_used_by_a_fresh_registry) | test(=native_mount::registry::tests::invalid_state_roots_use_the_fallback_in_a_fresh_process)'
```

Apply `rust/crates/filesystem/fuzz/mutations/native_registry_relative_root.patch` from this crate's
repository root to restore the prior nonempty-root admission. The invalid-root
property must fail with `invalid state root selected a registry according to
process cwd`. Reverse the patch and run both properties again. The child checks
selection before creation, so the negative control cannot write relative state.

Admission adds no filesystem operations, barriers, waits, allocations or copies.
It checks at most three process configuration candidates during initialization;
normal mount operations keep the initialized root. Missing absolute roots follow
the existing typed service-unavailable path. This property establishes directory
selection and real fixture initialization, not adversarial path-component custody
or complete native mount behavior.
