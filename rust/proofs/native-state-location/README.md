# Absolute ambient state locations

The production native environment resolver accepts an explicitly injected root,
then an absolute process data override, then an absolute platform default.
Ambient home discovery also requires an absolute path. Empty or relative
ambient values cannot make persistence or user settings depend on the current
working directory. Explicitly injected paths retain their caller-owned contract.

The existing fresh-process property calls the production `state` and `home`
entry points with real process environments. Its cases cover a literal absolute
override, empty and relative overrides, relative HOME, and explicit injection.
The relative-HOME case supplies its own expected refusal rather than deriving
that expectation from the directory dependency.

Run the restored property from the repository root:

```sh
cargo test --locked -p acyclic-harness-core --lib --no-default-features native_environment::tests::native_state_honors_literal_data_override -- --exact --test-threads=1
```

Each mutation changes the actual production resolver, leaving that property
unchanged. Apply one mutation, run the same command, and reverse the mutation
before the next control. Require an actual failing property with its original
assertion, not a compiler refusal, missing test, signal or timeout. Run the
restored property again after all mutations are reversed.

| Mutation | Platform | Required failure |
| --- | --- | --- |
| `omit-data-override.patch` | macOS | Absolute XDG_DATA_HOME override differs from the platform default |
| `relative-home.patch` | Unix | `relative HOME reached user settings` |
| `relative-data-default.patch` | Unix | State root is relative instead of unavailable |

For example:

```sh
git apply rust/proofs/native-state-location/relative-home.patch
# Require the runtime property failure described above.
cargo test --locked -p acyclic-harness-core --lib --no-default-features native_environment::tests::native_state_honors_literal_data_override -- --exact --test-threads=1
git apply -R rust/proofs/native-state-location/relative-home.patch
```

The resolver adds no wait, filesystem operation, durability barrier or
allocation for validation. Absolute-path predicates are lexical; accepted
override storage moves into PathBuf without a payload copy. The native default
keeps its existing platform lookup costs. No constant-time claim is made over
arbitrary path byte length or process environment size.

This property isolates environment resolution; it does not qualify native
process capture or the shipping binary's registry composition. Runtime
qualification of the mutations remains required; mutation applicability is
only a source check.
