# Shared fixture initialization

The library compiles the same initializer used by the SDK generation CLI
regressions. An initializer error preserves its original value and leaves the
slot available. An initializer that unwinds poisons the original standard-library
mutex; later callers receive `InitError::Poisoned` without rerunning initialization.

Run the real standard-library owner-loss check:

```sh
cargo test --locked -p sdk-fixture-verification --lib tests::lost_initializer_owner_refuses_the_slot_without_reinitializing -- --exact
```

The retained mutation restores the original unchecked mutex acquisition. The
same owner-loss check must fail when that mutation is applied:

```sh
git apply rust/verification/sdk-fixtures/mutations/unwrap-poison.patch
git apply -R rust/verification/sdk-fixtures/mutations/unwrap-poison.patch
```

The Loom configuration exercises the same initializer's concurrent publication,
retry after a returned error, and non-nested slot ownership:

```sh
RUSTC_WORKSPACE_WRAPPER=./scripts/loom-rustc.sh cargo test --locked -p sdk-fixture-verification --features model-checking --test fixture-model -- --exact package_is_initialized_once_and_published_whole failed_initialization_leaves_the_slot_retryable producer_then_cfg_slots_publish_without_nested_locks cargo_and_fixture_tool_outputs_publish_as_one_initialization model_detects_initialization_outside_the_lock
```

Loom 0.7.2 does not return standard-library mutex poison to its caller. Its
mutex adapter unwraps that error internally, so poison refusal is checked with
the real standard-library mutex rather than claimed from the Loom run.
