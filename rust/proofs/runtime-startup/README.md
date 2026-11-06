# Native runtime startup retirement

The existing
`executor::code_mode::allocator::tests::real_engine_releases_native_memory_and_preserves_ledger_refusal`
property constructs the real QuickJS runtime with the production `EngineAllocator`.
It first measures successful startup on its original native ledger, then admits
every shorter allocation prefix. A refused or recovered startup must return all
engine bytes to that same ledger baseline. A subsequent real interpreter call
covers funded execution and refusal with no engine-byte credit.

Run from the repository root:

```sh
cargo test --locked -p acyclic-harness-core --features filesystem --lib executor::code_mode::allocator::tests::real_engine_releases_native_memory_and_preserves_ledger_refusal -- --exact --nocapture
```

The retained `mutation.patch` removes `JS_FreeRuntime` from the allocator-backed
constructor's real `opaque.initialize` failure branch. The pointer remains
validated, the original allocator supplies the refusal, and construction returns
its original error. The property must reject the leaked engine bytes at
`runtime allocation prefix`; a compiler error, signal or unrelated assertion is
not this negative control. Restore the source before running the same property:

```sh
git apply rust/proofs/runtime-startup/mutation.patch
git apply -R rust/proofs/runtime-startup/mutation.patch
```

This uses the production constructor and allocation accounting, with no
replacement engine, allocator mock or hand-written state machine. The mutation
is generated from the retained constructor source. It adds no test and replaces
no existing coverage. The constructor introduces no wait: either construction
returns its runtime or unwinds the original allocations before its allocator
drops. Success-path syscalls, durability barriers, round trips, allocations and
copies are unchanged. Refused partial initialization needs one runtime destructor;
the mutation removes that required work.

This control covers the allocator-backed constructor and measured startup
allocation prefixes. It does not independently identify every class-registration
branch, qualify the system-allocator constructor, measure total engine complexity
or establish Windows/macOS execution. Successful construction and interpreter
work remain proportional to the engine's requested work. Runtime qualification
requires the intended negative assertion and a passing exact restoration on the
same source; this document alone is not evidence of either result.
