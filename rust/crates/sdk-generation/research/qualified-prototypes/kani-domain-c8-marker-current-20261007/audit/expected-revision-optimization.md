# c8 expected-revision compile-only audit

Captured 2026-10-07 after the three production marker proofs. This is a source-binding and code-generation check only; it is not a mathematical verification result.

- Checkout: `Q:/sdk/work/sdkgen-actors-c8-minimal`
- Harness: `contract::generated::domain::kani_proofs::update_request_try_from_preserves_expected_revision`
- Production path: `UpdateActorRequest::try_from` and `From<UpdateActorRequest> for wire::UpdateActorRequest`
- Invocation: Kani 0.68.0, CBMC 6.11.0 banner, nightly-2026-08-21, `--only-codegen`, `--default-unwind 2`, one build job, offline mode
- Task-local target: `/tmp/kani-domain-c8-minimal-codegen-expected`
- Terminal exit: `0`
- Raw output: `audit/update-request-expected-revision-c8-only-codegen.raw.txt`
- Raw output SHA-256: `09FF6F99FD59190BC7B3B34A9C048403C7903AB3886492837EA77097CE4A1EDB`
- Launch source revision: `84a508449ea247297a0170db67377dd65a1baa41`
- Capture source revision after compile: `737775737a54c31c1d50b11c677ca9ae900b45b2`
- The intervening commits changed only README/metadata outside the bound Actors source; the bound domain, Kani, contract, wire, and lib.rs SHA-256 values remained identical to `audit/c8-source-inventory.json`.

The maintained harness already carries `#[kani::unwind(2)]`. The long original Actors proof's CBMC invocation also showed formula slicing (`--slice-formula`) and an effective unwind of 2. These are the available burden controls already present in the maintained proof setup. The compile-only probe confirms the real current production harness resolves and code-generates with that source path, but `--only-codegen` does not run CBMC and has no check count or theorem result. The expected-revision theorem therefore remains pending; no solver restart or proof-only validation algorithm was introduced.

The source harness constructs a fixed valid `wire::UpdateActorRequest`, calls the generated production `TryFrom`, asserts preservation of symbolic `expected_configuration_revision`, and round-trips through the actual production conversion. It does not call the separate `validate_update` admission helper, so its eventual claim must remain conversion preservation under the fixed valid fixture unless a separate production admission theorem is added.

The live original command confirms the concrete burden: CBMC is using `--unwind 2`, `--slice-formula`, CaDiCaL, `--object-bits 16`, and the generated expected-revision harness. The active formula tail was approximately 4,281,081 variables and 17,780,052 clauses, with no terminal result. A future bounded decomposition can remain source-bound while dropping only the round-trip equality assertion from a separate harness and retaining the same actual `UpdateActorRequest::try_from` call plus symbolic expected-revision accessor assertion. That would establish only field preservation under the fixed valid fixture; it would not prove admission, duplicate binding rejection, arbitrary strings, or whole-request round-trip. A second direct-production harness for admission must call the maintained `validate_update` helper itself, with bounded real wire inputs; it must not reimplement its HashSet scan. Neither decomposition is counted as a result until independently compiled and solved on an exact source freeze.
