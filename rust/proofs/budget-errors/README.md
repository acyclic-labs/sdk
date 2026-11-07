# Exact budget refusals

Run `node rust/proofs/qualify.mjs budget-errors` from the repository root.

The existing 128-case property calls production `WorkCounters::verify_within`
and compares the complete typed result with its independent ordered-field oracle.
Each invocation retains its full-width mixed input and budget, including the
unbounded path and first-error priority. It also forces an isolated refusal for
each declared field, so an earlier error cannot hide a later field's identity.

The retained mutation mislabels the final `materializations` error in the actual
production verifier. Qualification requires the intended exact-counter assertion
to fail, restores the original source bytes, then requires the same property to
pass. Compiler errors, empty selections and timeouts do not qualify.

The property subsumes `budgets_fail_on_the_exact_counter`, which covered only
page reads and source entries. Serialization comparisons of unchanged counters
are removed: the production method safely borrows an all-integer value. Original
budget address, verification-count, physical-allocation and completed-receipt
checks remain in the separate kernel completion control.

Verification examines a fixed declared counter set, with no wait or external
operation. This control covers error safety; it does not establish cost or
progress for arbitrary filesystem operations or native backends.
