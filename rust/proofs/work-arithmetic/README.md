# Full-width work arithmetic

`performance::tests::full_width_work_arithmetic_preserves_exact_sums_and_refusals`
compares the production `WorkCounters::checked_add` with independent `u128`
arithmetic. Every one of the 24 declared fields is exercised alone and in mixed
receipts, with zero, maximum and arbitrary full-width inputs. Peak allocation
uses maximum; additive counters must refuse overflow without returning a wrapped
receipt. In-place addition must agree and leave its original receipt unchanged
on refusal.

Every generated invocation also exercises positive mixed fields bounded to half
the integer range, so every sum succeeds without overflow. This keeps successful
mixed additions and charges covered even when the full-width mixture overflows.

The same generated cases call actual admission and charging with unbounded,
arbitrary, exact and just-below budgets. A refused charge cannot partially change
any field. The separate existing borrowed-verification property independently
checks budget error contents and priority. These checks replace two overlapping
fixed-input addition tests; production counter code remains unchanged.

From the repository root:

```sh
node rust/proofs/qualify.mjs work-arithmetic
node rust/proofs/qualify.mjs work-mixed
```

The kept mutation removes the real `checked_add` overflow rejection. The original
property must fail at `full-width work sum disagreed with wider arithmetic`.
Compilation failure, empty selection and timeout do not qualify. The shared
driver restores exact source bytes and requires that selected property to pass.
The second mutation corrupts a completed sum only when both receipts contain
nonzero page reads and writes. Overflow still refuses correctly; the guaranteed
successful mixture must detect that corruption through the same wider oracle.

This establishes arithmetic and refusal behavior for these production methods,
not conservation of external reservations, native concurrency, complete physical
cost or end-to-end progress. Their implementation processes a fixed declared
counter set without I/O, allocation, waits or population traversal; no new
production operation is introduced here.
