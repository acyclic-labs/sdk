---
name: debugging
description: Diagnose a reproducible failure or unexpected result and fix its cause with discriminating evidence.
---

Capture the failure, expected behavior, and smallest reproduction that preserves the relevant conditions. Identify the first boundary where observations diverge from the contract. Keep observations separate from hypotheses.

Choose the next check to distinguish competing causes: trace a value across that boundary, vary one relevant condition, or compare with a working control. Independent investigations may proceed in parallel when their inputs and ownership are clear. Update the hypothesis from the result before changing more code.

Fix the violated invariant at the layer that owns it. Avoid hiding the symptom with retries, larger limits, or special cases. Verify the original reproduction now succeeds and that a negative control still detects the failure. Check affected neighboring behavior, then remove temporary instrumentation that serves no continuing purpose.
