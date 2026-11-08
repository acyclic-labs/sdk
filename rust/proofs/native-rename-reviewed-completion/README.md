# Rename completion identity and work

Run `node rust/proofs/qualify.mjs native-rename-reviewed-completion`.

The production rename plan carries only the cached identities it already prepares. The shared checkout installer checks those identities against the final rebased candidate before adoption. Only identities present in that exact candidate inherit the installed completion stamp. Candidate adoption still refuses a concurrent root change; this adds no post-effect lookup or clock read.

The real shared-source cache control replaces a cached descendant externally, both before and after rename preparation. It executes the actual source rename, prepared installation and invalidation path. The replaced identity must invalidate while its unchanged sibling remains bound. The retained mutation certifies every prepared identity without verification and must fail that original assertion.

The existing finite-work property now also measures the actual item count, refuses the final completion-item cut before the effect, and succeeds at the exact count. Its retained mutation moves the completion reservation after source adoption and must expose a changed namespace on refusal. The same restored run retains the original allocation-prefix, accepted/refused directory-reply, cached-name and concurrent-FORGET controls.

Completion uses one existing ledger reservation instead of a non-reserving admission probe. It records the fixed funded work without a fallible post-effect admission. A refused directory row drops its original credits and preserves its cached miss. There is no new producer, wait, task, timer, durability barrier or round trip.

Rename preparation already traverses resident bindings and directory streams. Each moved binding now adds its original file identity and a verification bit in that existing plan, and one final-candidate namespace lookup. A single lazily constructed inspector serves the entire plan; removal-only plans create none. Path conversion and checkout traversal use the existing metered buffers and accounting. Their allocations, copies and provider reads remain path- and backend-dependent; neither constant whole-rename cost nor native kernel mount survival is established here.
