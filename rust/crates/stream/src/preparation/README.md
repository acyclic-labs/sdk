# Settled effect selection and canonical condition membership

Effects retain request order because receipts name skipped effects by index.
Within each effect, normalization sorts conditions and mutations and rejects
duplicate paths. A soft effect skips atomically at its first failed condition;
a hard conflict refuses the entire publication. Fully skipped publications
retain their idempotency receipt.

Application and receipt validation share `settled_effects`: a lazy, borrowed
view of every effect and its original skipped-index subsequence. Each effect
compares at most once with the next skipped index; a match advances that one
cursor. Cloning preserves the partially consumed selection. Selecting and
counting effects allocates and copies nothing. Applied mutation references
are collected in caller-admitted backing and sorted across groups to preserve
the canonical path order of envelopes and provider admission.

Shape and receipt validation share the same forward condition cursor within
an effect. Each comparison consumes one condition or finishes one mutation's
lookup, so a complete walk compares at most C + M times. Fork-descendant
overlap validation is separate. The deleted mutation-specific SoftAppend
carrier scan has no counterpart: an effect group is the shared mechanism.

The production traversal property checks canonical mixed append/fork groups,
empty and populated skip subsequences, original reference identity, partially
consumed clones, exhaustion, and actual comparison counts. Its observers run
the same generic implementations as production's zero-sized observers.

```sh
bash rust/crates/stream/src/preparation/check.sh
```

The two retained patches replace the forward cursors with repeated binary
searches. The same property must fail at runtime with nonzero excess in both
selection and condition comparisons. A compiler error is not detection. The
script restores the exact source and requires the positive property to pass.
