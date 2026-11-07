# Bind child observations to their original selection

Run from the repository root:

```sh
node rust/proofs/qualify.mjs children-selection
```

The existing trusted-head/wire-budget control obtains pages from the public
MemoryStream and LocalStream entry points, after actual appends and a subtree
fork. It round-trips the original view through the real wire codecs. The
surviving property also checks the shared provider convenience operation.
No external response fixture or provider mock is constructed.

The same actual page must refuse a different parent, an inclusive cursor, a
stale captured hierarchy fence and an unsolicited tail projection. Reordering
or repeating its actual children, substituting an ancestor continuation and
moving the cursor backwards must refuse. Valid pinned and continued pages
remain accepted. An empty authorization projection may retain the provider's
original advancing continuation; a visible child is not required to own it.

The retained production mutation restores the original tail-only validation in
`Heads::validate_for`. It must fail the unchanged original-selection assertion;
exact restoration must complete the same selected control.

Validation replaces the existing child-tail traversal with one borrowed pass
over the bounded returned page. It adds no heap allocation, copy, provider read,
round trip, syscall, durability barrier or wait. Each comparison reads bounded
path text; no history or total-child population is scanned. The cursor must
advance whenever present, which prevents a malformed response from requesting
the same page indefinitely. This establishes response-binding safety and local
continuation progress, not authoritative flat ancestry, authorization coverage,
full-tree operation complexity or platform acceptance.
