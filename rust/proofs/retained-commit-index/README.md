# Retained commit selection on original backing

```sh
node rust/proofs/qualify.mjs retained-commit-index
```

The shared `collections::radix::select` walks both the boxed `FixedIndex`
links and Streams' original retained inventory. Each adapter exposes one
physical branch or leaf; neither allocates or copies keys. Branch coordinates
increase along every original path. Exact lookup visits at most 256 branches
and one leaf, then admits all 32 key bytes before comparing identity. Insertion
reserves the shared width-derived bound plus its original new-leaf observation
before publication. Completion records that same retained reservation, without
checking a cancelled caller or obtaining fresh admission.

The original MemoryStream property publishes 1,000 real append envelopes,
queries old commits and retained records as history grows, and rebuilds the
same derived index through the production snapshot codec. Physical work is
counted at the original production observation sink, including all key bytes;
the initial one-leaf public read must report33 examinations. The original sink
also rejects comparison work with a typed refusal. The retained mutation removes
that production comparison admission. The same public-provider assertion must
fail before the exact source is restored. Native Heads deliberately refuses
retained-record requests; this control neither enables that surface nor claims
its output allocation/copy funding.

Restoration also runs the existing every-key-bit property, original native
append/coordinated/fork publication refusals, and prepared completion through
62 interleavings after original allocation/work exhaustion and cancellation.
The shared collection's original adversarial-width and refused-publication
properties cover both boxed cursor adapters, including keys wider than256bits,
zero width, cancelled retirement and retained funding. No parallel model or
mock index is used. Selection is synchronous and introduces no event or wait;
progress follows its strictly increasing coordinates and original typed work
refusals. Existing prepared completion retains its original publication event.

Costs: zero additional index allocations, payload copies, IO, durability
barriers, transport calls, locks or tasks. The original retained branch layout
stays compact; byte/offset coordinates are expanded only on the stack. This
control establishes fixed-width selection and comparison accounting, not full
request cost. Mutation search inside an envelope, output allocation/copy
accounting, variable-width replay AVL lookup, retained-vector growth, all-platform
execution and deployment topology remain separate obligations.
