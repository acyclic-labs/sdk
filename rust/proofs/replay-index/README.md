# Exact binary replay on original retained backing

```sh
node rust/proofs/qualify.mjs replay-index
```

The shipped MemoryStream append, fork, commit, recovery and native inspection
paths share `collections::radix::select` with the fixed-width collection and
retained commit index. A borrowed binary key interprets each original byte as
a continuation bit followed by eight data bits, ending in a zero continuation
bit. Length and every original byte participate in identity. No encoded key,
hash, padding, allocation, second inventory or population-dependent descent
is introduced. A short key stops at its own boundary and compares the original
branch owner's representative, which remains a member of that subtree.

For L request bytes, strictly increasing coordinates admit at most 9L+1
branches and one selected leaf or boundary. Complete comparison admits at
most L bytes and one length observation. Lookup therefore reserves at most
10L+3 observations. Insertion reserves two descents, that comparison and the
original identity observation: at most 19L+6. Checked arithmetic and compact
coordinate representation refuse before publication. Empty binary keys remain
representable in the general view; the public retry-key constructor rejects
empty requests under its existing contract.

Ordinary insertion prepays its bound. Prepared completion prepays lookup plus
insertion and the original sixteen result observations; recovery prepays one
lookup and its original eight result observations. These original reservations survive cancellation, allocation refusal,
budget exhaustion and arbitrary intervening insertions. No fresh admission is
needed after acceptance. Snapshot rebuild uses the same original backing and
comparison. No new wait exists: each descent advances its coordinate and ends
at the requested boundary, and original admission refusals remain typed.

Five retained production mutations separately omit comparison admission,
replace the terminal zero, descend beyond the requested boundary, and
underfund insertion or omit the recovery result credit. The real provider assertions must reject each before the
exact source is restored. The restored properties cover every data coordinate
of a 33-byte identity, prefixes, trailing zeroes, the protective maximum input,
absent short identities, a three-node fork tree and production snapshot rebuild.
The existing prepared property now uses binary prefixes, retains each original
recovery future, spans every data bit and shorter terminator through 318 writes,
then exhausts work and allocation and
cancels before completion and recovery. Collection cursor, fixed commit-bit,
copy-growth and refused pre-acceptance properties also run.

Costs: the original replay metadata remains 40 bytes on 64-bit targets (20 on
32-bit targets), replacing two optional AVL children and height with two tagged
children and one compact coordinate. Each new identity contributes at most one
branch in its original funded slot. Added index allocations, key copies, IO,
durability barriers, round trips, locks, tasks and waits: zero. Removed work:
recursive population-dependent comparisons, height updates and rotations.
Complete key comparison is now charged instead of being hidden inside each
ordered comparison. This proves request-width replay work, not constant cost in
input length or whole-request cost. Deployment transport topology, OS crash
durability and full-platform acceptance remain separate obligations.
