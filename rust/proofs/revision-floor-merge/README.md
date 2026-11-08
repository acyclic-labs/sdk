# Revision floor merge

Run `node rust/proofs/qualify.mjs revision-floor-merge`.

The existing generated minimum-cut property calls the public `RevisionTracker`
and `RevisionCut` APIs. It compares the streaming floor with independent
canonical construction for empty inputs, explicit participants before, at and
after the observed participant, increasing and decreasing indices, and foreign
incarnations. It uses the production merge, without a second tracker or mock.

Three retained mutations discard an explicit participant, select the smaller
index, or admit a foreign incarnation. Each must fail the unchanged property;
the restored production source must pass.

Each conditional iterator consumption returns the original checked participant
and advances exactly once. No unchecked extraction remains in the merge.
Every loop step consumes an input; the merge waits on no producer and performs
constant work per participant, aside from comparing or cloning its scope bytes.
The complete result still costs linear work in the input participants. No
population-independent bound on tracking every authority is claimed.
