# Retained merge-directory generation and page credit

Run `node rust/proofs/qualify.mjs merge-directory-custody` from the repository root.

The qualifier reuses the existing public merge-directory test with default
product features. It creates real
memory-backed workspaces, diverges an original parent and child, stages the
original retained preview and resolves proposals from pinned generations.

One production mutation removes both generation ownership guards. An otherwise
valid proposal in a foreign staging generation must still be refused as
`StaleCandidate`. The unchanged original test rejects that mutation.

A second production mutation consumes an extra clean row for each page. The
original test requests one row, checks the actual producer counts after every
page and rejects consumption beyond that credit. This checks bounded selected
work; it does not establish constant total cost across arbitrary populations.

The same test retains original cancellation after EOF, staging-credit refusal,
failed-preview retirement and publication preconditions. No new test or semantic
model is introduced. No production behavior, allocation, copy, syscall, round
trip, durability barrier or wait changes. The qualifier runs each mutation
against the unchanged test, restores the exact source between controls and then
requires the original test to pass.

This control uses public library entry points with the memory backend. It does
not establish durable crash recovery, remote failure behavior or a platform
matrix. The durable public merge capture is retained separately.
