# Native cleanup task custody

`cancelled_command_retires_before_next_entrant_and_preserves_acknowledged_head`
uses the actual native projection, worker, acknowledged volume head and next
entrant. Its seeded scope uses the existing native admission's real clock
origin and must retain the cleanup task after the original waiter disappears.
After its release channel, the same physical worker drops its claims and
announces that retirement was submitted. The next entrant waits on the same
execution guard while drain drives the accepted cleanup concurrently. Waiting
for the entrant before driving cleanup would deadlock. Every scheduler boundary
is released by that test's driver.

Run `node rust/proofs/qualify.mjs controlled-cleanup` from the repository root.
The retained `unscoped-claims.patch` restores direct runtime submission in the
production owner. The unchanged assertion must reject that scope loss. The
runner restores the exact source before checking the native output and three
authenticated owner-publication properties alongside the cancellation case.

Both projection claims and accepted output retain the existing `TaskSpawner`.
Output's existing flush task uses its `TaskHandle`. This adds no task, thread,
IO request, durability barrier or wait: one captured submission context replaces
one runtime handle per owner. Capturing and submitting remain constant work per
owner, independent of agent count and history. Cleanup still retires precisely
the claims or accepted bytes it already owns.

The cancellation mutation qualifies projection task custody. The native output
case covers actual successful output staging; it does not establish exhaustive
output-cancellation, provider or platform coverage.
