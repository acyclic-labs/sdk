# Original follower generation exhaustion

Run `python3 rust/proofs/qualify.py auth-generation` from the repository root.
The qualifier mutates the production `Follower::withdraw` increment to reuse
generation zero after exhaustion, requires the existing oracle to fail, restores
the exact original source and requires that same oracle to pass.

`swap_precedes_wake_and_lost_follow_fences` applies real signed issuer records,
verifies signed credentials and follows two accounts. It checks publication
before wake, refused stale reads and delayed acknowledgements. Its existing
full-width boundary sets the original generation to `u64::MAX`, registers the
original `changed()` future and calls the shipped `connect` path. That wait must
immediately return `SourceClosed` with `GenerationExhausted`; later `resume` and
`serve` calls cannot reopen trust. The mutation must fail this semantic oracle,
not compilation or a timeout. No additional test or parallel state model is used.

The same test holds the actual account-map write lock during withdrawal. This
challenges a traversal of the account population on the shared path used by
`fence` and `connect`. Generation bookkeeping has fixed work, but the original
watch notification still costs work proportional to actual waiters. This check
does not establish complete credential, revocation-fanout or backend cost.
