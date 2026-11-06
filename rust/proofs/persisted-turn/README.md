# Persisted turn inputs

Run `python3 rust/proofs/qualify.py persisted-turn` from the repository root.

The existing native reopen control calls the original `AgentHost` with an actual
`LocalStream`. It commits a paused turn and its children, drops the host and
provider, then reopens the same store. An unrelated approval preserves that turn;
the matching approval resumes it and commits every recorded spawn.

The retained mutation discards the reconstructed turn in the production
checkpoint decoder. Qualification requires the unchanged oracle to reject the
lost checkpoint at runtime, restores the exact source bytes, then requires the
same native control to pass. Compilation failures and empty selections do not
qualify. The shared qualifier selects this integration target directly; it uses
the same mutation, restoration and diagnostic checks as its library targets.

Expected states retain durable values across reopening, without retaining the
original provider through a bound child index. The control uses a deterministic
turn driver. It does not qualify native executor or provider transport behavior,
every failure branch, another platform, or the complete cost of decoding.
