# Trust dependency waits do not retain their producer

The existing `account_events_preserve_exact_dependencies_without_unrelated_wakes`
oracle uses actual issuer records, the production follower and real dependency
events. Its final case retains an original account entry and registers a pending
account dependency. Dropping the follower must resolve that event immediately
with `SourceClosed`, even though the retained account's own sender remains live.

Run `node rust/proofs/qualify.mjs auth-producer-loss` from the repository root.
The kept production mutation captures the actual global watch sender inside its
own dependency future. The unchanged oracle must reject the resulting `Pending`
at `account wait retained its lost global producer`. This control detects the
owner cycle without awaiting a timeout or adding a replacement event. The shared
driver restores exact original production bytes before the positive run.

This establishes producer-loss progress at the follower's account event boundary.
Existing dependency assertions retain selective wakes, fencing and final close;
the separate lossy-follow and waiter schedules cover their own ordering claims.
This does not prove transport reconnection, every Auth cost, or every platform.
