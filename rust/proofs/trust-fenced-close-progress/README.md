# Follower closure ends a fenced feed

Run `node rust/proofs/qualify.mjs trust-fenced-close-progress`.

The existing public oracle persists malformed untrusted input through the actual
MemoryStream and opens its original TrustFeed without a bootstrap-key sender.
The refused environment remains fenced, with its original malformed-input
cause. Closing that original Follower must wake the parked run immediately,
return Unavailable and preserve Closed(Shutdown).

The retained production mutation leaves the original closed-state future
constructed but removes its poll from the actual selection against anchor
publication. With no anchor sender, that selection would otherwise wait
forever. The original waker assertion rejects this omission before any later
await. No timeout, replacement producer, mock Source or new test is added.

This is a distinct wait from anchor sender loss: the absent anchor has no
promised publication producer, while the original Follower supplies its close
event. Production costs do not change: zero new tasks, allocations, copies,
syscalls, round trips or barriers. The assertion covers this actual public
MemoryStream path, not remote provider completion or executor fairness.
