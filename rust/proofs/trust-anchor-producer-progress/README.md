# Trust anchor producer lifetime

Run `node rust/proofs/qualify.mjs trust-anchor-producer-progress`.

The existing integration oracle drives public TrustFeed over the shipped
MemoryStream adapter. It generates and signs real environment and account
records with the original Issuer, persists them through public Stream append,
recovers from a placeholder anchor through the actual watch publication, and
checks that a live account still follows through the same run.

Dropping the original bootstrap-key watch sender must wake the already parked
driver. One immediate poll must return Unavailable and retain AnchorLost as
the original public fence cause. The two production mutations independently
discard sender loss into a pending future and replace the original cause.
The same oracle must reject each before passing with restored source. No new
test, mock Source, timeout or readiness loop is added.

Production work is unchanged: zero added syscall, barrier, round trip,
allocation, copy, task or wait. The original watch is the event and the sender
being dropped is its producer. Publication fanout is proportional to actual
subscribers; this does not claim constant total fanout or completion from a
live sender that never publishes. Remote transport, replicated storage,
arbitrary executor starvation and ordinary live Stream noncompletion remain
separate obligations.
