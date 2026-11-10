# Original approval request

Dispatch and action verification retain the exact private-volume request read
for that operation through policy resolution. Resolution receives the borrowed
request, rather than reopening it by identity. Review and decision files remain
durable evidence; verification cannot start a review or execute an effect.

Run the real filesystem-backed control:

```sh
cargo test --locked --offline -p acyclic-harness-core --features filesystem,test-support --lib filesystem::memory::tests::approvals::approval_dispatch_and_verification_retain_one_original_request -- --exact
```

The control forwards original request/review files, uses the production policy
and effect owner, checks operation and action-digest substitution, reopens the
same volume with another owner, and checks both release and loss of the request
producer without a timer. Resolution must perform one request read, independent
of the number of retained files. The existing policy traversal is unchanged.

Apply each `mutations/approval_*.patch` independently and reverse it before the
next run. Dispatch/verification reread mutations must fail their respective
one-read counters; ignoring the action binding must fail the authority check;
losing the request producer must fail the progress check on the same dispatch.

For dispatch with a turn owner and for successful verification, immutable
request reads and byte buffers fall from two to one. Rejected bindings still
use one read. No syscall, durability barrier or round trip is added; each
removed read removes the work its selected storage adapter requires. The fixed
request-selection cost is O(1); decoding and hashing still cost O(request bytes),
and the separate policy chain still depends on the configured approvers.
