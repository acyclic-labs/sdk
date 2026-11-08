# Local failure ends original observations

Run `node rust/proofs/qualify.mjs local-failure-progress`.

The existing public Local oracle opens the real journal with default options
and makes the OS refuse a write through its original read-only descriptor.
Thirteen already parked operations must receive their original wake and
immediately refuse while both publication fences remain held. The same oracle
checks ordinary and native failures and preserves the physical cause, worker
custody and funding release.

Three mutations independently remove failure observation while acquiring the
publication fence, failure observation while opening an accepted snapshot, or
the original applied-revision closure. The last mutation leaves both ordinary
failure watches intact: future-cut waits must still reject the lost authority.
`ProviderExt::wait_applied` uses the public `heads` request, and Local heads,
read, follow and native read/heads join the same applied-revision authority
before acquiring publication access. The oracle checks each registered wake
and polls each original future once before any further await; a missing close
therefore fails at the revision assertion instead of hanging the test.

No additional test, mock, replacement producer, timer or readiness loop is
introduced. Production code and per-operation costs are unchanged. These
controls prove terminal failure propagation through the named real operations;
they do not promise completion of still-live I/O or eventual publication by
a caller who retains an open Memory provider.
