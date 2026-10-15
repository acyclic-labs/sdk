# Client latency proof

Run `node scripts/proof-machines-client-latency.mjs` from the repository root
inside one `proof-` labelled bb1 job, immediately before opening the latency PR.
Use the lane's persistent target directory. Do not run this during development
or repeat it after a failed or inconclusive result.

The runner measures the baseline and candidate sequentially on the same box,
using identical fixtures and dependencies. It verifies that the only production
source difference is the bounded immediate watch reconnect, and restores the
candidate source even if either child fails. The ordinary scoped check and
changed regression tests are separate from this proof.
Other candidate code, including bounded child inspection, stays identical in
both revisions; the baseline wait implementation is checked against its pinned
base-commit hash before either run starts. This needs no history fetch in a
shallow remote checkout.

Each revision reports seven create and exec samples for a healthy watch and
an interrupted watch followed by a successful pending inspection. Create ends
when the SDK returns the inspected Machine. Exec ends after output `hi`, exit
code zero and EOF have been checked. Raw microsecond samples and medians are
printed as JSON; put the actual before/after numbers in the PR body.

This fixture uses a real loopback TCP gRPC client and server with bearer
authentication. Connection setup is outside the timed region. The server is
scripted: this measures SDK observation and transport overhead, not image boot,
physical guest execution, TLS setup or a staging service. The healthy case and
exec are controls for a change intended to remove one avoidable retry delay.
No performance improvement has been established until the paired job runs.
