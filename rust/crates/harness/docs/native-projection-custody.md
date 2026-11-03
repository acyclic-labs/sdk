The file bridge uses one no-follow presence classifier for reply delivery,
request retirement and filesystem replay retention. Only `NotFound` denotes
absence. Another native error ends observation before changing the tracked
request, and the filesystem retention predicate keeps its saved result.
Presence is a visibility fact; neither absence nor this control proves durable
acknowledgement or permits deleting a persistence barrier.

The existing configured-server conformance entry also checks queued, running
and answered requests against a real directory-link loop. It passes an actual
completed server result to the existing reply encoder, keeps the original
request directory and bytes, calls the production observer and replay predicate,
restores the name, then performs real withdrawal and retirement. The native
query must fail with a cause other than `NotFound`; otherwise the fixture has
not exercised the error cut and fails its prerequisite. No invented metadata
error, fake provider response or alternate request state machine is involved.

Run from the repository root:

```sh
cargo test --locked -p acyclic-harness --no-default-features --features filesystem-local,native-execution --lib local::host_files::tests::workspace_projection_uses_shared_results_without_client_state -- --exact
```

The retained patches restore the two original production statements, one at a
time, without changing the control:

| Patch under `docs/mutations` | Required failure |
| --- | --- |
| `replay-error-as-acknowledgement.patch` | `observation failure acknowledged a replay result` |
| `request-error-as-withdrawal.patch` | `observation must retain its original native failure` |

Apply one patch with `git apply`, run the same command, then restore it with
`git apply --reverse` before another run. A compiler failure, fixture refusal,
unrelated assertion or missing producer does not qualify either negative.

Each presence check still makes one standard-library metadata query, and the
request predicate retains its existing name formatting, path construction and
one shared closure allocation. No additional query, directory listing, worker,
wait, copy or durability barrier is introduced. Native syscall counts remain
platform dependent. Lookup work depends on the original path, never request
history or agent count; the compatibility result sweep is still proportional
to its outstanding results. The negative controls cover false acknowledgement
and retirement; restored-name and real-withdrawal assertions cover progress.
Physical completion reporting, power loss, restart and the unlink-before-barrier
acknowledgement cut require their own original custody evidence.
