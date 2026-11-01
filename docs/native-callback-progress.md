# Native callback completion

The native bindings use the pinned N-API implementation in
`rust/vendor/napi-3.15.0`. `ORIGIN.json` binds the registry archive and each
imported file to its original bytes. The only runtime correction reclaims a
queued callback item before returning when Node supplies no live environment
or callback. The shared dependency retains its registry identity, and the root
`[patch.crates-io]` selects the corrected source locally. Publication checks
therefore keep treating NAPI as an external dependency. Generated bindings do
not implement a second callback owner.

The source override is necessary because the affected `call_js_cb` function is
private to NAPI. Cargo's dependency overrides replace packages, not individual
source files, so a smaller `[patch]` declaration would still require a complete
crate source. A wrapper cannot reclaim the item after Node disposes the queue.
As checked on 2026-10-31, the published
[`napi` 3.16.1 archive](https://static.crates.io/crates/napi/napi-3.16.1.crate)
and upstream main still return on a missing environment before reclaiming the
item. Updating to that release would retain the missing-completion defect.
Only `src/threadsafe_function.rs` differs in runtime behavior from the pinned
3.15.0 source; the manifest additionally disables publication. All other
imported files retain their original bytes and formatting. Once upstream
reclaims this data, the registry dependency can replace this source after the
same shutdown and original-source negative controls qualify it.

Safety requires exactly one destruction of each admitted queue item, using
the original error-first or direct allocation layout. The shutdown branch
does not dereference the finalized callback context or call Node APIs.
Progress waits on the original oneshot completion: a JavaScript result sends
it, and disposal of an unexecuted item drops its sender. The existing receiver
then returns `GenericFailure`. A missing sender is an error, never success.

Required work per item remains constant: the original queue admission, one
item allocation and one completion-closure allocation. The correction adds
one null-data check and moves the existing item reclamation before the
shutdown return. It adds zero allocations, copies, syscalls, durability
barriers or transport requests. Accepted shutdown previously retained one
item and its completion closure indefinitely; it now destroys both. The
ordinary callback path retains its original allocation and request counts.
These are source counts, not latency measurements.

`acyclic-napi-progress-test` is an on-demand native addon. Build it with
`cargo build --locked -p acyclic-napi-progress-test`, copy the resulting
platform library to a `.node` filename, then run
`node scripts/qualify-napi-progress.mjs /absolute/path/to/fixture.node`.
Both callback layouts use actual Node Workers and actual queue admission.
The native task owns a socket that reports its typed completion; EOF without
that result fails. Ordinary Worker shutdown can attempt callback delivery and
return `PendingException`. Aborting an admitted queue before shutdown exercises
disposal without JavaScript and must return `GenericFailure`. A single escape
deadline fails an absent completion;
it does not retry or complete the operation. Rebuilding the same addon with
the original early return is the progress negative control. Native execution
and that omission control must be recorded separately from source review.
On Linux, `python scripts/qualify-napi-disposal.py` builds and runs both layouts
through both shutdown paths, then rebuilds the exact original dependency and
requires the unchanged queue-disposal assertion to fail.
