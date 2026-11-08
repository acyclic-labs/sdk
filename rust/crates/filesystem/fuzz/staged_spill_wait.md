# Original spill lease cancellation

The existing `staged_objects::tests::native_read::native_spill_denial_corruption_and_abandoned_frame_preserve_original_prefix`
property opens the shipped logical store over `LocalObjects::open`, stages a
real spill, and holds its original exclusive lease. Scalar and mixed reads
must wake and return the caller's or original native cancellation while that
lease remains held. Completed durable members keep their original buffers and
result vector funded until the pending mixed read retires. A second controlled
poll at the same blocked event must change no work counter.

The existing `staged_objects::tests::native_write::native_flush_caller_cancellation_refuses_before_admission`
property opens the same local provider and checks both publication and window
relief with their original lease and collection producers held. Cancellation
wakes each writer and returns its original typed error without publishing,
writing, or releasing private objects.

Publication also acquires the spill lease before blocking on the actual
collection sweep gate. The original gate notification confirms this branch;
ordinary caller, native caller and native original cancellation must finish
while the sweep guard stays held. Collection admission, direct durable writes,
spill draining and generation pins share this funded primitive wait.

Both properties exercise ordinary accounting and distinct caller/native
cancellation. The original alias and batch-read properties retain their
physical ownership and I/O checks. The ready branch of the shared wait returns
before constructing either cancellation waiter.

Run the two controls with:

```sh
cargo nextest run --locked -p acyclic-fs --no-default-features --features local,memory --lib -E 'test(=staged_objects::tests::native_read::native_spill_denial_corruption_and_abandoned_frame_preserve_original_prefix) | test(=staged_objects::tests::native_write::native_flush_caller_cancellation_refuses_before_admission)' --no-tests=fail
```

Apply one kept mutation with `git apply`, run the read property, then restore
that patch with `git apply -R` before requiring the unchanged property to pass:

- `mutations/staged_spill_cancellation.patch` removes cancellation from the
  actual spill lease wait. The original cancellation must fail to wake the read.
- `mutations/staged_spill_waiter_funding.patch` bypasses admission for the
  actual caller waiter. Its original allocation count must fail.
- `mutations/staged_spill_waiter_growth.patch` replaces the original waiter on
  every pending poll. The unchanged-counter assertion must fail.
- `mutations/staged_collection_cancellation.patch` removes cancellation from
  the actual collection gate wait. Run the writer property; cancellation must
  fail to wake it while the original sweep guard stays held.

The shared `KernelAccounting::wait_for` uses the existing cancellation nodes.
A ready event needs zero waiter allocations. A blocked event retains one
original node per distinct cancellation source, at most two. Waiter nodes link
and unlink directly, independently of the number of other waiters. Each wait
has the original lease owner and cancellation token as its producers.
Cancellation drops only the queued lease acquisition; physical I/O already
submitted keeps its original completion and retirement path. No task, timer
or polling loop is added.

The unchanged-counter control establishes bounded waiter registration, not a
bound on the existing cancellation token's ancestry traversal. The underlying
`CancellationToken::is_cancelled` still checks its parent chain; this property
does not establish constant cost in that separate dimension.
Collection admission also retains its existing generation retry; this property
does not establish constant work across concurrent generation replacement.
