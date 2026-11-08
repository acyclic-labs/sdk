# Original cancellation backing

The existing cancellation properties compare the installed allocator's live
bytes and completed allocation count with the original accounting delta:

- `cancellation::tests::measured_waiter_credit_follows_the_last_node_alias_after_detach`
  exercises ordinary measured registration, cancellation, detachment and the
  last retained node alias.
- `cancellation::tests::native_waiter_replacement_is_reentrant_and_releases_its_original_owner`
  exercises funded token construction and native registration before replacing
  a real reentrant waker.
- `cancellation::tests::linked_children_preserve_local_stop_parent_propagation_and_release_their_owner`
  exercises the actual linked child, parent waiter and propagation waker.
- `cancellation::native_tests::cancellation_notification_ends_with_its_registration`
  exercises the OS notification registration, including already cancelled tokens
  and both funded and unmetered callers.

Run these controls with:

```sh
cargo nextest run --locked -p acyclic-fs --no-default-features --features local,memory --lib -E 'test(=cancellation::tests::measured_waiter_credit_follows_the_last_node_alias_after_detach) | test(=cancellation::tests::native_waiter_replacement_is_reentrant_and_releases_its_original_owner) | test(=cancellation::tests::linked_children_preserve_local_stop_parent_propagation_and_release_their_owner) | test(=cancellation::native_tests::cancellation_notification_ends_with_its_registration)' --no-tests=fail
```

The oracle observes real production allocations. It contains no parallel Arc
layout formula. The shared `arc_layout::allocation_bytes` accounts for the
header and alignment padding before each funded allocation. Allocation counts,
copies, wait events and cancellation delivery remain the same.

Apply `mutations/cancellation_arc_backing.patch` with `git apply` to restore the
five payload-only admissions in the actual production constructors. The same
properties must reject the underfunded backing. Restore with `git apply -R`
before requiring the unchanged properties to pass.

The properties retain their original lifetime and progress assertions. This
backing check adds constant-size layout computation and no syscall, durability
barrier, round trip, allocation or copy. It does not prove constant cost for the
separate cancellation ancestry traversal or qualify every operating system.
