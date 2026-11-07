# Cached physical descriptor classification

The existing physical descriptor cache handle retains the same `HeldDevice`
used by `HeldFile`. Its lifetime is the physical file's lifetime, including
original closing flushes. A reopened handle starts with fresh evidence. Linux
and Windows use a zero-sized state; no allocation or descriptor clone is added.
All cached full flush entry points share the existing native implementation.

Census: `rg -n 'sync_file_completed|sync_data_completed|flush_file_data|impl Files|Handle \{' rust/crates/native-runtime/src -g '*.rs'`.
The real Apple property
`file::device_tests::owned_flush_reuses_device_only_for_the_retained_file`
now also uses public `CachedFile` imports and lease callbacks. It checks all
three callback flush methods, original closing flush and a fresh physical owner
of the same inode, plus ordering and real `/dev/null` refusal. There is no
new test-only constructor, provider mock, timer or polling loop.

`mutation.patch` restores borrowed classification at the cached native targets.
The original `HeldFile` behavior remains intact. On Apple, the existing property
must fail at `cached flush reclassified its original physical descriptor`.

```sh
git apply rust/proofs/cached-device-classification/mutation.patch
cargo test --locked -p acyclic-native-runtime --lib file::device_tests::owned_flush_reuses_device_only_for_the_retained_file -- --exact
# Restore the two files from the tested commit and run the same control.
```

This negative requires execution on an actual Apple filesystem. Linux cannot
qualify it. The replacement control closes one public cached owner and imports
the same inode again; it does not yet cover eviction/reopen on a surviving
cached entry. Native fallback/full/error counts remain in the existing public
lease custody property. Every native attempt, funding boundary and original
producer wait is unchanged; no extra durability operation is introduced.

For repeated full flushes of one retained local descriptor, classification
changes from two native metadata calls and one device-map lookup per request to
one initial classification, then zero. Read-only handles and successful ordering
barriers classify zero times. State access is constant per physical descriptor;
the existing device-map lookup is not proved worst-case constant time. Earlier
barrier coverage by later device drains, every concurrency schedule, multiple
devices and physical Windows execution remain separate obligations.
