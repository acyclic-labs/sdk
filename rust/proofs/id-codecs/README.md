Run these bounded proofs on demand with Kani installed:

```sh
cargo kani --manifest-path rust/proofs/id-codecs/Cargo.toml
```

The harnesses call production methods through the owning crates. They do not
copy a codec into a model. The obligations are:

| Harness | Safety and negative control |
| --- | --- |
| `every_object_kind_and_storage_association_round_trips` | Every bit of all eight kinds, digests and optional sealed associations survives encoding. Plain and sealed lengths differ. |
| `generation_preserves_the_complete_root` | A GenerationId retains the exact root, locator, file identity and origin generation. A raw 32-byte digest is rejected. |
| `arbitrary_bytes_have_one_canonical_identity_or_are_rejected` | Accepted encodings reproduce every input byte; invalid tags, widths and non-root generation kinds are rejected. Inputs cover all lengths through the maximum plus one. |
| `another_seal_is_another_identity_even_for_the_same_plaintext` | Changing a locator bit cannot retain the old complete identity, even when the plaintext digest is unchanged. |
| `fixed_width_digest_codecs_preserve_every_bit` | Filesystem and Objects digests and Streams CommitId preserve all 256 bits. |
| `uuid_codecs_preserve_every_bit` | AuthorityId, OperationId, VolumeId, CheckoutId, MountId, WatchId and FileId preserve all 128 bits. |
| `every_varint_prefix_and_eof_is_independent_of_chunking` | Symbolic inputs include every ten-byte varint prefix and partial EOF. Arbitrary chunking produces the same result, with strict input consumption at every step. |
| `overflowing_credit_grants_never_partially_change_the_window` | Both counters advance together; overflow in either counter leaves both unchanged. |
| `a_dropped_partial_frame_is_a_typed_eof_error` | Missing body bytes end in InvalidResponse; supplying the promised body ends successfully. |

Kani's unwinding assertions check the stated bounds; there are no waits or
timeouts. ID operations inspect a fixed maximum of 113 bytes, with no heap
allocation or population/history input. The framing proof bounds its input to
12 bytes to cover the complete prefix plus body/EOF boundaries; unbounded
payloads are exercised by the separate fuzz target. Frame decoding performs
only input-proportional work, and credit updates have constant work.

These harnesses establish properties only when their Kani run succeeds. They
do not establish that unrelated consumers use the codecs: that is a separate
whole-source boundary audit. In particular, a correct GenerationId codec alone
cannot detect a caller that reconstructs a generation from a digest.
