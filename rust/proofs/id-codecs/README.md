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
| `version_admission_borrows_payload_and_refuses_before_decoding` | Arbitrary domain/version prefixes either return the exact borrowed payload or a typed refusal. Old and unknown headers refuse even without a body. |
| `bounded_resource_pairs_round_trip_and_refuse_other_versions` | Opaque four-byte accounts and IDs preserve every bit through the versioned resource codec; old versions and unversioned fields refuse before field parsing. |
| `bounded_actor_triples_round_trip_and_refuse_resource_encodings` | Admitted four-byte account, namespace and ID triples round-trip; Actor and Resource grammars are disjoint, and old/unknown Actor versions refuse before fields. |
| `usn_checkpoint_preserves_accepted_bytes_and_refuses_other_versions` | Every admitted 40-byte checkpoint has one encoding and a representable signed USN; truncations and old/unknown versions refuse before fields. |

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

The filesystem crate also keeps two bounded harnesses beside the production
native witness codec. From the repository root, run them individually:

```sh
cargo kani -p acyclic-fs --harness native_witness_header_preserves_all_fingerprint_bits
cargo kani -p acyclic-fs --harness native_witness_header_is_canonical_and_refuses_before_fields
```

These call the actual 67-byte header writer and decoder, covering every optional
fingerprint bit, accepted-byte uniqueness, noncanonical absent fields, truncated
current headers and unsupported versions without fields. They perform no IO,
allocation or waits. They are separate from the ID proofs: a fingerprint is not
an ObjectId. Their presence does not establish an executed proof.

Two more filesystem harnesses cover the ownership receipt's actual fixed-field
codec and admission path:

```sh
cargo kani -p acyclic-fs --harness native_ownership_fields_preserve_all_bits
cargo kani -p acyclic-fs --harness native_ownership_version_refuses_before_payload_and_hash
```

They preserve all 76 field bytes and require both original physical identities;
changing either identity refuses the binding. An unsupported version cannot
require a payload or reach hashing. These harnesses do not prove BLAKE3; the
separate production-codec fuzz oracle checks checksum corruption. These proof
runs remain required before claiming their results.
