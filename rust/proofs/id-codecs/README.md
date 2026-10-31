Run these bounded proofs on demand with Kani installed:

```sh
cargo kani --manifest-path rust/proofs/id-codecs/Cargo.toml --harness generation_preserves_the_complete_root
```

The harnesses call production methods through the owning crates. They do not
copy a codec into a model. The obligations are:

| Harness | Safety and negative control |
| --- | --- |
| `scalar_metadata_preserves_all_facts_and_refuses_missing_opaque_bodies` | Every optional u32/u64/i64 scalar survives the public wire writer/parser, including zero and signed extrema; each opaque-presence flag refuses without its separate body. |
| `impossible_declared_frames_refuse_before_body_io` | Every impossible declared extent or body capacity returns the original `InvalidData` after only the fixed prefix read. A body-read tripwire would return `BrokenPipe` and increment its counter if admission moved after body IO. No payload allocation or checksum is needed on this refusal path. |
| `every_object_kind_and_storage_association_round_trips` | Every bit of all eight kinds, digests and optional sealed associations survives encoding. Plain and sealed lengths differ. |
| `every_object_prefix_preserves_the_following_field` | Every kind and storage association decodes through the production prefix parser, preserving a borrowed suffix through eight bytes; each truncated identity refuses, and the strict codec refuses a nonempty suffix. |
| `arbitrary_object_prefixes_have_one_exact_identity_and_borrowed_tail` | Every admitted input through 121 bytes splits into its exact canonical identity and untouched borrowed tail. Prefix refusal implies strict identity and generation refusal; strict generation admission requires a complete root without a suffix. |
| `generation_preserves_the_complete_root` | A GenerationId retains the exact root, locator, file identity and origin generation. A raw 32-byte digest is rejected. |
| `arbitrary_bytes_have_one_canonical_identity_or_are_rejected` | Accepted encodings reproduce every input byte; invalid tags, widths and non-root generation kinds are rejected. Inputs cover all lengths through the maximum plus one. |
| `another_seal_is_another_identity_even_for_the_same_plaintext` | Changing any chosen bit of the locator, root-file identity or origin generation cannot retain the old complete identity, even when the plaintext digest is unchanged. |
| `fixed_width_digest_codecs_preserve_every_bit` | Filesystem and Objects digests and Streams CommitId preserve all 256 bits. |
| `uuid_codecs_preserve_every_bit` | AuthorityId, OperationId, VolumeId, CheckoutId, MountId, WatchId and FileId preserve all 128 bits. |
| `every_varint_prefix_and_eof_is_independent_of_chunking` | Symbolic inputs include every ten-byte varint prefix and partial EOF. Arbitrary chunking produces the same result, with strict input consumption at every step. |
| `overflowing_credit_grants_never_partially_change_the_window` | Both counters advance together; overflow in either counter leaves both unchanged. |
| `a_dropped_partial_frame_is_a_typed_eof_error` | Missing body bytes end in InvalidResponse; supplying the promised body ends successfully. |
| `grpc_header_admission_and_eof_are_independent_of_chunking` | Every five-byte header and split has the same result: compressed/unknown flags refuse distinctly, oversized frames refuse, and a promised missing body ends in a typed EOF error. Only an uncompressed empty frame succeeds without a body. Every successful parser step consumes input. |
| `credit_debits_preserve_both_balances_or_refuse_without_mutation` | Data on a nonzero channel spends exactly its actual encoder width and one frame; insufficient credit or a zero channel changes neither balance. Control frames spend no data credit. |
| `version_admission_borrows_payload_and_refuses_before_decoding` | Arbitrary domain/version prefixes either return the exact borrowed payload or a typed refusal. The shared primitive and Stream projection agree; exactly one appropriate refusal constructor runs on failure and neither runs on success. Old and unknown headers refuse even without a body. |
| `bounded_resource_pairs_round_trip_and_refuse_other_versions` | Opaque four-byte accounts and IDs preserve every bit through the versioned resource codec; old versions and unversioned fields refuse before field parsing. |
| `bounded_actor_triples_round_trip_and_refuse_resource_encodings` | Admitted four-byte account, namespace and ID triples round-trip; Actor and Resource grammars are disjoint, and old/unknown Actor versions refuse before fields. |
| `usn_checkpoint_preserves_accepted_bytes_and_refuses_other_versions` | Every admitted 40-byte checkpoint has one encoding and a representable signed USN; truncations and old/unknown versions refuse before fields. |
| `log_prefixes_preserve_lengths_and_refuse_unrepresentable_or_outside_bounds` | Both four/eight-byte log prefixes preserve every length bit under arbitrary configured bounds, including reversed bounds. Narrow overflows, out-of-domain lengths and truncated prefixes refuse; every admitted prefix has one canonical spelling. |
| `full_frame_extents_refuse_overflow_without_clamping` | Arbitrary body lengths and both prefix widths either retain the exact mathematical frame size or refuse integer/prefix/platform overflow. Maximum queries use the same checked operation. |
| `bounded_resource_components_are_lossless_and_have_one_spelling` | Every opaque byte sequence through four bytes round-trips; every admitted spelling through twelve bytes re-encodes identically. An incomplete appended escape refuses. |

Kani's unwinding assertions check the stated bounds; there are no waits or
timeouts. ID operations inspect a fixed maximum of 113 bytes, with no heap
allocation or population/history input. The framing proof bounds its input to
12 bytes to cover the complete prefix plus body/EOF boundaries; unbounded
payloads are exercised by the separate fuzz target. Frame decoding performs
only input-proportional work, and credit updates have constant work.
The gRPC proof bounds its input to the complete five-byte transport header;
full message bodies remain in the chunked fuzz oracle. The debit proof obtains
its charge from the real encoder, rather than duplicating protobuf size rules.
These added harnesses and the expanded seal negative control are unexecuted.
The prefix models use the merged production `ObjectId::from_prefix` implementation
and bound input to the maximum 113-byte identity plus eight suffix bytes. Their
source is present; both Kani runs remain unexecuted. The decoder examines only
the fixed identity width, with no scan of the following field, allocation or wait.
The log-prefix harness covers exactly the bounded prefix codec. The separate
extent harness covers fixed scalar arithmetic, including platform endpoint
width; neither proves body allocation or SHA-256. Both remain unexecuted.
The impossible-extent harness calls the actual frame reader with arbitrary
complete prefix bytes and a body-IO tripwire. It proves the fixed-prefix typed
refusal path, with no successful body allocation or hash obligation. This
harness also remains unexecuted; it qualifies only the selected target width
when run.
The resource-component model bounds decoded bytes to four and input syntax to
twelve bytes. The unbounded production grammar performs input-proportional
work; arbitrary lengths remain in the Auth fuzz target.

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

The multipart record header has one in-crate bounded harness:

```sh
cargo kani -p acyclic-fs --no-default-features --features s3-http --harness multipart_header_refuses_versions_before_record_fields
```

It calls the production cursor on every zero-, one- and two-byte input. Missing,
old and unknown versions refuse without consuming a record field; a current
truncated header is malformed, and a full current header preserves the tag.
The synchronous header performs fixed scalar work with no allocation, IO or
wait. This harness is unexecuted and does not prove multipart replay or storage.
