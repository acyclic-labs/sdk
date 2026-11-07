# Immutable profile admission controls

Apply `keep-original-revision.patch`, then run:

```sh
cargo nextest run --locked -p acyclic-harness-providers --lib -E 'test(projection::profile::tests::parsed_profiles_bind_original_selection_capacity_and_protocol_controls)'
```

The mutation returns an admitted binding under the old model identity. The
retained protocol control must fail at `profile admission retained the original
revision`. It also checks that a foreign selection and a second admission are
refused and that the original selection remains unchanged.

Undo that patch, apply `grow-revision-buffer.patch`, then run:

```sh
cargo nextest run --locked -p acyclic-harness-providers --lib -E 'test(projection::profile::tests::gemini_levels_follow_the_exact_retained_catalog_member)'
```

This mutation adds buffer growth to admission. The retained catalog control must
fail at `profile admission copied more than two model identities and one provider
owner`. Successful admission requires six owned model strings and one provider
allocation; the transport, credentials, catalog and accounting are shared. No
request, wait, schema traversal or history copy occurs during admission.

The controls use the embedded production catalog, generated profile data, the
public provider trait and the original protocol projection. They consume no
provider replies. Undo the patch before retaining a candidate.
