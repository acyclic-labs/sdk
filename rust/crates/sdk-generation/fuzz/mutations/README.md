# Cache-record authentication controls

Apply each patch separately from the repository root. Run the existing property
`file_cache::tests::shared_record_admission_authenticates_original_bytes` with
`cargo test -p sdk-generation --lib <property> -- --exact --nocapture`, then revert
the patch. Each patched decoder must fail with
`accepted an unauthenticated cache record`; the restored decoder must pass.

The key mutation admits another valid caller identity while verifying the stored
identity's checksum. The former property appended a character to the caller key,
so malformed-hash rejection hid this defect. The checksum mutation removes the
production authentication comparison. Both use the same original-byte oracle as
the `identity_record` fuzz target, with an independent SHA-256 implementation.

The property and target invoke the production writer for valid inputs. Raw fuzz
inputs additionally exercise the persisted decoder without rewriting its input.
The older on-disk corpus has no recorded capture provenance; it does not establish
execution of Cargo, Rustdoc or the filesystem cache caller. These controls do not
establish complete source branch coverage or a continuous fuzz campaign.
