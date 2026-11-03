# Rustdoc span selection mutations

These are pre-existing Rustdoc schema inputs. Their original compiler capture
provenance is unavailable; they do not establish a real Rustdoc process run.
The required original corpus admission remains a separate check.

The fuzzer calls the actual Rustdoc byte decoder and relative-span selector.
The selector property and fuzzer share one constructive oracle, cloning a parsed
Item and varying only its span and crate ownership. Two distinct local relative
sources must survive, including repeated occurrences; foreign and absolute
sources and an absent span must not add paths. The expected set comes from the
constructed source identities, rather than a second copy of the selector.
Arbitrary decoded paths retain their inclusion check separately.

The kept relative-spans-foreign.patch and relative-spans-absolute.patch mutate
the production filters. Run the actual rustdoc_adapter fuzzer against
`duplicate-span-paths` with each patch separately; the unchanged oracle must
reject the added path. Restore the source before the positive corpus replay and
`cargo test -p sdk-docs --lib relative_spans::tests::source_selection_keeps_only_local_relative_paths -- --exact`.
These typed mutations are not new external wire captures. Fixed-input replay
is not an instrumented fuzz campaign or complete source branch coverage.

The production projection property reuses valid.json to vary public names,
documentation, crate versions and receipt admission flags. It checks the actual
exported identity directly, rather than comparing two renderings with the same
defect. Decoder parity and serialization round trips remain in rustdoc_adapter;
the projection property decodes and extracts only once. The retained
public-projection-path.patch truncates actual exported paths. Apply it and run
`cargo test -p sdk-docs --lib tests::rustdoc_adapter::public_projection_preserves_names_and_rejects_nonpublic_receipts -- --exact`
to check that the property rejects identity loss. This property reaches the
production extractor; the decoder fuzzer does not, so extraction fuzz coverage
remains an independent obligation.
