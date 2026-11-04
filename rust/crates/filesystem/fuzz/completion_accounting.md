# Completion accounting source controls

The existing `encoder_work` and `decoder_work` targets retain their original
generated receipt, budget, cancellation and truncation inputs. Their independent
completed-work witnesses observe the real writer and decoder callbacks. Every
callback reaches the production accounting observer; there is no alternate
completion-suppressing observer.

`mutations/encoder_body_completion.patch` removes the actual production
`EncodeWorkStep::Completed` charge. The unchanged
`generated_canonical_inputs_preserve_exact_budget_prefixes_and_refunds` property
must fail at `exact independent encoding receipt`.

`mutations/decoder_copy_completion.patch` changes the actual production
`ReadAllocationStep::Copied` charge to zero. The unchanged
`canonical_round_trips_keep_exact_completed_work_and_denied_effects` property must fail
at `missing copy completion callback`. Canonical value, binding and extent-shape
relations remain in the same decoder oracle.

Apply each patch separately and run its original generated property. A compiler
error or another failure does not qualify the negative control. Reverse the
patch, verify the source digest, and run the same property on restored source.
The retained fuzz targets and corpus remain the entry points for coverage work;
these controls alone do not establish full decoder coverage or platform results.
