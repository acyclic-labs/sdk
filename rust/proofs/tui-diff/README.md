# Admitted diff presentation controls

Run on the qualified execution target, against the named source SHA. No control
is product code or a feature switch. Apply each patch only in that job's source
copy, run its exact existing regression, require an assertion failure in that
regression, and reverse the patch before the next check.

- `whole-line.patch` replaces word emphasis with whole-line emphasis. It must
  fail `diff::tests::diff_word_emphasis_preserves_unicode_context_across_themes`.
- `truncated-reflow.patch` discards continuation rows from both real wrapped
  panes. It must fail
  `diff::tests::diff_wide_reflow_and_hunk_folding_retain_every_source_grapheme`.

First run both unmodified regressions with
`cargo nextest run --locked -p acyclic-harness-tui --lib -E 'test(=diff::tests::diff_word_emphasis_preserves_unicode_context_across_themes) | test(=diff::tests::diff_wide_reflow_and_hunk_folding_retain_every_source_grapheme)'`.
Require their normal success, then the named failures under the corresponding
controls. Compiler or setup failures are not accepted as negative proof.

The same tests check literal untrusted filenames, narrow/wide geometry, light
and dark themes, monochrome fallback, hunk selection and fold/reveal without
losing Unicode text. Source paging and cursor ownership stay with the existing
query implementation; rendering never fetches another page.

`dropped-context.patch` reproduces the original consumer bug: the real diff
query's returned presentation is overwritten with default metadata. It must
fail `runner::view::query::tests::query_controller_retains_reads_coalesces_turns_and_ends_on_original_events`
in the `acyclic-plugin` library. That regression now opens the real local
harness and consumes an actual diff query before exercising the retained
cursor controller. Require its assertion about lost admitted presentation,
not a compiler, setup or producer failure.
