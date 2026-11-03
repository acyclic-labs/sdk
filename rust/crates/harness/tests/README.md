# Production mutation controls

The diff cancellation model calls the shipping `DiffFollow::next` and
`DiffView::apply`. It pins a real in-memory filesystem's file reference and
content before Shuttle explores one provider completion and one consumer,
including cancellation of a pending read. Its synthetic gate's dropped-wake
control tests that gate, not the production provider's publication mechanism.

`mutations/diff_pending_custody.patch` instead removes the shipping subscription's
ownership of a received publication before the awaited content read completes.
Apply it with `git apply`, then run the unchanged model:

```sh
cargo test --locked -p acyclic-harness --no-default-features --features filesystem,test-support --test main client_diff::model::follow_has_no_lost_wakeup_or_torn_publication_across_cancellation -- --exact --nocapture
```

The mutant must fail the original `publication` expectation in
`client_diff_model.rs`. Reverse the patch with `git apply -R` and run the same
command; restored production must pass. The model and its oracle stay unchanged.

This control covers custody across cancellation and atomic publication of one
file addition. It does not qualify provider wake generation, external wire
decoding, authorization changes, multiple files, a replacement's two endpoints,
physical restarts or non-owner topology. The separate replacement-endpoint
cancellation control remains necessary. No shipping operation or cost changes.
