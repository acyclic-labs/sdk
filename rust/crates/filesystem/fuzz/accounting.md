# Accounting model progress control

The page accounting model runs the production blob-page writer against a
concurrent claimant using the production effect-credit primitive. It checks
zero, one and two admitted page writes, completed work, charged work and final
allocation retirement. Its completed-work witness is read after both actors
join and introduces no modeled synchronization.

`mutations/blob_page_reject_all.patch` changes the producer's original page
admission to refuse every finite allowance. A bound-only oracle could accept
this defect because no page is written. The unchanged progress assertion must
reject it: with allowance two, the independent credit claimant completes one
effect while the real writer refuses the other.

From a clean repository root, apply the patch and run:

```sh
RUSTC_WORKSPACE_WRAPPER=scripts/loom-rustc.sh cargo test --locked -p acyclic-fs \
  --features model-checking --lib -- --exact --nocapture \
  kernel::codec::accounting::model_checking::blob_page_writer_competes_with_original_effect_credit
```

Require the runtime assertion `available encoding work must complete`, with
completed work one versus admitted work two. Compilation failures, signals and
timeouts do not qualify the control. Reverse the patch before ordinary checks.
This component model does not establish complete concurrent-writer, native IO,
provider or restart behavior. Decoder grammar and instrumented fuzz coverage
remain separate obligations.
