# Retained execution record work

The native growth property uses the real filesystem journal, its wire decoder,
and the driver's replay fallback. Between completion of a nonempty replay page
and the next replay request, the driver only indexes those records. Original
copy counters must stay unchanged in that interval. Decoder and output copies
remain outside it. Both standalone and native owners cover all 16 nonempty
pages, including every capacity crossing in the original 1,001-record storage.

```sh
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib executor::tests::turn_cost::native_growth::a_thousand_records_retain_live_metadata_without_decoder_scratch -- --exact
git apply rust/crates/harness-core/src/executor/tests/turn_cost/record_relocating.patch
cargo test --locked -p acyclic-harness-core --features filesystem,test-support --lib executor::tests::turn_cost::native_growth::a_thousand_records_retain_live_metadata_without_decoder_scratch -- --exact
git apply -R rust/crates/harness-core/src/executor/tests/turn_cost/record_relocating.patch
```

The patch restores the former production radix and both value vectors. Require
a runtime failure at `record insertion copied prior backing`, then require the
same property to pass after exact restoration. Compilation failure is not
detection. Selector tests additionally compare every event and wire order with
cold replay, including a Started record appearing after step records.
