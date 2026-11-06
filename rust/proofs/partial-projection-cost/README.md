# Partial change projection

```sh
python3 rust/proofs/qualify.py partial-projection-cost
```

The existing production projection property retains the actual v1 protocol
golden and current `AppendRecord::changes` output. Its marker, malformed body,
forged index and filter matrix checks refusal, selected content, identity and
original-owner transfer. Partial projection charges two input parser invocations
and one output validation; these counters do not measure individual byte visits.

The same property grows only a discarded entry through the current writer.
The selected result and measured heap work must remain identical. The retained
mutation restores decode-all-then-retain in the actual projection, so discarded
payload growth must fail the unchanged allocation assertion. The qualifier then
restores identical source and runs that same property. It adds no parallel
decoder, scheduler or result model.

The existing `stream_persistence` fuzz target and captured changes corpus remain
the decoder oracle. This control establishes bounded-record selection and
allocation custody, not global scheduling, device durability or platform coverage.
