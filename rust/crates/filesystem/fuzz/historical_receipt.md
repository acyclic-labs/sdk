# Historical authority receipt controls

`distributed::accounting::tests::historical_reads_preserve_the_complete_original_receipt`
compares the historical adapter with the same real snapshot and replay helpers,
using ordinary and native original funding. It checks every receipt field,
allocation/hash/copy refusal before the corresponding work, retired history,
cancellation before dispatch and cancellation after a real LocalStream head
has been admitted behind its publication writer. No alternate provider or
decoder implements the expected result.

The retained mutations exercise the same property:

- `mutations/historical_replay_projection.patch` restores the actual replay
  implementation that copied only three receipt fields from a replacement
  unbounded work frame. The complete replay receipt assertion must fail.
- `mutations/historical_codec_receipt.patch` injects seven `bytes_encoded`
  units through the actual decoder's original `AuthorityWork::admit` and
  `charge`. With the adapter repair, the printed outer
  `HISTORICAL_REPLAY_RECEIPT` must show exactly seven additional encoded units
  above the restored-source receipt for both ordinary and native funding.
  Replay also encodes the durable digest, so its total encoding delta includes
  that real work. This is an accounting fault injection, not an additional
  encoded format or a claim that decoding normally encodes bytes.

Apply both mutations for the negative control, then reverse only the replay
projection for the positive control. Run the same exact property each time:

```sh
cargo test --locked -p acyclic-fs --no-default-features --features local,memory,distributed --lib distributed::accounting::tests::historical_reads_preserve_the_complete_original_receipt -- --exact --nocapture
```

The negative must fail at `historical replay lost original completed work`;
a compiler error or another failure does not qualify it. Reverse the codec
mutation after the observation, verify the restored source and run the same
property to obtain the baseline receipt. These controls
do not establish producer-loss completion, remote restart behavior or full
platform validation.
