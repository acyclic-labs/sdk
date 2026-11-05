# Public live-update documents

The 34 `actor_codecs/live-update-*` inputs were captured on Linux x86_64 from
commit `4ccd70623e03f7b1b1d0c4f2790862b284846eb8` with:

```sh
cargo run --locked -p acyclic-codec-fuzz --example capture -- --only-live-update --extend-goldens
```

`live_update::seeds` calls the public `acyclic_actors::live_update::encode`
writer for all 23 document types, including nested variants and decimal
boundaries. Schema inputs contain the original Actors `FILE_DESCRIPTOR_SET`;
the actor pin comes from `ActorReference::encode`. The future-version and
trailing-whitespace inputs are derived from the original Selection writer and
are refusal inputs, not additional producer captures. `corpus/SOURCES.tsv`
records each input's original size, SHA256 and producer; the matching writer
outputs remain in `compatibility/frames/golden.hex`.

These are public codec inputs. They do not qualify deployed membership,
handoff, rollback, recovery or complete source branch coverage.
