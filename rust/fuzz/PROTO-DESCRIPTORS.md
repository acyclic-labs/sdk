# Descriptor canonicalization validation

`proto_descriptors` calls the public `acyclic_proto_build::canonicalize`. Every invocation selects one retained compiler output, requires its admission and exact identity, then checks arbitrary-byte canonicalization for idempotence. Production `FAMILIES` supplies the roots; file names and family count must match. There is no input-size skip.

The eight `current-*.bin` inputs are unchanged outputs of `cargo run --locked -p acyclic-proto-build --example capture_descriptors`, captured from protocol sources at revision 93e1318a02c55c2cb7bd39d42d5c429279c7cf92 and adopted after semantic review. They are library compiler outputs, not captured service traffic. Existing `auth`, `minimal` and `empty` inputs remain. Eight authored one-byte `family-*` selectors exercise each positive writer branch; these are fuzzer inputs, not external wire fixtures.

| Corpus input | Bytes | SHA256 |
| --- | ---: | --- |
| `current-auth-v1.bin` | 33585 | `7b6d18e3983e3d7cd5c9aa014e1a9880606bb3976038c513281e8c7d9e8e053c` |
| `current-actors-v1.bin` | 116012 | `5de0bb6bff54a8f3cc6fe869c30ccc14314538319e91ea3b4ea7cb48557a4bb2` |
| `current-filesystem-v2.bin` | 147669 | `3d7c9ad7acc10205121e6309fe9ca554b17974c2e41c13ea335befac6c6a8f1f` |
| `current-inference-v1.bin` | 129082 | `d6ac4a7e8a418150207a6ebf5beee0d9ea87f9329120ec40f25031819920c85c` |
| `current-machines-v1.bin` | 144845 | `461795ad758a680122dfaecb274a19a4c37e7fdbe989a7563858dcce0cad50b1` |
| `current-objects-v1.bin` | 106663 | `00c34d4ea5dd36c384631bb0bc950411aa4f7940e2cc2b9a9b6d397ee67818a7` |
| `current-stream-v1.bin` | 143507 | `dd0782a67e9cc81232aa2b87f84539e17ed38120a52fe0c6bf3a8ee86c241d81` |
| `current-harness-v2.bin` | 54608 | `b487ddb9d705dfd0ecba8834492cfb63897f1f27560c59969acc0804771beccb` |

Run from this directory:

```sh
cargo +nightly fuzz run --fuzz-dir . proto_descriptors --no-default-features --features proto-descriptors -- -runs=256 -seed=1 -max_len=147669
```

`mutations/proto-descriptors-reject-all.patch` changes the actual production canonicalizer to reject nonempty inputs. The old conditional oracle admits this defect; the repaired oracle must fail with `canonical production writer descriptor refused`. Restore production source before running the healthy corpus. A compiler failure is not defect detection.

The generated source-location/field-number property remains: its generated domain is not subsumed by retained compiler outputs. Finite execution and feedback counters do not establish complete source branches, unknown-option preservation, malformed-input refusal, compiler version independence or deployed service behavior. Source branch coverage and additional semantic mutations remain open.
