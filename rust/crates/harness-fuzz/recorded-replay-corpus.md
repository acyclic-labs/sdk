# Recorded replay schedules

`recorded_replay` calls the shared
`acyclic_harness_core::test_support::assert_replay_admission` oracle, which uses
the production `RecordedModel` native generation, reconciliation and tool-result
entrances. The ordinary retained-corpus test calls the same oracle.

The first sixteen input bytes also select the operation identity, padded with
zeroes for shorter inputs. Those bytes still select actions. No random UUID is
created inside an input: independent runs therefore use identical 52-byte
dispatch index keys. This takes sixteen byte observations and no allocation,
system call or entropy request.

Each byte chooses one of two interleaved canonical SDK requests, one of four
dispatch coordinates, returned-object allocation credit, cancellation after
acceptance, and an observed event prefix. The small schedule domain is a test
generator, not a product limit. Every request, source event and tool result is
encoded by the actual canonical SDK writer. No provider response or credential
fixture is synthesized or claimed as a recording.

The three initial schedules are explicitly generated controls:

| File | Hexadecimal schedule |
| --- | --- |
| `refusal-retry` | `00081018202830380038` |
| `interleaved-cancellation` | `78797a7b7c7d7e7f38393a3b` |
| `accepted-reconciliation` | `b8b9babbbcbdbebff8f9fafbfcfdfeff` |

The ordinary corpus control also replays `refusal-retry` with operation identity
`369244c89aa44235bc4f4ab23daa7479`, captured unchanged from the first
`INDEX52` observation in the native diagnostic at commit
`d559de01eb80e9c91b171065b0ea8080b15dfe6f`. The original observation began
with action `00`, an empty index and no differing bit. This preserves that
original key independently of the deterministic generated operation identities;
it does not establish a compiler or index repair.

The oracle independently tracks only accepted source occurrence coordinates and
consumed tool results. A refused returned object must change neither; an
accepted cancellation must retain its typed cause and the original answer on
retry. Reconciliation returns the exact remaining source suffix. The retained
mutation `fuzz/mutations/recorded_replay_before_admission.patch` moves cursor
consumption before returned-object admission; the unchanged oracle must reject
it on `refusal-retry`.

Run the target from the repository root using the harness fuzz package's pinned
toolchain:

```sh
cargo +nightly-2026-07-01 fuzz run --fuzz-dir rust/crates/harness-fuzz --no-default-features --features replay recorded_replay
```

Native work/cancellation is exercised in process. Live-producer admission,
pending overlap, malformed persisted input, browser lifecycle, native provider
transport and full source branch coverage remain separate obligations. The
original focused regression retains pending/live admission coverage.
