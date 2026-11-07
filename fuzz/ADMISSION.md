Both invocation headers and failure codes require positive production admission
on every fuzz input. An arbitrary malformed packet must not let an implementation
which rejects every packet satisfy a conditional round-trip assertion.

`invocation_headers` reuses the original public-constructor/prost writer captures
in `rust/codec-fuzz/corpus/protobuf/acyclic.actors.v1.InvokeActorRequest-semantic`
and `acyclic.actors.v1.InvokeWorkerRequest-semantic`. Their original hashes and
producer are recorded in `rust/codec-fuzz/corpus/SOURCES.tsv`; the matching seeds
in `fuzz/corpus/invocation_headers` remove only the eight-byte corpus selector.
Their payload SHA-256 hashes are respectively
`efc9b79b8f7fa8d19f2dee384d9484501b8dc6f58529b83f92b9dca152a8a845`
and `39bbe221f82ba975295c27dd90b9460c602683d170c8e2921947ccc84ec6c304`.
The same assertions check admission, body refusal, and metadata preservation for
the retained writer output and arbitrary inputs.

`stream_error` selects a public `StreamError::ALL` value independently of
`from_code`, serializes it through the production serializer, and requires its
original value back. Raw-input differential checks remain. Capture the failure
corpus with the same serializer:

```sh
cargo run --locked -p acyclic-guest-fuzz --example stream_error_corpus -- fuzz/corpus/stream_error
```

The shared qualifier first applies each retained production mutation, requires
the intended assertion to fail inside libFuzzer, restores the exact production
source, and runs 1,024 instrumented inputs with the retained corpus:

```sh
node rust/proofs/qualify.mjs invocation-header-admission
node rust/proofs/qualify.mjs stream-error-admission
```

The invocation mutation rejects every deployment envelope in the shared
production admission; the failure mutation removes all canonical code matches.
Neither mutation changes the oracle. These are codec checks, with no external
producer or scheduling assumption. Work is bounded by input bytes and the two
fixed invocation captures or one fixed failure value, with no I/O per input.
They do not establish transport, storage, or executor progress.

The sixteen retained failure seeds were captured on Linux x86_64 using the
command above at source `365a0a114659a59036801b729f07d88d0f78e512`.
Each file contains the original serializer output without a newline:

| Seed | Bytes | SHA-256 |
| --- | ---: | --- |
| invalid_response | 18 | `b0083fa76e35b00fd866d4bd77fb3d046f50e01e2464fb1f97156803737cfded` |
| invalid_path | 14 | `09cb7f293ccd720d5e788e7928c9a4629e1f2aa9ccd36f8f1580c9707fb5d599` |
| invalid_argument | 18 | `ad1178c68f284945f11e5c9b420e65568290954840a93094b04337e7eae548e9` |
| limit_exceeded | 16 | `89d71ca64d962ee1e520efe84d4005c9ecd26bf97a92a489d47879693449372a` |
| not_found | 11 | `75be4a295c8deb47a4949dc99a0ff684306ff97dfccac48d4e183df4dc19b4f2` |
| already_exists | 16 | `78c990d7626544c9308ea27cfc057ad4c9ea146d9fca20297ab380d2241b7de9` |
| prefix_not_retained | 21 | `ddd429793446fafe94bd6e8ce7009a8f104b2365055be92ec39419836bd8d5e4` |
| out_of_range | 14 | `944d835112feb039142261de1c4b0904d3d65c4db1737834b18c5fcadc4f7dd3` |
| idempotency_mismatch | 22 | `428837f3445fa81d54ec585dcb5eb200cc851e157daa124110f4fd747c870982` |
| capacity | 10 | `84594495fd229041f2e657348480b248e0ed8d3f1ab78e68024f8fa06386736c` |
| access_denied | 15 | `e25e1a4d1eeb7f1334d915eed070314e22d9b8f03be18bc7db8c0cbb929f8d32` |
| unavailable | 13 | `ec833b19d159ba681e50bda3b3397116bad0053bb383e42149b8ac22a17a580f` |
| hierarchy_changed | 19 | `a3595d6b40e5c157b3fd4a1383e1dac69a4b07412af1ac11485c277e78b3e4e0` |
| deadline_elapsed | 18 | `38710b3fb214a0a97eea66a9f003ba0a11873022ee6e6b2749e0bd3153abb8eb` |
| unsupported | 13 | `0bc6f540bbf1d48a52b38b78f201053bc6ac6a49a3bd6105deccacea34c8b9b8` |
| revision_foreign | 18 | `e9c4efa792081a5bbd7d5a0d180ab5183dd169b8ca5edf98c9b6b6b68051f3ba` |

The qualifier selects the compiler host explicitly as its Cargo target.
This confines coverage instrumentation to target code and keeps host build
scripts and procedural macros free of unresolved coverage callbacks.
