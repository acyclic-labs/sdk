# Filesystem corpus provenance

## Merge-directory writer

The four `merge_directory_resolution/*-v1` files are unchanged bytes read from
the staging generation produced by `JoinPreview::directory` and
`MergeDirectory::write_page` at source
`ef1d2a58e493b32ed9a5920d5c0cb2f54731c90f`, Linux x86_64, 2026-11-07:

```sh
cargo run --locked -p acyclic-fs --no-default-features --features local --example capture_merge_directory
```

The example uses `Fs::local` and durable local storage, forks a child and a
grandchild, makes divergent binary/deletion changes, and creates one clean
file. It reads the original staging generation with bounded directory pages.
The output envelope records each filename and its exact byte array. Extraction
does not parse, rewrite or substitute the documents' identities or metadata.

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| `plan-v1` | 525 | `8d40244039992da842cb3ecffbd399aa68bf6cbda1890cce7a1cf29b557439c4` |
| `clean-0-v1` | 90 | `a8b5b7436bbf3df951f6a5b10d4be930a4eac0bfcbf67ac2702363732b4a1447` |
| `conflicts-0-v1` | 737 | `77628a6af3d22b0d543be4abd85d3b50e49223e499e9f887cbb1619016835542` |
| `conflicts-1-v1` | 554 | `d068ca4b236221834626e17333d381d34fb3512078ea4e1557484f9af379c2b0` |

`type-v1` was captured with the same command on Linux x86_64 at source
`1bad6a85e4502f18e6945f428ec2be386a25491e`. The local nested forks created a
regular file and a directory at the same original binding. The retained 905
bytes are unchanged writer output, SHA-256
`5fda6e169af209a8e58166648bcd529511fd3bc1b95b1fe0e012d1f7e5b1f42e`.

`mode-v1` was captured with the same command on Linux x86_64 at source
`69d66eea15271f4edaa15d6dad33cff3a37300c2`. Divergent directory metadata on the
original nested forks produced a Mode conflict through the canonical publisher.
The retained 731 bytes are unchanged writer output, SHA-256
`40f8958f6f61d1b1be01d9a156c0a14ab13b253e438b91d6874c4eadd9a92f84`.

These witnesses cover admission of the plan, clean and non-text conflict
schemas, with binary, deletion, entry-kind and directory-mode conflicts. They do not cover
restart, upgrades or remote serving.

## Accepted merge resolutions

The three `merge_directory_resolution/resolution-*-v1` traces were captured on
Linux x86_64 at source `4cee52c6ac89460a72223a6bbec2c6ab5f1e8fed`, using the same
`capture_merge_directory` command above. The example writes each caller proposal
into the durable staging volume, pins that generation, and calls the public
`MergeDirectory::resolution` with the original conflict key. It then records the
original conflict, unchanged proposal bytes read from that generation, and the
actual accepted `MergeResolution`. These are public admission traces, not wire
frames or a second semantic model.

| Trace | Bytes | SHA-256 |
| --- | ---: | --- |
| `resolution-take-ours-v1` | 774 | `73294db0232306c2f29d700efdab57b2e59a255a10e3865e57eec2d77b798957` |
| `resolution-take-theirs-v1` | 784 | `40a8dc208246e780144667317d5d6ab00f2a11c394700dbf4cec7e073dd08df8` |
| `resolution-remove-v1` | 591 | `ed1a88b7a5099b301f72ecff8319c35d6143e5efb7fbd9876d420b32283306ef` |

The decoder is checked against these accepted outputs unconditionally. Raw fuzz
inputs remain untrusted and never supply their own expected resolution.

## Plain-text merge resolutions

Two additional traces were captured with the same command on Linux x86_64 at
source `b359e8b3ee87b4fb23d3f5755e3f95703f89cf4d`. The durable local workspace,
child and grandchild change the same original text file. The capture calls
`MergeDirectory::resolution` on the unchanged writer output and requires the
typed `InvalidMergeResolution` refusal for unresolved markers. It then writes
an edited proposal, pins its generation, and records the actual admitted
`MergeResolution::Text`. The original conflict and exact proposal bytes are
retained; the refusal trace has a null expected resolution.

| Trace | Bytes | SHA-256 |
| --- | ---: | --- |
| `resolution-text-accepted-v1` | 224 | `76da544e2c97cdb5711579c7c42324ca5ae427e852a02e20e0b66d46a678f822` |
| `resolution-text-refused-v1` | 418 | `91c0fd4b3b589c4b31d5b2352f6b9c0ead5ec8d21e5fb5829ebe583e35c771d2` |

These traces exercise plain-text proposals rather than JSON resolution files.
Restart, upgrades and remote serving remain outside this capture.

## Session subtree

`subtree-publication-v1` is the unmodified Changes value emitted by the public filesystem producer in `session::resources::subtree::tests::volume_changes_subtree_retains_commit_revision_replay_and_revocation` at source `f52742e907088362fc304a69fe3f35a73b604e6b`, captured on Linux x86_64, 2026-11-02.

Capture command:

```sh
cargo test --locked -p acyclic-fs --no-default-features --features memory,session --lib volume_changes_subtree -- --nocapture
```

The source uses `Fs`, `StreamAuthorityStore`, `MemoryStream` and `MemoryObjectStore`, publishes through the ordinary workspace API, and checks three actual filesystem forks, replay, resume and policy revocation. Authorization is supplied through the test credential helper. This is a library producer capture, not a served network frame or a deployed credential fixture. The seed retains the complete generation and entry references exactly as emitted; no identities or metadata were substituted.
