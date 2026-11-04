# Retained owner envelopes

These three nonsecret inputs were captured on Linux x86_64 from SDK source
`234e045c0b23061db2afe553224c6414b5f2bee0` using the original
`owner_record::encode_with` implementation:

```sh
cargo run --locked -p acyclic-native-runtime --example capture_owner_record -- rust/crates/native-runtime/tests/owner_codec/corpus
```

| File | Bytes | SHA-256 |
| --- | ---: | --- |
| binary | 45 | 868361675d3dbffd1c83ac7fa87fa7ba31dd721c7dc02e0e74fd7f23f5d9b7f9 |
| minimal | 41 | 93ef0cda77b20fa9d9ae17662d856093092c9b650513998fb3cbe59a97136876 |
| text | 81 | 4c146cce76b33c36e33c107fb5fa1627d3d1a79773f9ba9d579942b45c2952e1 |

The capture uses only the deliberate payloads in the shared oracle's `seeds`;
it reads no credentials or signing keys. Ordinary tests replay the retained
bytes and check current-owner outputs. Windows protection is randomized and
user-bound, so retained Unix bytes do not establish Windows decryption.
