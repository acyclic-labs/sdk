# Python Objects, Filesystem, and Workers remote sweep

This receipt records one fresh installed-Python run against the Rust fixture. The runner used the generated package identified by the Rust model SHA and wheel hash below, sent every method in the descriptor inventory, and used explicit header/body/complete frames for Objects `PutObject` and `UploadPart`.

- Rust model source revision: `f7a2e0eedea058cf85b1746e8dd0af8716cb362e035c45938b0c1ddf3d4d9b2a`
- Installed wheel SHA-256: `8496E7D67AF04F60E6197BAF9FF521F79D3B28F62B71A119541E2077F73505AD`
- Fixture binary SHA-256: `1CFB09F18959E4013513B40C4F68ADCE935583803E9BA936BFD2EE6885D5DAE6`
- Fixture source SHA-256: `7096011f17bf2760e7c19ec72f2e55d9cff953f52963c754918f45c786ef2d81`

The run invoked Workers 7/7, Objects 13/13, and Filesystem 30/30 over gRPC with `execution_mode` set to `remote` on every row. All 50 calls completed at the transport and protobuf boundary. The two server streams that are empty by fixture definition are recorded as successful remote calls with `semantic_check=fixture_defined_empty_stream`: Objects `GetObject` and Filesystem `Export`. The remaining handlers return explicit default messages, so this evidence does not promote stateful object-content or filesystem-generation semantics.

The same 50-method inventory also ran from the installed Go module archive (`4c10017b9fe2b15dcc8d764ab0ff77dd23cb6bed510237a41822ca454ae376f5`): Workers 7/7, Objects 13/13, and Filesystem 30/30 completed over gRPC, with the same two fixture-defined empty streams. The Go and Python logs are kept separately so package provenance stays explicit.

The Rust-owned Objects scenario defines a local `MemoryObjects` put/get body roundtrip, while the Rust-owned Filesystem scenario defines a local mounted workspace. Those local semantics remain separate from this remote sweep until the fixture handlers expose equivalent stateful behavior. The full 106-method gate remains pending until the same frozen authority is rerun with those handlers and the remaining stateful families.

No registry publishing, deployment, or main merge is included.


