# Contributing

Contributions are licensed under Apache-2.0. Every commit must include a
`Signed-off-by` trailer certifying the [Developer Certificate of Origin](DCO.md).

Before opening a pull request, run the Rust, Bun/TypeScript, Protobuf, provenance,
license, secret, and private-namespace checks used by CI. Imported code must add
an entry to `provenance/manifest.json` before it is merged.

`ci.json` is the only authored CI graph. The immutable Fleet renderer generates
`azure-pipelines.yml`; do not hand-edit the generated file or add a second CI
authority. Independent platform and browser lanes run concurrently, cache only
registries, tools, and compiler outputs, and reuse a successful result only for
the exact source tree and semantic job identity.

Public contracts originate here. A service implementation may validate a
candidate commit, but it must not maintain a competing customer schema.

Every extraction must follow [ARCHITECTURE.md](ARCHITECTURE.md) and include a
deletion summary for the private repository. Copying code without centralizing
ownership is not accepted.
