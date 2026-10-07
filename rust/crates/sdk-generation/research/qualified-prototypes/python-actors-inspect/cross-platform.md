# Python cross-platform packaging and typing qualification

## Linux toolchain

The approved WSL Ubuntu host is `Var`. It has Python 3.10.12, Rust 1.98.1,
`mypy` 1.17.1, and Pyright 1.1.404. The generated Python source was checked
with both maintained type checkers from an isolated external staging directory.

Both checkers report all six deliberate errors in `typing/negative.py`:
wrong `ActorId` input, wrong `CodeSha256` input, wrong `ActorLimits` input,
wrong header list element, wrong invoke body, and wrong response assignment.
The exact diagnostics are retained in `typing/mypy-negative.txt` and
`typing/pyright-negative.json`.

The positive fixture has one generated-output error in both checkers. UniFFI
emits `SubscriptionStart.CURSOR(1)` as the nested `CURSOR` class, while the
`SubscriptionSpec` constructor annotates its `start` parameter as
`SubscriptionStart`. The runtime enum is valid, but the generated Python type
relationship is not. The maintained UniFFI 0.32.2 template has the same
behavior. The proposed upstream correction is recorded in
`uniffi-032-enum-typing.md`.

## Linux package

The Rust facade was built with the pinned WSL toolchain into
`Q:\sdk\work\actors-uniffi-linux-build-current-20261007`:

- `libacyclic_actors_uniffi.so`
- SHA-256 `AA6427DD1C6836F164CE83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24`
- size 6,566,744 bytes

The source-owned packaging recipe in `rust/crates/actors-uniffi/python` was
used to create:

- `acyclic_actors_uniffi-0.2.0-py3-none-linux_x86_64.whl`
- SHA-256 `0D96809BE41C62DC893EDA16CB8DC1A1F1602F7227622EF1A167BDDA5288D154`
- size 2,302,199 bytes

The wheel contains the generated Python module, `py.typed`, and the Linux
`.so`. It installed into an isolated WSL Python 3.10 environment and passed
nominal constructor checks including `u64::MAX`. The remote fixture is bound
to Windows loopback and is not reachable from WSL (`curl https://localhost:55755`
returned code 000), so the all-eight remote run remains Windows-qualified.

## Windows packaging

The same source-owned recipe now includes README metadata and all three native
suffixes. A fresh Windows wheel was produced with the current native DLL:

- `acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`
- SHA-256 `340984D8F57F154BDFEEB4F017D0AA07628626978B0A4932625BF165AB36B571`
- size 3,016,134 bytes

Its metadata includes the package README, `py.typed`, and the Windows `.dll`.
The source recipe derives the version from the Rust `Cargo.toml`, with an
explicit `ACYCLIC_ACTORS_CARGO_MANIFEST` override for staged builds.

## macOS toolchain

The approved SSH host reports Darwin 24.6.0 arm64, Python 3.14.3, and Rust
1.96.0 as its default. Rustup already has the explicit pinned
`1.98.1-aarch64-apple-darwin` toolchain installed, so no global or default
change was needed. There is no macOS Actors checkout or native artifact on the
host yet; no macOS package qualification is claimed.

No binary artifacts were copied into the repository, and no merge or
publication was performed.

