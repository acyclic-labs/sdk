# Python cross-platform packaging and typing qualification

## Linux toolchain

WSL Ubuntu uses Rust 1.98.1, Python 3.10.12, mypy 1.17.1, and Pyright 1.1.404.
Both checkers reject all six deliberate negative typing cases. The positive
fixture exposes the non-flat enum relationship documented in
`uniffi-032-enum-typing.md`.

## Linux package

Built with the pinned WSL toolchain:

- `libacyclic_actors_uniffi.so`, SHA-256 `AA6427DD1C6836F164CE83BF0D14C83E6CD8BC1598683654F7619DB5BF76A24`, 6,566,744 bytes
- `acyclic_actors_uniffi-0.2.0-py3-none-linux_x86_64.whl`, SHA-256 `E410FEE2924CA8FCAFC749CB6747FA3191E6B534BDB7EBC6AC509D1F5A7A5FF4`, 2,302,199 bytes

The wheel includes the generated Python module, `py.typed`, and the Linux
`.so`. It installed in isolated WSL Python 3.10 and passed nominal constructor
checks including `u64::MAX`. The Windows loopback fixture is not reachable
from WSL (`curl https://localhost:55755` returned code 000), so all-eight
remote behavior remains qualified by the Windows run.

## Windows packaging

The source-owned recipe includes README metadata and all native suffixes. The
fresh wheel is `acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`, SHA-256
`340984D8F57F154BDFEEB4F017D0AA07628626978B0A4932625BF165AB36B571`, 3,016,134
bytes. It includes README metadata, `py.typed`, and the current Windows DLL;
the installed wheel passed all eight remote operations plus service-error and
cancellation checks.

## macOS toolchain

SSH host `ivar` is Darwin 24.6.0 arm64 with Python 3.14.3 and default Rust
1.96.0. The explicit pinned `1.98.1-aarch64-apple-darwin` toolchain is already
installed and runs Rust 1.98.1 and Cargo 1.98.1. There is no macOS Actors
checkout or native artifact on the host yet, so macOS package qualification is
pending.

No binary artifacts were copied into the repository, and no merge or
publication was performed.