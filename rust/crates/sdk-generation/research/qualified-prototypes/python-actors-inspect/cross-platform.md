# Python cross-platform packaging and typing qualification

## Linux toolchain

WSL Ubuntu uses Rust 1.98.1, Python 3.10.12, mypy 1.17.1, and Pyright 1.1.404.
Both checkers reject all six deliberate negative typing cases. The positive
fixture passes both checkers with the isolated maintained-generator correction
documented in `uniffi-python-typing-patch.md`; the generated public
`SubscriptionStartValue` union preserves the nested `CURSOR` payload type.

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

SSH host `ivar` is Darwin 24.6.0 arm64 with Python 3.14.3. The explicit pinned
`1.98.1-aarch64-apple-darwin` toolchain built the current Actors UniFFI native
facade. The fresh arm64 wheel installed and passed the nominal constructor and
runtime probe:

- `libacyclic_actors_uniffi.dylib`, SHA-256 `28D885561244BD2D682D1103B70AD8C1989AC341777F28499731C1140FF3B719`, 5,122,144 bytes
- `acyclic_actors_uniffi-0.2.0-py3-none-macosx_15_0_arm64.whl`, SHA-256 `F83C3D76CA2E8A8C26AF13A29403B4C294879F84574003BC4BBCE5D110BCD15B`

The live remote fixture is Windows-local and was not run from ivar.

No binary artifacts were copied into the repository, and no merge or
publication was performed.

The macOS arm64 native asset is reusable by the Swift and JVM qualification jobs: the ivar build at `/tmp/actors-uniffi-mac-target/release/libacyclic_actors_uniffi.dylib` has SHA-256 `28D885561244BD2D682D1103B70AD8C1989AC341777F28499731C1140FF3B719` and 5,122,144 bytes. Those jobs should consume that exact asset and record the hash instead of rebuilding the Rust native library.
