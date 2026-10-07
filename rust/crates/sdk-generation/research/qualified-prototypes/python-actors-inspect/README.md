# Python Actors all-eight qualification

This record tracks the Rust-owned `actors-uniffi` facade and its installed
Windows x64 Python wheel. The checked-in consumer and receipt are the source
record for the external qualification run.

The source revision is `371bb4170e16aca973176b6756a261ee5add7297`. The adapter
`rust/crates/actors-uniffi/src/lib.rs` has SHA-256
`0AB7C322373075A4DD60113A7C768F15B15CC5BE5EEAEACCC44277049292F3B5`.
The generated Python facade has SHA-256
`AC3E8CD8FD86ECFDB7515D3D9195DFD79B021CECE1BEA279C420BDAA09456FB8`, and the
rebuilt native DLL has SHA-256
`A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`.

The installed wheel is
`acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`, SHA-256
`151B60F7644E1EEE98969FDF0F0DB11CE5A6FDE967E604DB6DEEA9F62C807919`.
The package contains generated annotations and the source-owned `py.typed`
marker. The wheel was installed into an isolated environment without network
resolution.

The installed consumer exercises all eight canonical operations: create,
update, inspect, add subscription, remove subscription, resume subscription,
checkpoint, and invoke. It also verifies the typed service error for an
unauthorized call and a pre-cancelled operation. The run passed against the
Rust-backed fixture. The full source and result are retained in
`installed-all8-remote-conformance.py` and `all8-qualification-receipt.json`.

`typing/positive.py` and `typing/negative.py` are maintained-checker fixtures.
Mypy 1.17.1 and Pyright 1.1.404 both reject all six deliberate negative cases.
The source-only patch in `uniffi-python-typing.patch` corrects the generated
enum union metadata, so `SubscriptionStart.CURSOR(1)` is accepted as the
`SubscriptionStartValue` parameter type while retaining the nested runtime
class. `run-uniffi-python-typing-patch.sh` reproduces the generated module
from the pinned UniFFI 0.31.0 archive and verifies its hash. The full vendor
copy used during diagnosis has been removed; provenance and license evidence
remain in `uniffi-python-typing-patch-provenance.md`.

The generated Kotlin and Swift artifact hashes and the shared fixture identity
are recorded in `all8-qualification-receipt.json`. No binary artifacts are
checked into this research directory.


Cross-platform package and host evidence is recorded in cross-platform.md and cross-platform-qualification-receipt.json.

