# Python Actors inspect qualification

This record covers the installed Windows x64 wheel for the Rust-owned
`actors-uniffi` facade. It qualifies the current `ActorsClient::inspect_actor`
slice; it is not a qualification of a complete Python SDK. The adapter does
not currently export `ActorLimits`, `SubscriptionStart`, or a stream reader.

The source snapshot was revision `371bb4170e16aca973176b6756a261ee5add7297`.
The Rust crate version was read from
`rust/crates/actors-uniffi/Cargo.toml` (`0.2.0`, Cargo.toml SHA-256
`382B7819A8EAC5CCC8264E9C4EF743CC4CD359808A67434BE2E687271A14200F`). The
adapter source SHA-256 was
`A7E0399E381BA2A2B1D8D07587E81CF2C6245FAC3E04277E04331179DC2BC8CF`.

The generated Python facade SHA-256 was
`A7ECAEAD2058C98B1231EAD7F3C10EC5AC194903E29A271F70D1D61822203744` and the
matching native artifact SHA-256 was
`F04A0403781A67916B941307E67BC660C5CCDE0B6098F3F4ED6C960DC88FA77A`.
The external source-staged probe passed before packaging. Its SHA-256 was
`8BB6AC79E44100EBC671D82EE42C284C97E6298B91E99EC6CBABC5C676E3B615`.

The maintained PyPA build path (`python -m build --wheel --no-isolation`,
setuptools `84.0.0`, wheel `0.46.3`) produced
`acyclic_actors_uniffi-0.2.0-py3-none-win_amd64.whl`, SHA-256
`B1A7B5E1561999F65725D2EFC44E92937EBF1D1F91C0AD4DE7F3EAB52A4E3D24`.
The wheel uses Python plus ctypes and a Windows native library, so the generic platform tag py3-none-win_amd64 is correct; current runtime qualification ran on CPython 3.12. The wheel metadata records `Root-Is-Purelib: false` and the native Windows
tag. Version `0.2.0` came from the Rust Cargo manifest; it was not separately
chosen for the Python artifact.

The wheel was installed into an isolated virtual environment with
`pip install --no-index --no-deps`. The installed consumer reran the same
shared fixture probe and passed nominal constructors, exact 32-byte digest
preservation, `u64::MAX` and invalid-value checks, CA-authenticated unary
`inspect_actor`, exact typed response fields, and pre-cancelled inspection.

The complete external receipts are retained at:

- `Q:\sdk\work\actors-uniffi-python-qualification-receipt.json`
- `Q:\sdk\work\actors-uniffi-python-wheel-20261007\wheel-qualification-receipt.json`

No package was published.



