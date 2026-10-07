# Swift final-producer type verification (2026-10-07)

The final producer checkout at `Q:\\sdk\\work\\sdkgen-main-actual03bb` compiled with UniFFI 0.31 (`cargo check --features bindgen` and release cdylib build both passed). The source cohort is pinned by `producer-source-final-20261007.lock.json` with fingerprint `E9DC7271C3A051368AB6BDC23582E142DDCD25AF07A9EA22C0FB1E3F15B4AD4C`. The generated output is preserved under `generated-final-20261007`.

The generator consumed the actual Rust cdylib hash `CC1CCF87020FE7345D82279D370E3BE22701B27ACE421C802AD8C3B832399BEF`. `StartProto` is present in the maintained producer and the build completed successfully.

The strongest Swift type checks are currently **blocked** by the final producer's direct UniFFI custom-type and record derives:

- `ActorId` is emitted as `public typealias ActorId = String`, so an arbitrary string can be forged.
- `PositiveU64` is emitted as `public typealias PositiveU64 = UInt64`, so zero can be forged in a record initializer.
- `CodeSha256` is emitted as `public typealias CodeSha256 = Data`, so length and non-zero validation are not represented in Swift's type surface.
- Records expose mutable `public var` fields and public initializers.
- `Start.currentHead(Bool)` admits `false`; the Rust semantic conversion rejects false on ingress, but the Swift constructor surface does not enforce the invariant.
- `UInt64` fields are preserved losslessly and oneof decoding is exhaustive (`default` throws `unexpectedEnumCase`).

This is a producer surface finding, not a Swift cancellation workaround. The generated package must not be qualified as satisfying opaque constructors, immutable records, or false-current-head rejection until the maintained producer or generator metadata supplies those semantics without a handwritten mirror.

Generated hashes: `acyclic_actors.swift` `4F55C84E0A2CF3586F5F94CFEEA18AD9259DE96CD30610CDB9EEF3E04800EB43`; `acyclic_actors_uniffi.swift` `F08BDB43E3EE8D78CF0404FEE5909D61933C004039954758A24A8AE79611DDE8`; header `E4F3AF42A00C9D2E73668EFD2C3874DF4C9D569BACDDCFC09EF6F1F093001C0C`; modulemap `5F7D09F8908EC3C7C8AD568B9DE1C91B60C39B69B04D5FC7A7904E0BD98604F5`.
