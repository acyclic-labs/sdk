# Swift installable package remote verification — 2026-10-07

This is a read-only preservation check of the package and native resources named by `installable-package-20261007-receipt.json`. No fixture was started or restarted and no maintained source was changed.

Maintained generator source: UniFFI Swift source commit `c3d82376d5b5e32a7886a4af7f8a65de8fe60795` (0.31.0). The cancellation template hashes remain:

- `Async.swift`: `830FB89B3C97713CA2C1CF844BE63E0A1AD5D458AF7A3C87A3E2FF58A2B5E0C6`
- `macros.swift`: `5EB1C64482514E8962099B42BE14BBC2C0B375570869CFE391006A77802ABDB4`
- `Helpers.swift`: `932944185A48F7C99C81E0A08C3F89304515D7C20D025F552875D8BB816A7E44`

The Windows package at `Q:\sdk\work\swift-install-pending-20261007-windows-package` was present. Read-only SHA-256 checks returned:

- `Package.swift`: `E2E08C0C9BD42BF7ADC59558398C4A42B4006E41C01BBBBF92E12EF8BC16EF0D`
- `Sources/AcyclicActors/Actors.swift`: `F8812DAF0B3F9C8C653CBE45F19A79CAE780C7732E6166AD65FD02E675AE997B`
- `Native/windows-x86_64/acyclic_actors_uniffi.dll`: `A09452F273B201FA7E3D288F6C3B3FDFC4ABD979431392797D4C80D375D85241`

The macOS consumer package at `/Users/var/swift-install-pending-20261007-mac-package` on `ivar` was present and the remote host reported Apple Swift 6.1.2 on Darwin 24.6.0 arm64. Read-only SHA-256 checks returned:

- `Package.swift`: `52381F0CAF63DB3D84F413F57B466679015DA21203BBB62BB080B1D98E386D0C`
- `Sources/AcyclicActors/Actors.swift`: `F8812DAF0B3F9C8C653CBE45F19A79CAE780C7732E6166AD65FD02E675AE997B`
- `Sources/acyclic_actors_uniffiFFI/acyclic_actors_uniffiFFI.h`: `A840ACE58A5D895D1E166A0F98311C2F71D90C7F53DC40DA6E56A918809D5578`
- `Sources/acyclic_actors_uniffiFFI/module.modulemap`: `7ABF028E5A5A4E169B3F85E4002F66B3463D0E499C1CA034111D6FB1F4709183`
- `Native/macos-arm64/libacyclic_actors_uniffi.dylib`: `8959C7F8A903ED9DBE3139935CE23BA9FC5E0356FE65390493853995DA9B5AB4`
- built consumer executable: `2774E0BD85AD657960EC8B4A5A5FD59F2299A1341D645CBA00E222ECF8B6ADA2`

The remote package and executable paths were verified present; the previously used fixture control/endpoint ports were not listening during this check. Existing runtime results remain those recorded by `installable-package-20261007-receipt.json`: Windows and macOS all-eight and in-flight `Task.cancel` runs passed with `CancellationError()` and server active-to-aborted cleanup.