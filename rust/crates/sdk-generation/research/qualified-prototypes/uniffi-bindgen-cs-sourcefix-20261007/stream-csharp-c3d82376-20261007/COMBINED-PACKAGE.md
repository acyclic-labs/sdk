# Combined Actors.Stream package

`out-combined/Actors.Stream.0.1.0-c3d82376-combined.nupkg` is the single parent package for the c3d82376 producer cohort. It contains the generated `net8.0` assembly and NuGet `runtimes` assets for `win-x64`, `linux-x64`, and `osx-arm64`; a consuming project selects the native asset from its normal runtime identifier.

The package uses the maintained C# generated source and thin `IAsyncEnumerable` adapter recorded by `receipt-combined.json`. `make_combined_package.py` creates the package from the already-qualified all-assets package while changing only the NuGet package identity to the neutral `0.1.0-c3d82376-combined` version. This keeps the existing `out/` and `out-macos-arm64/` cohort artifacts intact.

The combined package was restored, built, and executed from a fresh package cache on ivar (macOS 15.6 arm64), including local and TLS remote operations, full-width `ulong` boundaries, cancellation recovery, and child/follow server close markers. Existing Windows/Linux receipts remain historical source-cohort evidence; fresh combined-package Windows/Linux consumer runs are still a separate gate.
