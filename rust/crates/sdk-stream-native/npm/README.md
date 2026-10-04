# Platform package fixtures

Each directory is an installable platform package shape for the native Stream bridge. The
release builder copies the matching Rust N-API binary beside `index.js`; package metadata
restricts installation to the platform, architecture, and libc that produced that binary.

Build one package with the pinned Rust target and an isolated Cargo directory:

```text
RUSTC_VERSION="$(rustc -V)" node scripts/build-stream-native-package.mjs \
  --target linux-x64-musl \
  --target-dir "$RUNNER_TEMP/stream-native-target" \
  --output "$RUNNER_TEMP/stream-native-packages"
cd "$RUNNER_TEMP/stream-native-packages/linux-x64-musl"
mkdir -p "$RUNNER_TEMP/npm-cache"
npm pack --ignore-scripts --offline --cache "$RUNNER_TEMP/npm-cache" \
  --pack-destination "$RUNNER_TEMP/stream-native-packages"
```

The release/manual workflow runs this builder for all eight targets. Linux musl targets
provision the pinned runner's `musl-tools` package before Cargo starts; Darwin and Windows
use their native runner toolchains. Each artifact includes `BUILD.json` with the Rust target
and binary digest, and the Windows x64 lane installs the packed archive into a clean consumer.

The checked in runtime fixture exercises the Windows x64 binary. The release/manual workflow
builds and archives all eight target packages, and installs the Windows x64 archive into a
clean consumer before uploading it.
