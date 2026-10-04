# Platform package fixtures

Each directory is an installable platform package for the native Stream bridge. The release
builder copies the matching Rust N-API binary beside `index.js`; package metadata restricts
installation to the platform, architecture, and libc that produced that binary. `BUILD.json`
ships in the archive and binds the napi-rs versions, Rust target, source revision, and artifact
hashes.

Build one package with the pinned Rust target and an isolated Cargo directory:

```text
SOURCE_REVISION="$(git rev-parse HEAD)" RUSTC_VERSION="$(rustc -V)" node scripts/build-stream-native-package.mjs \
  --target linux-x64-musl \
  --target-dir "$RUNNER_TEMP/stream-native-target" \
  --output "$RUNNER_TEMP/stream-native-packages" \
  --provenance "$RUNNER_TEMP/stream-native-packages/linux-x64-musl.provenance.json"
cd "$RUNNER_TEMP/stream-native-packages/linux-x64-musl"
mkdir -p "$RUNNER_TEMP/npm-cache"
npm pack --ignore-scripts --offline --cache "$RUNNER_TEMP/npm-cache" \
  --pack-destination "$RUNNER_TEMP/stream-native-packages"
```

The release/manual workflow runs this builder for all eight targets. Linux musl targets
provision the pinned runner's `musl-tools` package before Cargo starts; Darwin and Windows
use their native runner toolchains. Each artifact includes `BUILD.json` with the Rust target,
source revision, and binary digest, plus a target provenance file alongside the package. The
Windows x64 lane installs the packed archive into a clean consumer after packing.

The checked in runtime fixture exercises the Windows x64 binary. The release/manual workflow
builds and archives all eight target packages, and installs the Windows x64 archive into a
clean consumer before uploading it.
