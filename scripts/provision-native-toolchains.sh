#!/usr/bin/env bash
set -euo pipefail

language=$1
root="${RUNNER_TEMP:?}/acyclic-native-toolchains"
mkdir -p "$root/src" "$root/bin"
jobs="${CMAKE_BUILD_PARALLEL_LEVEL:-2}"

protoc_version='36.2'
protoc_platform=''
protoc_sha256=''
case "$(uname -s):$(uname -m)" in
  Linux:x86_64)
    protoc_platform='linux-x86_64'
    protoc_sha256='121f6c7afe1d4d0e3ea6aab9432038599250134cbf4474cb1167d2c7decd4278'
    ;;
  Linux:aarch64)
    protoc_platform='linux-aarch_64'
    protoc_sha256='8b8f18bd2b30346efbc698dd5a73dd7c805f3ef8380f6dfc95c768f3f1852f6a'
    ;;
  Darwin:arm64)
    protoc_platform='osx-aarch_64'
    protoc_sha256='9cd98a532c5c5e0c4161314de0225de27e4c8a323917b6ea7b1b714d3ae23466'
    ;;
  *)
    echo "unsupported protoc host: $(uname -s):$(uname -m)" >&2
    exit 2
    ;;
esac
protoc_archive="$root/protoc-${protoc_version}-${protoc_platform}.zip"
protoc_root="$root/protoc-${protoc_version}-${protoc_platform}"

if [[ ! -x "$protoc_root/bin/protoc" ]]; then
  command -v curl >/dev/null
  command -v unzip >/dev/null
  curl --fail --location --silent --show-error --retry 3 \
    "https://github.com/protocolbuffers/protobuf/releases/download/v${protoc_version}/protoc-${protoc_version}-${protoc_platform}.zip" \
    --output "$protoc_archive"
  if command -v sha256sum >/dev/null 2>&1; then
    echo "${protoc_sha256}  ${protoc_archive}" | sha256sum --check --status
  else
    echo "${protoc_sha256}  ${protoc_archive}" | shasum -a 256 -c - >/dev/null
  fi
  rm -rf "$protoc_root"
  mkdir -p "$protoc_root"
  unzip -q "$protoc_archive" -d "$protoc_root"
fi

case "$language" in
  cpp)
    grpc_revision='f5e2d6e856176c2f6b7691032adfefe21e5f64c1'
    protobuf_revision='2c74169b34066ceb8ddb6b882fcb3fb32d737a55'
    grpc_source="$root/src/grpc-${grpc_revision}"
    protobuf_source="$root/src/protobuf-${protobuf_revision}"
    absl_source="$grpc_source/third_party/abseil-cpp"
    absl_prefix="$root/absl"
    protobuf_prefix="$root/protobuf"
    grpc_prefix="$root/grpc"

    if [[ ! -f "$grpc_source/CMakeLists.txt" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/grpc/grpc.git "$grpc_source"
      git -C "$grpc_source" checkout --detach "$grpc_revision"
      git -C "$grpc_source" submodule update --init --depth 1
    fi
    if [[ ! -f "$protobuf_source/CMakeLists.txt" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/protocolbuffers/protobuf.git "$protobuf_source"
      git -C "$protobuf_source" checkout --detach "$protobuf_revision"
      git -C "$protobuf_source" submodule update --init --depth 1
    fi

    cmake -S "$absl_source" -B "$root/absl-build" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$absl_prefix" \
      -DABSL_BUILD_TESTING=OFF
    cmake --build "$root/absl-build" --target install --parallel "$jobs"
    cmake -S "$protobuf_source" -B "$root/protobuf-build" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$protobuf_prefix" \
      -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_BUILD_EXAMPLES=OFF \
      -Dprotobuf_BUILD_PROTOC_BINARIES=OFF -Dprotobuf_BUILD_LIBPROTOC=OFF \
      -Dprotobuf_ABSL_PROVIDER=package \
      -DCMAKE_PREFIX_PATH="$absl_prefix"
    cmake --build "$root/protobuf-build" --target install --parallel "$jobs"
    cmake -S "$grpc_source" -B "$root/grpc-build" -G Ninja \
      -DCMAKE_BUILD_TYPE=Release -DCMAKE_INSTALL_PREFIX="$grpc_prefix" \
      -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_BUILD_CODEGEN=ON \
      -DgRPC_PROTOBUF_PROVIDER=package -DgRPC_ABSL_PROVIDER=package \
      -DCMAKE_PREFIX_PATH="$protobuf_prefix;$absl_prefix"
    cmake --build "$root/grpc-build" --target install --parallel "$jobs"
    protoc="$protoc_root/bin/protoc"
    grpc_plugin="$grpc_prefix/bin/grpc_cpp_plugin"
    test -x "$protoc"
    test -x "$grpc_plugin"
    {
      echo "SDK_CPP_PROTOC=$protoc"
      echo "SDK_CPP_GRPC_PLUGIN=$grpc_plugin"
      echo "SDK_CPP_PREFIX_PATH=$grpc_prefix;$protobuf_prefix;$absl_prefix"
    } >> "${GITHUB_ENV:?}"
    ;;
  swift)
    swift_protobuf_revision='55d7a1cc5666b85c13464aea1c4b4a90feccb4c8'
    swift_collections_revision='98ef3c98609a1e31b7e157b5b619579001a789d6'
    grpc_swift_revision='21fe69ab7ce0e87ac089534733c52f037e74a3eb'
    grpc_swift_protobuf_revision='176c5a434fd76f6f479848d1a8f7d44967534168'
    swift_protobuf_source="$root/src/swift-protobuf-${swift_protobuf_revision}"
    swift_collections_source="$root/src/swift-collections-${swift_collections_revision}"
    grpc_swift_source="$root/src/grpc-swift-${grpc_swift_revision}"
    grpc_swift_protobuf_source="$root/src/grpc-swift-protobuf-${grpc_swift_protobuf_revision}"
    command -v swift >/dev/null
    if [[ ! -f "$swift_protobuf_source/Package.swift" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/apple/swift-protobuf.git "$swift_protobuf_source"
      git -C "$swift_protobuf_source" checkout --detach "$swift_protobuf_revision"
    fi
    if [[ ! -f "$grpc_swift_source/Package.swift" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/grpc/grpc-swift-2.git "$grpc_swift_source"
      git -C "$grpc_swift_source" checkout --detach "$grpc_swift_revision"
    fi
    if [[ ! -f "$swift_collections_source/Package.swift" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/apple/swift-collections.git "$swift_collections_source"
      git -C "$swift_collections_source" checkout --detach "$swift_collections_revision"
    fi
    if [[ ! -f "$grpc_swift_protobuf_source/Package.swift" ]]; then
      git clone --filter=blob:none --no-checkout https://github.com/grpc/grpc-swift-protobuf.git "$grpc_swift_protobuf_source"
      git -C "$grpc_swift_protobuf_source" checkout --detach "$grpc_swift_protobuf_revision"
    fi
    # Git on Windows checks out these upstream shared-source symlinks as files.
    # Materialize them so SwiftPM sees the same plugin sources on every host.
    for shared_target in \
      "$grpc_swift_protobuf_source/Plugins/GRPCProtobufGenerator/PluginsShared" \
      "$grpc_swift_protobuf_source/Plugins/GRPCProtobufGeneratorCommand/PluginsShared"; do
      if [[ -f "$shared_target" ]]; then
        rm -f "$shared_target"
        cp -R "$grpc_swift_protobuf_source/Plugins/PluginsShared" "$shared_target"
      fi
    done
    # Materialize Swift plugin shared sources before building on Windows.
    # Pin Swift Collections to the exact OSS revision used by grpc-swift-2.
    python3 - "$grpc_swift_source/Package.swift" "$swift_collections_source" <<'PY'
from pathlib import Path
import re
import sys

manifest = Path(sys.argv[1])
text = manifest.read_text()
text = re.sub(r'\.package\(\s*url: "https://github.com/apple/swift-collections\.git",\s*from: "[^"]+"\s*\)', f'.package(path: "{sys.argv[2]}")', text, count=1)
manifest.write_text(text)
PY
    # Force the generator package to use the exact sibling source revisions above.
    python3 - "$grpc_swift_protobuf_source/Package.swift" "$grpc_swift_source" "$swift_protobuf_source" <<'PY'
from pathlib import Path
import re
import sys

manifest = Path(sys.argv[1])
text = manifest.read_text()
text = re.sub(r'\.package\(\s*url: "https://github.com/grpc/grpc-swift-2\.git",\s*from: "[^"]+"\s*\)', f'.package(path: "{sys.argv[2]}")', text, count=1)
text = re.sub(r'\.package\(\s*url: "https://github.com/apple/swift-protobuf\.git",\s*from: "[^"]+",\s*traits: \[\]\s*\)', f'.package(path: "{sys.argv[3]}")', text, count=1)
manifest.write_text(text)
PY
    python3 "${GITHUB_WORKSPACE:-.}/scripts/run-swift-sanitized.py" swift build --package-path "$swift_protobuf_source" -c release --product protoc-gen-swift
    python3 "${GITHUB_WORKSPACE:-.}/scripts/run-swift-sanitized.py" swift build --package-path "$grpc_swift_protobuf_source" -c release --product protoc-gen-grpc-swift-2
    swift_plugin="$swift_protobuf_source/.build/release/protoc-gen-swift"
    grpc_plugin="$grpc_swift_protobuf_source/.build/release/protoc-gen-grpc-swift-2"
    test -x "$swift_plugin"
    test -x "$grpc_plugin"
    {
      echo "SDK_SWIFT_PROTOC=$protoc_root/bin/protoc"
      echo "SDK_SWIFT_PLUGIN=$swift_plugin"
      echo "SDK_GRPC_SWIFT_PLUGIN=$grpc_plugin"
    } >> "${GITHUB_ENV:?}"
    ;;
  *)
    echo "unsupported native toolchain: $language" >&2
    exit 2
    ;;
esac


