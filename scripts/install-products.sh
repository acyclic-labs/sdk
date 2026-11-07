#!/bin/sh
set -eu
version=${1:?recovery: sh install-products.sh VERSION [PREFIX]}
prefix=${2:-"$HOME/.local"}
case "$version" in ''|*[!0-9A-Za-z.-]*) printf '%s\n' 'version admission failed; recovery: pass an exact release version' >&2; exit 64;; esac
case "$(uname -s)/$(uname -m)" in
  Linux/x86_64) platform=linux-x64;;
  Linux/aarch64|Linux/arm64) platform=linux-arm64;;
  *) printf '%s\n' 'platform admission failed; recovery: install a Linux x64 or arm64 release' >&2; exit 64;;
esac
temporary=$(mktemp -d)
step=download
trap 'status=$?; rm -rf "$temporary"; if [ "$status" -ne 0 ]; then printf "Install step failed: %s; recovery: sh install-products.sh VERSION PREFIX\n" "$step" >&2; fi' 0
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
base="https://github.com/acyclic-labs/sdk/releases/download/acyclic-v$version"
fetch() {
  curl --fail --location --silent --show-error "$base/$1" -o "$temporary/$1" || {
    printf 'download failed: %s; recovery: sh install-products.sh %s %s\n' "$1" "$version" "$prefix" >&2; exit 69;
  }
}
fetch SHA256SUMS
for product in graphcoder acyclic-plugin; do
  asset="$product-$version-$platform.tar.gz"
  fetch "$asset"
  expected=$(awk -v name="$asset" '$2 == name && length($1) == 64 { print $1 }' "$temporary/SHA256SUMS")
  if [ "${#expected}" -ne 64 ] || ! (cd "$temporary" && printf '%s  %s\n' "$expected" "$asset" | sha256sum -c -); then
    printf 'checksum verification failed: %s; recovery: sh install-products.sh %s %s\n' "$asset" "$version" "$prefix" >&2; exit 65
  fi
  step=extract
  tar -xzf "$temporary/$asset" -C "$temporary"
done
step=install
install -d -- "$prefix/bin" "$prefix/share/acyclic-plugin"
cp -R -- "$temporary/plugin/." "$prefix/share/acyclic-plugin/"
install -m 755 -- "$temporary/graphcoder" "$prefix/bin/graphcoder"
printf 'Installed GraphCoder in %s/bin and plugin in %s/share/acyclic-plugin\n' "$prefix" "$prefix"
