#!/usr/bin/env bash
# Selects and verifies the exact acyclic CLI plugin binaries a successful
# qualification run retained, for publish-plugin.yml. Nothing is rebuilt:
# every byte shipped is a byte the acceptance suite (or the Windows smoke)
# ran against on a lane. Mirrors prepare-npm-publication.sh, whose run and
# tag helpers it reuses.
set -euo pipefail

# shellcheck source=scripts/prepare-npm-publication.sh
source "$(dirname "${BASH_SOURCE[0]}")/prepare-npm-publication.sh"

# The published targets and the lane artifact each one is retained in.
plugin_targets() {
  printf '%s\n' darwin-arm64 darwin-x64 linux-x64 linux-arm64 win32-x64
}

plugin_artifact_for() {
  case "$1" in
    darwin-arm64|darwin-x64) printf '%s\n' plugin-macos ;;
    linux-x64) printf '%s\n' plugin-linux ;;
    linux-arm64) printf '%s\n' plugin-linux-arm64 ;;
    win32-x64) printf '%s\n' plugin-windows ;;
    *) echo "unknown plugin target: $1" >&2; return 1 ;;
  esac
}

plugin_exe_suffix() {
  case "$1" in win32-*) printf '%s\n' .exe ;; *) printf '%s\n' '' ;; esac
}

# stage/npm/plugin/<version> -> <version>
plugin_stage_version() {
  local tag=$1 version
  [[ "$tag" =~ ^stage/npm/plugin/([0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?)$ ]] || {
    echo 'plugin staging tags must use stage/npm/plugin/<version>' >&2
    return 1
  }
  version=${BASH_REMATCH[1]}
  printf '%s\n' "$version"
}

# Every plugin crate and plugin/LATEST must name exactly this version; the
# release tag, the binaries' --version output and the npm packages all
# derive from it.
plugin_version_agrees() {
  local version=$1 root=${2:-.} manifest crate_version
  for manifest in "$root"/plugin/crates/*/Cargo.toml; do
    crate_version=$(awk -F'"' '/^version = /{print $2; exit}' "$manifest")
    [[ "$crate_version" == "$version" ]] || {
      echo "$manifest names version $crate_version, tag names $version" >&2
      return 1
    }
  done
  local latest
  latest=$(tr -d ' \r\n' < "$root/plugin/LATEST")
  [[ "$latest" == "$version" ]] || {
    echo "plugin/LATEST names $latest, tag names $version" >&2
    return 1
  }
}

# A retained directory holds the binary, a SHA256SUMS naming it, and the
# SOURCE_COMMIT the lane qualified. Anything else is not a release input.
verify_retained_binary() {
  local dir=$1 source_sha=$2 name=$3 retained
  [[ -f "$dir/$name" && -f "$dir/SHA256SUMS" && -f "$dir/SOURCE_COMMIT" ]] || {
    echo "retained plugin binary is incomplete in $dir" >&2
    return 1
  }
  retained=$(tr -d ' \r\n' < "$dir/SOURCE_COMMIT")
  [[ "$retained" == "$source_sha" ]] || {
    echo "$dir was qualified for $retained, not $source_sha" >&2
    return 1
  }
  (cd "$dir" && sha256sum --check --strict --quiet SHA256SUMS) || {
    echo "$dir/SHA256SUMS does not match its binary" >&2
    return 1
  }
  grep -q " $name\$" "$dir/SHA256SUMS" || {
    echo "$dir/SHA256SUMS does not name $name" >&2
    return 1
  }
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then return 0; fi

tag="${PLUGIN_STAGE_TAG:?plugin staging tag is required}"
version=$(plugin_stage_version "$tag")
plugin_version_agrees "$version"
# shellcheck source=plugin/scripts/product.sh
source plugin/scripts/product.sh
name="$PRODUCT_NAME"
package="$PRODUCT_NPM_PACKAGE"
release_tag="${PRODUCT_RELEASE_TAG_PREFIX}${version}"

read -r tag_oid source_sha < <(qualified_tag_revisions "$tag")
test "$(git rev-parse HEAD)" = "$source_sha"
test "${GITHUB_EVENT_NAME:?GitHub event name is required}" = workflow_dispatch
test "${PLUGIN_STAGE_TAG_OID:?annotated tag object SHA is required}" = "$tag_oid"
git fetch --no-tags origin main
git merge-base --is-ancestor "$source_sha" origin/main

run_json=$(gh api --method GET \
  "repos/${GITHUB_REPOSITORY:?}/actions/workflows/qualification.yml/runs" \
  -f head_sha="$source_sha" -f status=success -f event=push -F per_page=20)
read -r run_id run_attempt < <(qualified_run "$source_sha" <<<"$run_json")

artifact_root="${RUNNER_TEMP:?}/qualified-plugin"
dist="${RUNNER_TEMP}/plugin-dist"
mkdir -p "$artifact_root" "$dist"
downloaded=""
while read -r target; do
  artifact=$(plugin_artifact_for "$target")
  if [[ " $downloaded " != *" $artifact "* ]]; then
    gh run download "$run_id" --name "${artifact}-${run_id}-${run_attempt}" \
      --dir "$artifact_root/$artifact"
    downloaded="$downloaded $artifact"
  fi
  exe=$(plugin_exe_suffix "$target")
  verify_retained_binary "$artifact_root/$artifact/$target" "$source_sha" "$name$exe"
  install -m 0755 "$artifact_root/$artifact/$target/$name$exe" "$dist/$name-$target$exe"
  (cd "$dist" && sha256sum "$name-$target$exe" >> SHA256SUMS)
done < <(plugin_targets)
(cd "$dist" && sha256sum --check --strict --quiet SHA256SUMS)

printf 'PLUGIN_VERSION=%s\nPLUGIN_NAME=%s\nPLUGIN_NPM_PACKAGE=%s\nPLUGIN_RELEASE_TAG=%s\nPLUGIN_DIST=%s\nSOURCE_SHA=%s\nQUALIFICATION_RUN=%s\n' \
  "$version" "$name" "$package" "$release_tag" "$dist" "$source_sha" "$run_id" >> "$GITHUB_ENV"
echo "plugin $version from qualification run $run_id ($source_sha):"
cat "$dist/SHA256SUMS"
