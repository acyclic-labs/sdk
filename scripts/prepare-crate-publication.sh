#!/usr/bin/env bash
set -euo pipefail

tag="${GITHUB_REF_NAME:?GitHub tag is required}"
subject=${tag#publish/}
if [[ "$subject" == "$tag" || "$subject" != */* || "${subject#*/}" == */* ]]; then
  echo "release tags must use publish/<crate>/<version>" >&2
  exit 1
fi

package=${subject%%/*}
version=${subject#*/}
[[ "$package" =~ ^[a-z0-9][a-z0-9_-]*$ ]]
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]]

case "$package" in
  acyclic-objects) family=objects ;;
  acyclic-stream) family=stream ;;
  inference-sdk) family=inference ;;
  acyclic-machines) family=machines ;;
  acyclic-fs) family=filesystem ;;
  *) echo "package has no qualified release family" >&2; exit 1 ;;
esac
release_tag="${family}-v${version}"

tag_oid=$(git rev-parse "$GITHUB_REF")
publish_sha=$(git rev-parse "$GITHUB_REF^{commit}")
release_oid=$(git rev-parse "refs/tags/${release_tag}")
source_sha=$(git rev-parse "refs/tags/${release_tag}^{commit}")
test "$(git cat-file -t "$tag_oid")" = tag
test "$(git cat-file -t "$release_oid")" = tag
test "$(git rev-parse HEAD)" = "$publish_sha"
test "$GITHUB_SHA" = "$publish_sha"
git fetch --no-tags origin main
git merge-base --is-ancestor "$publish_sha" origin/main
git merge-base --is-ancestor "$source_sha" origin/main

asset="${package}-${version}.crate"
crate="${RUNNER_TEMP:?}/release-crate/${asset}"
mkdir -p "$(dirname "$crate")"
sha256=$(python3 scripts/fetch-release-crate.py "$release_tag" "$asset" "$crate")
python3 scripts/publish-crate.py --check "$package" "$version" "$crate" "$sha256" "$source_sha"

printf 'PACKAGE=%s\n' "$package" >> "$GITHUB_ENV"
printf 'VERSION=%s\n' "$version" >> "$GITHUB_ENV"
printf 'CRATE=%s\n' "$crate" >> "$GITHUB_ENV"
printf 'CRATE_SHA256=%s\n' "$sha256" >> "$GITHUB_ENV"
printf 'SOURCE_SHA=%s\n' "$source_sha" >> "$GITHUB_ENV"
printf 'PUBLISH_SHA=%s\n' "$publish_sha" >> "$GITHUB_ENV"
printf 'TAG_OID=%s\n' "$tag_oid" >> "$GITHUB_ENV"
printf 'RELEASE_TAG=%s\n' "$release_tag" >> "$GITHUB_ENV"
printf '%s  %s\n' "$sha256" "$asset"
