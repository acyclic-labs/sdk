#!/usr/bin/env bash
set -euo pipefail

validate_release_object_type() {
  case "$1" in tag|commit) ;; *) echo 'release tag must resolve to a tag or commit' >&2; return 1 ;; esac
}

qualified_npm_package() {
  case "$1" in
    objects) printf '%s\t%s\t%s\n' @acyclic-labs/objects objects objects ;;
    stream) printf '%s\t%s\t%s\n' @acyclic-labs/stream stream stream ;;
    inference) printf '%s\t%s\t%s\n' @acyclic-labs/inference inference inference ;;
    machines) printf '%s\t%s\t%s\n' @acyclic-labs/machines machines machines ;;
    fs) printf '%s\t%s\t%s\n' @acyclic-labs/fs filesystem fs ;;
    sdk) printf '%s\t%s\t%s\n' @acyclic-labs/sdk sdk sdk ;;
    *) echo 'package has no qualified npm release' >&2; return 1 ;;
  esac
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then return 0; fi

tag="${GITHUB_REF_NAME:?GitHub tag is required}"
subject=${tag#stage/npm/}
if [[ "$subject" == "$tag" || "$subject" != */* || "${subject#*/}" == */* ]]; then
  echo 'staging tags must use stage/npm/<package>/<version>' >&2
  exit 1
fi
slug=${subject%%/*}
version=${subject#*/}
[[ "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z.-]+)?$ ]]
IFS=$'\t' read -r package directory asset_slug < <(qualified_npm_package "$slug")
manifest="typescript/packages/$directory/package.json"
manifest_version=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1], encoding="utf-8"))["version"])' "$manifest")
sdk_version=$(python3 -c 'import json; print(json.load(open("typescript/packages/sdk/package.json", encoding="utf-8"))["version"])')
test "$version" = "$manifest_version"
release_tag="typescript-v${sdk_version}"

tag_oid=$(git rev-parse "$GITHUB_REF")
publish_sha=$(git rev-parse "$GITHUB_REF^{commit}")
release_oid=$(git rev-parse "refs/tags/${release_tag}")
source_sha=$(git rev-parse "refs/tags/${release_tag}^{commit}")
test "$(git cat-file -t "$tag_oid")" = tag
validate_release_object_type "$(git cat-file -t "$release_oid")"
test "$(git rev-parse HEAD)" = "$publish_sha"
test "$GITHUB_SHA" = "$publish_sha"
test "$source_sha" = "$publish_sha"
git fetch --no-tags origin main
git merge-base --is-ancestor "$source_sha" origin/main

asset="acyclic-labs-${asset_slug}-${version}.tgz"
archive="${RUNNER_TEMP:?}/release-npm/${asset}"
mkdir -p "$(dirname "$archive")"
sha256=$(python3 scripts/fetch-release-crate.py "$release_tag" "$asset" "$archive")
observed=$(python3 scripts/validate-npm-package.py "$archive" "$package" "$version" "typescript/packages/$directory")
test "$sha256" = "$observed"
archive_size=$(stat -c %s "$archive")
qualification="${RUNNER_TEMP}/release-npm/QUALIFICATION.json"
python3 scripts/fetch-release-crate.py "$release_tag" QUALIFICATION.json "$qualification" >/dev/null
python3 scripts/typescript-qualification.py verify "$qualification" "$source_sha" "$asset" "$archive"

printf 'NPM_PACKAGE=%s\nNPM_VERSION=%s\nNPM_ARCHIVE=%s\nSOURCE_SHA=%s\nRELEASE_TAG=%s\n' \
  "$package" "$version" "$archive" "$source_sha" "$release_tag" >> "$GITHUB_ENV"
printf '%s  %s  %s bytes\n' "$sha256" "$asset" "$archive_size"
