#!/usr/bin/env bash
set -euo pipefail

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

qualified_run() {
  local source_sha=$1
  jq -er --arg sha "$source_sha" '
    [.workflow_runs[] | select(
      .head_sha == $sha and .status == "completed" and .conclusion == "success"
    )]
    | sort_by([.run_number, .run_attempt]) | last
    | [.id, .run_attempt] | @tsv
  '
}

qualified_tag_revisions() {
  local tag=$1 ref tag_oid source_sha
  ref="refs/tags/$tag"
  git show-ref --verify --quiet "$ref"
  tag_oid=$(git rev-parse "$ref")
  test "$(git cat-file -t "$tag_oid")" = tag
  source_sha=$(git rev-parse "$ref^{commit}")
  printf '%s\t%s\n' "$tag_oid" "$source_sha"
}

if [[ "${BASH_SOURCE[0]}" != "$0" ]]; then return 0; fi

tag="${NPM_STAGE_TAG:?npm staging tag is required}"
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
test "$version" = "$manifest_version"

read -r tag_oid source_sha < <(qualified_tag_revisions "$tag")
test "$(git rev-parse HEAD)" = "$source_sha"
test "${GITHUB_EVENT_NAME:?GitHub event name is required}" = workflow_dispatch
test "${NPM_STAGE_TAG_OID:?annotated tag object SHA is required}" = "$tag_oid"
git fetch --no-tags origin main
git merge-base --is-ancestor "$source_sha" origin/main

asset="acyclic-labs-${asset_slug}-${version}.tgz"
artifact_root="${RUNNER_TEMP:?}/qualified-npm"
mkdir -p "$artifact_root"
run_json=$(gh api --method GET \
  "repos/${GITHUB_REPOSITORY:?}/actions/workflows/qualification.yml/runs" \
  -f head_sha="$source_sha" -f status=success -f event=push -F per_page=20)
read -r run_id run_attempt < <(qualified_run "$source_sha" <<<"$run_json")
artifact_name="packages-linux-${run_id}-${run_attempt}"
gh run download "$run_id" --name "$artifact_name" --dir "$artifact_root"
archive="$artifact_root/typescript/$asset"
qualification="$artifact_root/typescript/QUALIFICATION.json"
test -f "$archive"
test -f "$qualification"
sha256=$(python3 scripts/validate-npm-package.py "$archive" "$package" "$version" "typescript/packages/$directory")
archive_size=$(stat -c %s "$archive")
python3 scripts/typescript-qualification.py verify "$qualification" "$source_sha" "$asset" "$archive"

printf 'NPM_PACKAGE=%s\nNPM_VERSION=%s\nNPM_ARCHIVE=%s\nSOURCE_SHA=%s\nQUALIFICATION_RUN=%s\n' \
  "$package" "$version" "$archive" "$source_sha" "$run_id" >> "$GITHUB_ENV"
printf '%s  %s  %s bytes  qualification run %s\n' "$sha256" "$asset" "$archive_size" "$run_id"
