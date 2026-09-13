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

tag_oid=$(git rev-parse "$GITHUB_REF")
source_sha=$(git rev-parse "$GITHUB_REF^{commit}")
test "$(git cat-file -t "$tag_oid")" = tag
test "$(git rev-parse HEAD)" = "$source_sha"
test "$GITHUB_SHA" = "$source_sha"
git fetch --no-tags origin main
git merge-base --is-ancestor "$source_sha" origin/main

metadata=$(cargo metadata --locked --no-deps --format-version 1)
record=$(printf '%s' "$metadata" | python3 -c '
import json, sys
package = sys.argv[1]
matches = [item for item in json.load(sys.stdin)["packages"] if item["name"] == package]
if len(matches) != 1 or matches[0].get("publish") == []:
    raise SystemExit("tag does not select exactly one publishable workspace crate")
print(matches[0]["version"])
' "$package")
test "$record" = "$version"

cargo publish --dry-run --locked -p "$package"
git diff --exit-code
test -z "$(git status --porcelain --untracked-files=no)"

crate="target/package/${package}-${version}.crate"
test -f "$crate"
sha256=$(sha256sum "$crate" | cut -d' ' -f1)
[[ "$sha256" =~ ^[0-9a-f]{64}$ ]]

printf 'PACKAGE=%s\n' "$package" >> "$GITHUB_ENV"
printf 'VERSION=%s\n' "$version" >> "$GITHUB_ENV"
printf 'CRATE=%s\n' "$crate" >> "$GITHUB_ENV"
printf 'CRATE_SHA256=%s\n' "$sha256" >> "$GITHUB_ENV"
printf 'SOURCE_SHA=%s\n' "$source_sha" >> "$GITHUB_ENV"
printf 'TAG_OID=%s\n' "$tag_oid" >> "$GITHUB_ENV"
printf '%s  %s\n' "$sha256" "$crate"
