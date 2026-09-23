#!/usr/bin/env bash
# Keeps the agent instructions and contributor docs truthful:
#   1. CLAUDE.md is exactly an import of AGENTS.md.
#   2. Every CI lane in scripts/qualify-ci.sh is named by the workflow matrix.
#   3. Every backticked repository path in the top-level docs exists.
#   4. Nothing outside the changelog and provenance still names the retired
#      plugin repository or its git-dependency plumbing.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
fail=0

[ "$(cat CLAUDE.md)" = "@AGENTS.md" ] || { echo "CLAUDE.md must be exactly '@AGENTS.md'" >&2; fail=1; }

for lane in $(grep -oE '^  [a-z0-9-]+\)$' scripts/qualify-ci.sh | tr -d ' )'); do
  grep -qE "^\s+- lane: $lane$" .github/workflows/qualification.yml || {
    echo "lane '$lane' is in scripts/qualify-ci.sh but not in the qualification.yml matrix" >&2; fail=1; }
done

# Top-level docs name paths from the repository root.
docs=(AGENTS.md CONTRIBUTING.md README.md ARCHITECTURE.md)
while read -r path; do
  path="${path%%:*}"   # drop a :line suffix
  [ -e "$path" ] || { echo "path named in docs does not exist: $path" >&2; fail=1; }
done < <(grep -ohE '`(plugin|plugins|rust|typescript|proto|generated|provenance|scripts|\.github)/[^` ]*`' "${docs[@]}" \
  | tr -d '`' | sed -E 's/[.,;)]+$//' | grep -vE '[*<>]' | sort -u)

# plugin/README.md names paths relative to plugin/ (it says so), except
# rust/ ones. Host config it writes into a user's repository is not checked.
while read -r path; do
  path="${path%%:*}"
  case "$path" in rust/*) full="$path" ;; *) full="plugin/$path" ;; esac
  [ -e "$full" ] || { echo "path named in plugin/README.md does not exist: $full" >&2; fail=1; }
done < <(grep -ohE '`(crates|tests|scripts|packaging|docs|rust)/[^` ]*`' plugin/README.md \
  | tr -d '`' | sed -E 's/[.,;)]+$//' | grep -vE '[*<>]' | sort -u)

stale="$(git grep -nE 'graphcoder-plugin|CARGO_NET_GIT_FETCH_WITH_CLI|releases/latest/download' -- . \
  ':!plugin/CHANGELOG.md' ':!provenance/manifest.json' ':!scripts/check-agent-docs.sh' ':!plugin/docs/design/' || true)"
[ -z "$stale" ] || { echo "retired references:" >&2; echo "$stale" >&2; fail=1; }

[ "$fail" -eq 0 ] && echo "agent docs are consistent"
exit "$fail"
