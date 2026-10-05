#!/usr/bin/env bash
set -euo pipefail

# Thin runtime harness: Rust owns the contract and projections; this script
# only compiles the generated package and records installed toolchain evidence.
target=${1:?target is required}
package_root=${2:?generated package root is required}
output_root=${3:?receipt directory is required}
archive=${4:-}
mkdir -p "$output_root"

if [[ -n "$archive" ]]; then
  [[ -s "$archive" ]] || { echo "generated archive is missing: $archive" >&2; exit 1; }
  command -v unzip >/dev/null 2>&1 || { echo 'unzip is required to qualify the installable artifact' >&2; exit 2; }
  installed_root="$output_root/unpacked"
  rm -rf "$installed_root"
  mkdir -p "$installed_root"
  unzip -q "$archive" -d "$installed_root"
  [[ -d "$installed_root/actors" ]] || { echo 'archive did not contain the Rust-generated actors package' >&2; exit 1; }
  package_root="$installed_root"
fi

run_logged() {
  local name=$1
  shift
  "$@" >"$output_root/$name.stdout" 2>"$output_root/$name.stderr"
}

archive_sha256=''
if [[ -n "$archive" ]]; then
  if command -v sha256sum >/dev/null 2>&1; then
    archive_sha256=$(sha256sum "$archive" | awk '{print $1}')
  else
    archive_sha256=$(shasum -a 256 "$archive" | awk '{print $1}')
  fi
fi

case "$target" in
  ada)
    command -v gprbuild >/dev/null 2>&1 || { echo 'gprbuild is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name '*.gpr' -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Ada project files were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      run_logged "ada-$(basename "${project%.gpr}")" gprbuild -p -P "$project"
    done
    runtime='gnat/gprbuild'
    ;;
  clojure)
    command -v lein >/dev/null 2>&1 || { echo 'Leiningen is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name project.clj -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Clojure projects were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      run_logged "clojure-$(basename "$(dirname "$project")")" lein -f "$project" check
    done
    runtime='clojure/leiningen'
    ;;
  crystal)
    command -v shards >/dev/null 2>&1 || { echo 'Shards is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name shard.yml -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Crystal shards were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      run_logged "crystal-$(basename "$(dirname "$project")")" bash -c "cd \"$(dirname \"$project\")\" && shards build"
    done
    runtime='crystal/shards'
    ;;
  elm)
    command -v elm >/dev/null 2>&1 || { echo 'Elm is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name elm.json -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Elm projects were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      dir=$(dirname "$project")
      cat >"$dir/QualificationMain.elm" <<'EOF'
module QualificationMain exposing (main)

import Api.Request.Default
import Html exposing (Html, text)

main : Html msg
main =
    text (Debug.toString Api.Request.Default.createActor)
EOF
      run_logged "elm-$(basename "$dir")" bash -c "cd \"$dir\" && elm make src/QualificationMain.elm --output=qualification.js"
    done
    runtime='elm'
    ;;
  gdscript)
    command -v godot >/dev/null 2>&1 || { echo 'Godot 4 is required' >&2; exit 2; }
    mapfile -t roots < <(find "$package_root" -type f -name '*.gd' -printf '%h\n' | sort -u)
    ((${#roots[@]} > 0)) || { echo 'no GDScript files were generated' >&2; exit 1; }
    for dir in "${roots[@]}"; do
      cat >"$dir/project.godot" <<'EOF'
[application]
config/name="Acyclic generated package qualification"
[rendering]
renderer/rendering_method="gl_compatibility"
EOF
      cat >"$dir/qualification.gd" <<'EOF'
extends SceneTree
func _init() -> void:
    var files := []
    _collect("res://", files)
    for path in files:
        if path.ends_with(".gd") and path != "res://qualification.gd" and load(path) == null:
            push_error("failed to parse " + path)
            quit(1)
            return
    quit(0)
func _collect(path: String, files: Array) -> void:
    var dir := DirAccess.open(path)
    if dir == null:
        return
    dir.list_dir_begin()
    var name := dir.get_next()
    while name != "":
        if name != "." and name != "..":
            var child := path + name
            if dir.current_is_dir():
                _collect(child + "/", files)
            else:
                files.append(child)
        name = dir.get_next()
    dir.list_dir_end()
EOF
      run_logged "gdscript-$(basename "$dir")" godot --headless --path "$dir" --script qualification.gd
    done
    runtime='godot-4/gdscript'
    ;;
  nim)
    command -v nim >/dev/null 2>&1 || { echo 'Nim is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name '*.nimble' -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Nimble packages were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      main=$(find "$(dirname "$project")" -maxdepth 1 -name '*.nim' -type f | sort | head -n 1)
      [[ -n "$main" ]] || { echo "no Nim entrypoint beside $project" >&2; exit 1; }
      run_logged "nim-$(basename "$(dirname "$project")")" nim check "$main"
    done
    runtime='nim/nimble'
    ;;
  r)
    command -v R >/dev/null 2>&1 || { echo 'R is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name DESCRIPTION -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no R packages were generated' >&2; exit 1; }
    for description in "${projects[@]}"; do
      dir=$(dirname "$description")
      run_logged "r-$(basename "$dir")" bash -c "cd \"$dir\" && R CMD build --no-build-vignettes . && R CMD check --no-manual --no-vignettes --as-cran --no-tests --no-install \"\$(ls -1t *.tar.gz | head -n 1)\""
    done
    runtime='R/R CMD check'
    ;;
  *) echo "unsupported HTTP target: $target" >&2; exit 2 ;;
esac

python3 - "$output_root/runtime-qualification.json" "$target" "$runtime" "${ACYCLIC_RUST_SOURCE_REVISION:-unknown}" "$archive" "$archive_sha256" <<'PY'
import json, pathlib, sys
path, target, runtime, revision, archive, archive_sha256 = sys.argv[1:]
payload = {
    "schema": "acyclic.sdk.http-target-runtime-qualification.v1",
    "target": target,
    "status": "passed",
    "scope": "generated-rust-openapi-http-projection",
    "runtime": {"name": runtime},
    "source_revision": revision,
    "archive": archive or None,
    "archive_sha256": archive_sha256 or None,
    "installed_from_archive": bool(archive),
    "streaming": "not-applicable-to-http-projection",
    "native_grpc": "unqualified",
}
pathlib.Path(path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
