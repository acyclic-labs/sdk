#!/usr/bin/env bash
set -euo pipefail

language=${1:?language is required}
source_root=${2:?source root is required}
output_root=${3:?output root is required}

mkdir -p "$output_root"
proto_root="$source_root/proto"
proto="$proto_root/actors/v1/actors.proto"
test -f "$proto"

write_receipt() {
  local status=$1
  local proto_digest
  proto_digest=$(sha256sum "$proto" | awk '{print $1}')
  cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.additional-language-qualification.v1","language":"$language","status":"$status","source_revision":"${GITHUB_SHA:-local}","proto":"proto/actors/v1/actors.proto","proto_sha256":"$proto_digest","artifact_root":"$output_root"}
EOF
}

case "$language" in
  elixir)
    project="$output_root/elixir"
    mix new "$project" --sup >/dev/null
    python3 - "$project/mix.exs" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text()
s = s.replace('defp deps do\n      []', 'defp deps do\n      [{:protobuf, "0.13.2"}, {:grpc, "1.0.3"}]')
p.write_text(s)
PY
    pushd "$project" >/dev/null
    mix do deps.get, deps.compile
    mix escript.install hex protobuf 0.13.2 --force
    plugin="$HOME/.mix/escripts/protoc-gen-elixir"
    test -x "$plugin"
    mkdir -p lib/generated
    protoc -I "$proto_root" --plugin="$plugin" --elixir_out=plugins=grpc:"$project/lib/generated" "$proto"
    mix format --check-formatted
    mix compile --warnings-as-errors
    test -n "$(find lib/generated -type f -name '*.ex' -print -quit)"
    popd >/dev/null
    ;;
  erlang)
    project="$output_root/erlang"
    mkdir -p "$project/proto" "$project/src" "$project/include"
    cp "$proto" "$project/proto/actors.proto"
    cat >"$project/rebar.config" <<'EOF'
{erl_opts, [debug_info]}.
{plugins, [{rebar3_gpb_plugin, "2.10.0"}]}.
{gpb_opts, [{i, "proto"}, {o_erl, "src"}, {o_hrl, "include"}, type_specs]}.
{provider_hooks, [{pre, [{compile, {protobuf, compile}}]}]}.
EOF
    cat >"$project/src/acyclic_qualification.app.src" <<'EOF'
{application, acyclic_qualification, [{description, "Rust-derived Acyclic qualification"}, {vsn, "0.0.0"}, {applications, [kernel, stdlib]}]}.
EOF
    pushd "$project" >/dev/null
    rebar3 compile
    test -n "$(find src -type f -name '*_pb.erl' -print -quit)"
    popd >/dev/null
    ;;
  ocaml)
    project="$output_root/ocaml"
    mkdir -p "$project/proto" "$project/lib"
    cp "$proto" "$project/proto/actors.proto"
    opam install --yes ocaml-protoc-plugin.6.2.0 grpc.0.2.0
    plugin="$(command -v protoc-gen-ocaml || true)"
    test -n "$plugin"
    protoc -I "$proto_root" --plugin="$plugin" --ocaml_out="$project/lib" "$proto"
    cat >"$project/dune-project" <<'EOF'
(lang dune 3.7)
(name acyclic_qualification)
EOF
    cat >"$project/lib/dune" <<'EOF'
(library (name acyclic_qualification))
EOF
    (cd "$project" && dune build)
    ;;
  common-lisp)
    project="$output_root/common-lisp"
    git clone --filter=blob:none https://github.com/atgreen/ag-gRPC.git "$project"
    git -C "$project" checkout --detach f76354d8e3ba5a65c2767df68d32570a1668883d
    make -C "$project" cli
    mkdir -p "$project/generated"
    "$project/ag-protoc" -o "$project/generated/actors.lisp" "$proto"
    test -s "$project/generated/actors.lisp"
    sbcl --non-interactive --load "$project/generated/actors.lisp" --eval '(format t "generated Common Lisp package loaded~%")'
    ;;
  *)
    echo "unsupported qualification target: $language" >&2
    exit 2
    ;;
esac

write_receipt passed
