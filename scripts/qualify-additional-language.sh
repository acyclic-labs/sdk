#!/usr/bin/env bash
set -euo pipefail

language=${1:?language is required}
source_root=${2:?source root is required}
output_root=${3:?output root is required}

mkdir -p "$output_root"
proto_root="$source_root/proto"
proto="$proto_root/actors/v1/actors.proto"
test -f "$proto"

# This probe is deliberately generated alongside the Rust-owned product.  It
# exercises the selected ecosystem's streaming facade without inventing a
# second product contract: the Actors schema remains the serialization input.
streaming_proto="$output_root/streaming_probe.proto"
cat >"$streaming_proto" <<'EOF'
syntax = "proto3";
package acyclic.qualification.v1;

message ProbeFrame {
  string payload = 1;
}

service QualificationStream {
  rpc Exchange(stream ProbeFrame) returns (stream ProbeFrame);
}
EOF

archive_project() {
  local project=$1
  local archive_name=$2
  tar -czf "$output_root/$archive_name" -C "$output_root" "$(basename "$project")"
  test -s "$output_root/$archive_name"
}

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
    protoc -I "$proto_root" -I "$output_root" --plugin="$plugin" \
      --elixir_out=plugins=grpc:"$project/lib/generated" "$proto" "$streaming_proto"
    mix format --check-formatted
    mix compile --warnings-as-errors
    test -n "$(find lib/generated -type f -name '*.ex' -print -quit)"
    test -n "$(find lib/generated -type f -name '*_grpc.ex' -print -quit)"
    test -n "$(rg -l 'stream' lib/generated --glob '*_grpc.ex' | head -n 1)"
    module=$(sed -n 's/^defmodule \([^ ]*\).*/\1/p' lib/generated/*_pb.ex | head -n 1)
    test -n "$module"
    mix run --no-start -e "m = String.to_atom(\"Elixir.$module\"); value = struct(m); encoded = apply(m, :encode, [value]); decoded = apply(m, :decode, [encoded]); unless decoded == value, do: raise \"protobuf round trip failed\""
    mix archive.build --output "$output_root/acyclic_sdk_elixir.ez"
    test -s "$output_root/acyclic_sdk_elixir.ez"
    popd >/dev/null
    archive_project "$project" acyclic_sdk_elixir.tar.gz
    ;;
  erlang)
    project="$output_root/erlang"
    mkdir -p "$project/proto" "$project/src" "$project/include"
    cp "$proto" "$project/proto/actors.proto"
    cat >"$project/rebar.config" <<'EOF'
{erl_opts, [debug_info]}.
{deps, [{grpcbox, "0.18.0"}]}.
{plugins, [{grpcbox_plugin, "0.9.0"}]}.
{grpc, [{protos, "proto"}, {gpb_opts, [{module_name_suffix, "_pb"}, maps]}]}.
EOF
    cat >"$project/src/acyclic_qualification.app.src" <<'EOF'
{application, acyclic_qualification, [{description, "Rust-derived Acyclic qualification"}, {vsn, "0.0.0"}, {applications, [kernel, stdlib, grpcbox]}]}.
EOF
    cp "$streaming_proto" "$project/proto/streaming_probe.proto"
    pushd "$project" >/dev/null
    rebar3 grpc gen
    rebar3 compile
    test -n "$(find src -type f -name '*_pb.erl' -print -quit)"
    client_module=$(find src -type f -name '*_client.erl' -print -quit)
    test -n "$client_module"
    test -n "$(rg -l 'stream' src --glob '*_client.erl' | head -n 1)"
    module=$(basename "$(find src -type f -name 'actors_pb.erl' -print -quit)" .erl)
    test -n "$module"
    erl -noshell -pa _build/default/lib/*/ebin -eval "M=$module, [N|_] = M:get_msg_names(), B = M:encode_msg(#{}, N), _ = M:decode_msg(B, N), halt()."
    popd >/dev/null
    archive_project "$project" acyclic_sdk_erlang.tar.gz
    ;;
  ocaml)
    project="$output_root/ocaml"
    mkdir -p "$project/proto" "$project/lib"
    cp "$proto" "$project/proto/actors.proto"
    opam install --yes ocaml-protoc-plugin.6.2.0 grpc.0.2.0
    plugin="$(command -v protoc-gen-ocaml || true)"
    test -n "$plugin"
    protoc -I "$proto_root" -I "$output_root" --plugin="$plugin" --ocaml_out="$project/lib" "$proto" "$streaming_proto"
    cat >"$project/dune-project" <<'EOF'
(lang dune 3.7)
(name acyclic_qualification)
EOF
    cat >"$project/lib/dune" <<'EOF'
(library (name acyclic_qualification) (wrapped false)
 (libraries ocaml-protoc-plugin))

(executable (name serialization_smoke) (libraries acyclic_qualification ocaml-protoc-plugin))
EOF
    cat >"$project/lib/serialization_smoke.ml" <<'EOF'
let () =
  let value = ProbeFrame.make ~payload:"rust-owned" () in
  let encoded = ProbeFrame.to_proto value |> Protobuf.Writer.contents in
  match ProbeFrame.from_proto (Protobuf.Reader.create encoded) with
  | Ok decoded when decoded = value -> print_endline "protobuf round trip passed"
  | Ok _ -> failwith "protobuf round trip changed the message"
  | Error _ -> failwith "protobuf round trip could not decode"
EOF
    (cd "$project" && dune build)
    (cd "$project" && dune exec ./lib/serialization_smoke.exe)
    test -n "$(rg -l 'QualificationStream|stream' "$project/lib" --glob '*.ml' | head -n 1)"
    archive_project "$project" acyclic_sdk_ocaml.tar.gz
    ;;
  common-lisp)
    project="$output_root/common-lisp"
    git clone --filter=blob:none https://github.com/atgreen/ag-gRPC.git "$project"
    git -C "$project" checkout --detach f76354d8e3ba5a65c2767df68d32570a1668883d
    command -v ocicl >/dev/null
    (cd "$project" && ocicl install)
    make -C "$project" cli
    mkdir -p "$project/generated"
    "$project/ag-protoc" -o "$project/generated/actors.lisp" "$proto"
    "$project/ag-protoc" -o "$project/generated/streaming_probe.lisp" "$streaming_proto"
    test -s "$project/generated/actors.lisp"
    test -s "$project/generated/streaming_probe.lisp"
    sbcl --non-interactive --load "$project/generated/actors.lisp" \
      --load "$project/generated/streaming_probe.lisp" \
      --eval '(unless (find-package :acyclic.actors.v1) (error "generated Actors package missing"))' \
      --eval '(format t "generated Common Lisp package loaded~%")'
    test -n "$(rg -i 'stream|exchange' "$project/generated/streaming_probe.lisp" | head -n 1)"
    archive_project "$project" acyclic_sdk_common_lisp.tar.gz
    ;;
  *)
    echo "unsupported qualification target: $language" >&2
    exit 2
    ;;
esac

write_receipt passed
