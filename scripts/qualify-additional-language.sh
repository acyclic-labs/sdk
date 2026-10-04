#!/usr/bin/env bash
set -euo pipefail

language=${1:?language is required}
source_root=${2:?source root is required}
output_root=${3:?output root is required}
product_root=${4:-${RUST_PRODUCTS_ROOT:-}}

hash_file() {
  if command -v sha256sum >/dev/null; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

mkdir -p "$output_root"
[[ -n "$product_root" && -d "$product_root" ]] || {
  echo 'Rust product output is required; pass the generate-products directory as the fourth argument' >&2
  exit 2
}
proto_root="$product_root"
proto=$(find "$product_root" -type f -path '*/actors/v1/actors.proto' -print -quit)
streaming_proto="$product_root/objects/v2/objects.proto"
[[ -n "$proto" && -f "$streaming_proto" ]] || {
  echo 'Rust product output is missing Actors or Objects protobuf sources' >&2
  exit 1
}
descriptor_set="$output_root/rust-contracts.pb"
product_manifest="$product_root/rust-authority.json"
[[ -f "$product_manifest" ]] || { echo 'Rust authority manifest is missing from product output' >&2; exit 1; }
inventory_json="$output_root/rust-contract-inventory.json"
python3 "$source_root/scripts/resolve-rust-contract-inventory.py" \
  "$product_root" "$inventory_json"
read -r rpc_count archived_rpc_count all_rpc_count < <(
  python3 "$source_root/scripts/resolve-rust-contract-inventory.py" \
    "$product_root" "$inventory_json" --counts | tr -d '\r'
)
mapfile -t contract_protos < <(
  python3 "$source_root/scripts/resolve-rust-contract-inventory.py" \
    "$product_root" "$inventory_json" --current-protos | tr -d '\r' |
    while IFS= read -r contract_proto; do
      if command -v cygpath >/dev/null 2>&1; then
        cygpath -u "$contract_proto"
      else
        printf '%s\n' "$contract_proto"
      fi
    done
)
contract_proto_count=${#contract_protos[@]}
[[ "$all_rpc_count" == $((rpc_count + archived_rpc_count)) ]] || {
  echo "Rust product output inventory is inconsistent: current=$rpc_count archived=$archived_rpc_count total=$all_rpc_count" >&2
  exit 1
}
export ACYCLIC_RUST_CURRENT_RPC_COUNT="$rpc_count"
export ACYCLIC_RUST_ARCHIVED_RPC_COUNT="$archived_rpc_count"
export ACYCLIC_RUST_ALL_RPC_COUNT="$all_rpc_count"
# LuaJIT invokes its own Rust/C ABI qualification script. Keep this wrapper
# free of a protoc host dependency for that lane while retaining the complete
# descriptor inventory for the source-generating targets below.
if [[ "$language" != "lua-remote" ]]; then
  protoc -I "$product_root" --descriptor_set_out="$descriptor_set" --include_imports "${contract_protos[@]}"
  [[ -s "$descriptor_set" ]] || { echo 'Rust contract descriptor bundle is missing' >&2; exit 1; }
fi
proto_relative=${proto#"$product_root/"}
source_revision=$(git -C "$source_root" rev-parse HEAD 2>/dev/null || printf 'local')
manifest_digest=$(hash_file "$product_manifest")
export ACYCLIC_RUST_SOURCE_REVISION="$source_revision"
export ACYCLIC_RUST_AUTHORITY_MANIFEST_SHA256="$manifest_digest"
stream_request_frames="$output_root/rust-stream-request-frames.tsv"
if [[ -n "${ACYCLIC_RUST_TYPED_REQUEST_MANIFEST:-}" && -s "$ACYCLIC_RUST_TYPED_REQUEST_MANIFEST" ]]; then
  python3 "$source_root/scripts/write-rust-stream-request-scenarios.py" \
    "$ACYCLIC_RUST_TYPED_REQUEST_MANIFEST" "$stream_request_frames"
  if [[ -s "$stream_request_frames" ]]; then
    export ACYCLIC_RUST_STREAM_REQUEST_FRAMES="$stream_request_frames"
    export ACYCLIC_STREAM_SCENARIO_RPCS="$(cut -f1 "$stream_request_frames" | paste -sd, -)"
  fi
fi

run_runtime_probe() {
  local endpoint=${ACYCLIC_FIXTURE_GRPC_ENDPOINT:-}
  [[ -n "$endpoint" ]] || return 0
  command -v python3 >/dev/null || { echo "python3 is required for the Rust authority runtime probe" >&2; exit 1; }
  python3 -c "import grpc, google.protobuf" >/dev/null 2>&1 || {
    echo "grpcio and protobuf are required for ACYCLIC_FIXTURE_GRPC_ENDPOINT" >&2
    exit 1
  }
  python3 "$source_root/scripts/run-rust-authority-grpc-probe.py" \
    "$descriptor_set" "$endpoint" "$output_root/rust-authority-grpc-probe.json"
}
archive_project() {
  local project=$1
  local archive_name=$2
  tar -czf "$output_root/$archive_name" -C "$output_root" "$(basename "$project")"
  test -s "$output_root/$archive_name"
}

write_receipt() {
  local status=$1
  local proto_digest
  proto_digest=$(hash_file "$proto")
  local manifest_digest
  manifest_digest=$(hash_file "$product_manifest")
  cat >"$output_root/qualification.json" <<EOF
{"schema":"acyclic.additional-language-qualification.v1","language":"$language","status":"$status","source_revision":"$source_revision","rust_product_root":"generated-products","rust_authority_manifest_sha256":"$manifest_digest","proto":"$proto_relative","proto_sha256":"$proto_digest","contract_proto_count":$contract_proto_count,"rpc_count":$rpc_count,"current_rpc_count":$rpc_count,"archived_rpc_count":$archived_rpc_count,"all_rpc_count":$all_rpc_count,"inventory_scope":"current-rust-authority","archived_inventory_scope":"immutable-compatibility-only","descriptor_set":"rust-contracts.pb","streaming_proto":"objects/v2/objects.proto","artifact_root":"$output_root"}
EOF
}

require_stream_completion() {
  local receipt=$1
  if rg -q 'deferred-rust-scenario' "$receipt"; then
    echo "Rust-owned streaming scenarios are missing from $receipt; refusing count-only qualification" >&2
    return 1
  fi
}

require_current_receipt_count() {
  python3 - "$1" "$rpc_count" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as stream:
    payload = json.load(stream)
expected = int(sys.argv[2])
observed = int(payload.get("rpc_count", -1))
if observed != expected:
    raise SystemExit(f"runtime receipt contains {observed} observations; expected {expected} current Rust RPCs")
PY
}

verify_rust_wire_semantics() {
  local receipt=$1
  local output=$2
  [[ -n "${ACYCLIC_RUST_TYPED_REQUEST_MANIFEST:-}" && -s "$ACYCLIC_RUST_TYPED_REQUEST_MANIFEST" ]] || {
    echo 'Rust executable typed-request manifest is required for semantic qualification' >&2
    return 1
  }
  cargo run --locked --manifest-path "$source_root/rust/crates/sdk-contract-wire/Cargo.toml" \
    --bin verify-observations -- \
    --manifest "$ACYCLIC_RUST_TYPED_REQUEST_MANIFEST" \
    --observed "$receipt" \
    --source-git-sha "$source_revision" \
    --output "$output"
  test -s "$output"
}

case "$language" in
  elixir)
    project="$output_root/elixir"
    cargo test --locked --manifest-path "$source_root/rust/crates/sdk-contract-wire/Cargo.toml" \
      semantic_oracle::tests::only_exercised_scenarios_have_expectations --lib -- --exact
    cat >"$output_root/rust-semantic-oracle.json" <<EOF
{"schema":"acyclic.rust-semantic-oracle.v1","source_revision":"$source_revision","status":"passed","test":"semantic_oracle::tests::only_exercised_scenarios_have_expectations"}
EOF
    # `elixir` is reserved by the runtime, so the output directory cannot be
    # used as Mix's inferred application name on Windows or current Elixir.
    mix new "$project" --app acyclic_sdk_qualification --sup >/dev/null
    python3 - "$project/mix.exs" <<'PY'
from pathlib import Path
import sys
p = Path(sys.argv[1])
s = p.read_text()
s = s.replace('defp deps do\n      []', 'defp deps do\n      [{:protobuf, "0.13.0"}, {:grpc, "1.0.3"}]')
p.write_text(s)
PY
    pushd "$project" >/dev/null
    mix do deps.get, deps.compile
    mix escript.install hex protobuf 0.13.0 --force
    plugin="$HOME/.mix/escripts/protoc-gen-elixir"
    # On Windows the escript launcher is a batch file; protoc must be given
    # that launcher rather than the Unix text escript itself.
    if [[ "${OSTYPE:-}" == msys* || "${OSTYPE:-}" == cygwin* ]]; then
      plugin+=".bat"
      # Git Bash does not report Windows batch launchers as executable even
      # though protoc can invoke them through the Windows command shim.
      test -f "$plugin"
    else
      test -x "$plugin"
    fi
    mkdir -p lib/generated
    protoc -I "$proto_root" -I "$output_root" --plugin="$plugin" \
      --elixir_out=plugins=grpc:"$project/lib/generated" "${contract_protos[@]}"
    mix format --check-formatted
    mix compile --warnings-as-errors
    test -n "$(find lib/generated -type f -name '*.ex' -print -quit)"
    test -n "$(find lib/generated -type f -name '*_grpc.ex' -print -quit)"
    test -n "$(rg -l 'stream' lib/generated --glob '*_grpc.ex' | head -n 1)"
    python3 "$source_root/scripts/verify-generated-rpc-coverage.py" "$project/lib/generated" "${contract_protos[@]}"
    if [[ -n "${ACYCLIC_FIXTURE_GRPC_ENDPOINT:-}" ]]; then
      python3 "$source_root/scripts/write-elixir-runtime-consumer.py" \
        "$project/lib/generated" "${contract_protos[@]}" "$project/runtime_smoke.exs"
      mix run --no-start runtime_smoke.exs "$ACYCLIC_FIXTURE_GRPC_ENDPOINT"
      test -s "$project/runtime_smoke.exs"
      test -s "$project/runtime-consumer-receipt.json"
      rg -q '"source_revision":"[0-9a-f]{40}"' "$project/runtime-consumer-receipt.json"
      require_current_receipt_count "$project/runtime-consumer-receipt.json"
      require_stream_completion "$project/runtime-consumer-receipt.json"
      verify_rust_wire_semantics "$project/runtime-consumer-receipt.json" "$output_root/rust-wire-semantic-verification.json"
    fi
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
    while IFS= read -r contract_proto; do
      relative=${contract_proto#"$product_root/"}
      mkdir -p "$project/proto/$(dirname "$relative")"
      cp "$contract_proto" "$project/proto/$relative"
    done < <(printf '%s\n' "${contract_protos[@]}")
    # The current service inventory intentionally excludes option-only files
    # from its RPC count, but gpb still needs those Rust-owned imports while
    # compiling typed modules (for example validation/v1/options.proto).
    while IFS= read -r dependency_proto; do
      relative=${dependency_proto#"$product_root/"}
      mkdir -p "$project/proto/$(dirname "$relative")"
      cp "$dependency_proto" "$project/proto/$relative"
    done < <(find "$product_root" -type f -name '*.proto' -print)
    # grpcbox_plugin scans one directory level, while the Rust-owned products
    # preserve package directories. Pass every generated proto directory so
    # nested Rust packages are all compiled without flattening their imports.
    proto_dirs=()
    while IFS= read -r contract_proto; do
      relative=${contract_proto#"$product_root/"}
      proto_dir=$(dirname "$relative")
      found=false
      for existing_dir in "${proto_dirs[@]}"; do
        [[ "$existing_dir" == "$proto_dir" ]] && found=true && break
      done
      # grpcbox resolves each configured directory relative to the rebar
      # project root. The copied Rust products live below `proto/`, so keep
      # that prefix in the generated configuration instead of asking the
      # plugin to search a sibling path that cannot contain the sources.
      [[ "$found" == true ]] || proto_dirs+=("proto/$proto_dir")
    done < <(printf '%s\n' "${contract_protos[@]}")
    {
      cat <<'EOF'
{erl_opts, [debug_info]}.
{deps, [{grpcbox, "0.18.0"}]}.
{plugins, [{grpcbox_plugin, "0.9.0"}]}.
{grpc, [{protos, [
EOF
      for proto_index in "${!proto_dirs[@]}"; do
        [[ "$proto_index" -gt 0 ]] && printf ','
        printf '"%s"' "${proto_dirs[$proto_index]}"
      done
      cat <<'EOF'
]}, {gpb_opts, [{module_name_suffix, "_pb"}, maps, {i, "proto"},
  {rename, {msg_fqname, {prefix, {by_proto, [
    {actors, "actors_"}, {filesystem, "filesystem_"}, {harness, "harness_"},
    {inference, "inference_"}, {machines, "machines_"}, {objects, "objects_"},
    {protocol, "protocol_"}, {stream, "stream_"}, {validation, "validation_"},
    {workers, "workers_"}
  ]}}}}
]}]}.
EOF
    } >"$project/rebar.config"
    cat >"$project/src/acyclic_qualification.app.src" <<'EOF'
{application, acyclic_qualification, [{description, "Rust-derived Acyclic qualification"}, {vsn, "0.0.0"}, {applications, [kernel, stdlib, grpcbox]}]}.
EOF
    pushd "$project" >/dev/null
    # grpcbox_plugin compiles each generated protobuf module immediately and
    # writes its beam beside the application. Prime the fresh rebar project so
    # that ebin exists before the plugin starts emitting Rust-owned modules.
    rebar3 compile
    rebar3 grpc gen
    rebar3 compile
    test -n "$(find src -type f -name '*_pb.erl' -print -quit)"
    client_module=$(find src -type f -name '*_client.erl' -print -quit)
    test -n "$client_module"
    test -n "$(rg -l 'stream' src --glob '*_client.erl' | head -n 1)"
    python3 "$source_root/scripts/verify-generated-rpc-coverage.py" "$project/src" "${contract_protos[@]}"
    if [[ -n "${ACYCLIC_FIXTURE_GRPC_ENDPOINT:-}" ]]; then
      fixture_host=${ACYCLIC_FIXTURE_GRPC_ENDPOINT%:*}
      fixture_port=${ACYCLIC_FIXTURE_GRPC_ENDPOINT##*:}
      mkdir -p config
      cat >config/sys.config <<EOF
[{grpcbox,[{client,#{channels=>[{default_channel,[{http,"$fixture_host",$fixture_port,[]}],#{}}]}}]}].
EOF
      python3 "$source_root/scripts/write-erlang-runtime-consumer.py" \
        "$project/src" "${contract_protos[@]}" "$project/src/runtime_smoke.erl"
      rebar3 compile
      erl -noshell -config config/sys -pa _build/default/lib/*/ebin \
        -eval 'case runtime_smoke:run() of ok -> halt(0); _ -> halt(1) end.'
      test -s "$project/src/runtime_smoke.erl"
      test -s "$project/runtime-consumer-receipt.json"
      rg -q '"source_revision":"[0-9a-f]{40}"' "$project/runtime-consumer-receipt.json"
      require_current_receipt_count "$project/runtime-consumer-receipt.json"
      require_stream_completion "$project/runtime-consumer-receipt.json"
      verify_rust_wire_semantics "$project/runtime-consumer-receipt.json" "$output_root/rust-wire-semantic-verification.json"
    fi
    module=$(basename "$(find src -type f -name 'actors_pb.erl' -print -quit)" .erl)
    test -n "$module"
    erl -noshell -pa _build/default/lib/*/ebin -eval "M=$module, [N|_] = M:get_msg_names(), B = M:encode_msg(#{}, N), _ = M:decode_msg(B, N), halt()."
    popd >/dev/null
    archive_project "$project" acyclic_sdk_erlang.tar.gz
    ;;
  ocaml)
    project="$output_root/ocaml"
    mkdir -p "$project/proto" "$project/lib"
    opam install --yes ocaml-protoc-plugin.6.2.0 grpc.0.2.0
    plugin="$(command -v protoc-gen-ocaml || true)"
    test -n "$plugin"
    protoc -I "$proto_root" -I "$output_root" --plugin="$plugin" --ocaml_opt=prefix_output_with_package=true --ocaml_out="$project/lib" "${contract_protos[@]}"
    cat >"$project/dune-project" <<'EOF'
(lang dune 3.7)
(name acyclic_qualification)
EOF
    cat >"$project/lib/dune" <<'EOF'
(library (name acyclic_qualification) (wrapped false)
 (libraries ocaml-protoc-plugin ocaml-protoc-plugin.google_types))

(executable (name serialization_smoke) (libraries acyclic_qualification ocaml-protoc-plugin ocaml-protoc-plugin.google_types))
EOF
    cat >"$project/lib/serialization_smoke.ml" <<'EOF'
let () =
  let value = Acyclic.Objects.V2.MutationIdentity.make ~idempotency_key:"rust-owned" () in
  let encoded = Acyclic.Objects.V2.MutationIdentity.to_proto value |> Protobuf.Writer.contents in
  match Acyclic.Objects.V2.MutationIdentity.from_proto (Protobuf.Reader.create encoded) with
  | Ok decoded when decoded = value -> print_endline "protobuf round trip passed"
  | Ok _ -> failwith "protobuf round trip changed the message"
  | Error _ -> failwith "protobuf round trip could not decode"
EOF
    (cd "$project" && dune build)
    (cd "$project" && dune exec ./lib/serialization_smoke.exe)
    test -n "$(rg -l 'QualificationStream|stream' "$project/lib" --glob '*.ml' | head -n 1)"
    python3 "$source_root/scripts/verify-generated-rpc-coverage.py" "$project/lib" "${contract_protos[@]}"
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
    while IFS= read -r contract_proto; do
      relative=${contract_proto#"$product_root/"}
      generated="$project/generated/${relative%.proto}.lisp"
      mkdir -p "$(dirname "$generated")"
      "$project/ag-protoc" -o "$generated" "$contract_proto"
    done < <(printf '%s\n' "${contract_protos[@]}")
    generated_sources=$(find "$project/generated" -type f -name '*.lisp' -print | LC_ALL=C sort)
    test -n "$generated_sources"
    generated_args=()
    while IFS= read -r generated_source; do generated_args+=(--load "$generated_source"); done <<< "$generated_sources"
    sbcl --non-interactive "${generated_args[@]}" \
      --eval '(unless (find-package :acyclic.actors.v1) (error "generated Actors package missing"))' \
      --eval '(format t "generated Common Lisp package loaded~%")'
    test -n "$(rg -i 'stream|upload|put_object' "$project/generated" --glob '*.lisp' | head -n 1)"
    python3 "$source_root/scripts/verify-generated-rpc-coverage.py" "$project/generated" "${contract_protos[@]}"
    if [[ -n "${ACYCLIC_FIXTURE_GRPC_ENDPOINT:-}" ]]; then
      python3 "$source_root/scripts/write-common-lisp-runtime-consumer.py" \
        "$project/generated" "${contract_protos[@]}" "$project/runtime_smoke.lisp"
      ACYCLIC_RUNTIME_RECEIPT="$project/runtime-consumer-receipt.sexp" \
      ACYCLIC_RUNTIME_OBSERVATION_DIR="$project/runtime-observations" \
      sbcl --non-interactive "${generated_args[@]}" \
        --load "$project/runtime_smoke.lisp" --eval '(format t "Common Lisp Rust fixture consumer passed~%")'
      test -s "$project/runtime_smoke.lisp"
      test -s "$project/runtime-consumer-receipt.sexp"
      rg -q ':source-revision "[0-9a-f]{40}"' "$project/runtime-consumer-receipt.sexp"
      python3 "$source_root/scripts/collect-runtime-observation-receipt.py" \
        "$project" "$project/runtime-consumer-receipt.json" "$source_revision" "$manifest_digest"
      test -s "$project/runtime-consumer-receipt.json"
      rg -q '"source_revision": "[0-9a-f]{40}"' "$project/runtime-consumer-receipt.json"
      require_current_receipt_count "$project/runtime-consumer-receipt.json"
      require_stream_completion "$project/runtime-consumer-receipt.json"
      verify_rust_wire_semantics "$project/runtime-consumer-receipt.json" "$output_root/rust-wire-semantic-verification.json"
    fi
    archive_project "$project" acyclic_sdk_common_lisp.tar.gz
    ;;
  lua-remote)
    bash "$source_root/scripts/qualify-luajit-ffi-remote.sh" "$source_root" "$output_root"
    exit 0
    ;;
  *)
    echo "unsupported qualification target: $language" >&2
    exit 2
    ;;
esac

run_runtime_probe
write_receipt passed
