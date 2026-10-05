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
    command -v alr >/dev/null 2>&1 || { echo 'Alire is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name '*.gpr' -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Ada project files were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      project_dir="$(dirname "$project")"
      run_logged "ada-$(basename "${project%.gpr}")" bash -c "cd \"$project_dir\" && alr exec -- gprbuild -p -P \"$project\""
    done
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      # Keep the executable source beside the generated package.  Its request
      # record, route, JSON codec, response record, and AWS transport all come
      # from the Rust-derived Ada projection and compatibility adapter.
      actor_project="$(dirname "${projects[0]}")"
      transport_dir="$output_root/ada-transport"
      mkdir -p "$transport_dir"
      cat >"$actor_project/src/qualification.adb" <<'EOF'
with Ada.Environment_Variables;
with AcyclicActors;
with AcyclicActors.Clients;
with AcyclicActors.Models;

procedure Qualification is
   Client : AcyclicActors.Clients.Client_Type;
   Request : AcyclicActors.Models.AcyclicActorsV1CreateActorRequest_Type;
   Result : AcyclicActors.Models.AcyclicActorsV1CreateActorResponse_Type;
begin
   AcyclicActors.Set_Server
     (AcyclicActors.Client_Base_Type (Client),
      AcyclicActors.To_UString
        (Ada.Environment_Variables.Value ("ACYCLIC_FIXTURE_HTTP_ENDPOINT")));
   Request.Code_Sha_256 := AcyclicActors.To_UString
     ("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=");
   Request.Home_Region.Present := True;
   Request.Home_Region.Value := AcyclicActors.To_UString ("qualification");
   Request.Idempotency_Key.Present := True;
   Request.Idempotency_Key.Value := AcyclicActors.To_UString
     ("http-target-ada-qualification");
   Request.Limits.Checkpoint_Bytes.Present := True;
   Request.Limits.Checkpoint_Bytes.Value := AcyclicActors.To_UString ("1048576");
   Request.Limits.Handler_Timeout_Millis.Present := True;
   Request.Limits.Handler_Timeout_Millis.Value := AcyclicActors.To_UString ("1000");
   Request.Limits.Memory_Bytes.Present := True;
   Request.Limits.Memory_Bytes.Value := AcyclicActors.To_UString ("1048576");
   AcyclicActors.Clients.Create_Actor (Client, Request, Result);
   if not Result.Actor.Actor_Id.Present then
      raise Program_Error with "generated Ada response did not decode actor_id";
   end if;
   declare
      Inspect_Request : AcyclicActors.Models.AcyclicActorsV1InspectActorRequest_Type;
      Inspect_Result : AcyclicActors.Models.AcyclicActorsV1InspectActorResponse_Type;
   begin
      Inspect_Request.Actor_Id := Result.Actor.Actor_Id;
      AcyclicActors.Clients.Inspect_Actor (Client, Inspect_Request, Inspect_Result);
      if not Inspect_Result.Actor.Actor_Id.Present then
         raise Program_Error with "generated Ada inspect response did not decode actor_id";
      end if;
   end;
end Qualification;
EOF
      cat >"$actor_project/qualification.gpr" <<EOF
with "$(basename "${projects[0]}")";
project Qualification is
   for Source_Dirs use ("src");
   for Main use ("qualification.adb");
end Qualification;
EOF
      run_logged ada-transport-build bash -c "cd \"$actor_project\" && alr exec -- gprbuild -p -P qualification.gpr"
      run_logged ada-transport env ACYCLIC_FIXTURE_HTTP_ENDPOINT="$ACYCLIC_FIXTURE_HTTP_ENDPOINT" "$actor_project/bin/qualification"
      cp "$actor_project/src/qualification.adb" "$transport_dir/qualification.adb"
      client_transport='ada-generated-client-aws-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
    runtime='ada/alire-aws-gprbuild'
    ;;
  clojure)
    command -v lein >/dev/null 2>&1 || { echo 'Leiningen is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name project.clj -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Clojure projects were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      run_logged "clojure-$(basename "$(dirname "$project")")" lein -f "$project" check
    done
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      # Keep the call in the generated Clojure namespace: the request shape,
      # route, JSON serialization, and response decoding are all supplied by
      # the Rust-derived OpenAPI package.
      transport_dir="$output_root/clojure-transport"
      mkdir -p "$transport_dir"
      project_root="$(dirname "${projects[0]}")"
      mkdir -p "$project_root/src"
      cat >"$project_root/src/acyclic_actors_qualification.clj" <<'EOF'
(ns acyclic-actors-qualification
  (:require [acyclic-actors-api.core :refer [with-api-context]]
            [acyclic-actors-api.api.default :refer [create-actor-with-http-info inspect-actor-with-http-info]]))

(defn -main [& _]
  (let [endpoint (System/getenv "ACYCLIC_FIXTURE_HTTP_ENDPOINT")
        request {:codeSha256 "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
                 :homeRegion "qualification"
                 :idempotencyKey "http-target-clojure-qualification"
                 :limits {:checkpointBytes "1048576"
                          :handlerTimeoutMillis "1000"
                          :memoryBytes "1048576"}}
        response (with-api-context {:base-url endpoint}
                   (create-actor-with-http-info request))]
    (when-not (<= 200 (:status response) 299)
      (throw (ex-info "Rust fixture rejected generated Clojure request" {:status (:status response)})))
    (when-not (map? (:data response))
      (throw (ex-info "generated Clojure client did not decode the Rust fixture response" {})))
    (let [actor-id (or (get-in (:data response) [:actor :actorId])
                       (get-in (:data response) [:actor :actor-id]))
          inspect-response (with-api-context {:base-url endpoint}
                             (inspect-actor-with-http-info {:actorId actor-id}))]
      (when-not (string? actor-id)
        (throw (ex-info "generated Clojure client did not decode actor_id" {})))
      (when-not (<= 200 (:status inspect-response) 299)
        (throw (ex-info "Rust fixture rejected generated Clojure inspect request" {:status (:status inspect-response)})))
      (when-not (map? (:data inspect-response))
        (throw (ex-info "generated Clojure client did not decode the inspect response" {})))))
EOF
      cp "$project_root/src/acyclic_actors_qualification.clj" "$transport_dir/acyclic_actors_qualification.clj"
      run_logged clojure-transport env ACYCLIC_FIXTURE_HTTP_ENDPOINT="$ACYCLIC_FIXTURE_HTTP_ENDPOINT" lein -f "${projects[0]}" run -m acyclic-actors-qualification
      client_transport='clojure-generated-client-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
    runtime='clojure/leiningen'
    ;;
  crystal)
    command -v shards >/dev/null 2>&1 || { echo 'Shards is required' >&2; exit 2; }
    mapfile -t projects < <(find "$package_root" -name shard.yml -type f | sort)
    ((${#projects[@]} > 0)) || { echo 'no Crystal shards were generated' >&2; exit 1; }
    for project in "${projects[@]}"; do
      run_logged "crystal-$(basename "$(dirname "$project")")" bash -c "cd \"$(dirname \"$project\")\" && shards build"
    done
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      # Exercise the generated Crystal API against the Rust fixture. The
      # request model, JSON body, route, and response model all come from the
      # generated package.
      transport_dir="$output_root/crystal-transport"
      mkdir -p "$transport_dir"
      crystal_root="$(dirname "${projects[0]}")"
      cat >"$crystal_root/qualification.cr" <<'EOF'
require "./src/acyclic_actors_crystal"

endpoint = ENV.fetch("ACYCLIC_FIXTURE_HTTP_ENDPOINT")
config = AcyclicActorsHttp::Configuration.new
config.scheme = "http"
config.host = endpoint
client = AcyclicActorsHttp::Client.new(AcyclicActorsHttp::Connection.new(config))
request = AcyclicActorsHttp::AcyclicActorsV1CreateActorRequest.new(
  code_sha256: "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
  home_region: "qualification",
  idempotency_key: "http-target-crystal-qualification",
  limits: AcyclicActorsHttp::AcyclicActorsV1ActorLimits.new(
    checkpoint_bytes: "1048576",
    handler_timeout_millis: "1000",
    memory_bytes: "1048576"))
response = client.create.actor(request)
abort "Rust fixture rejected generated Crystal request: #{response.status}" unless response.success?
abort "generated Crystal client did not decode the Rust fixture response" if response.value.actor.nil?
inspect_request = AcyclicActorsHttp::AcyclicActorsV1InspectActorRequest.new(
  actor_id: response.value.actor.actor_id)
inspect_response = client.inspect.actor(inspect_request)
abort "Rust fixture rejected generated Crystal inspect request: #{inspect_response.status}" unless inspect_response.success?
abort "generated Crystal client did not decode the inspect response" if inspect_response.value.actor.nil?
EOF
      cp "$crystal_root/qualification.cr" "$transport_dir/qualification.cr"
      run_logged crystal-transport bash -c "cd \"$crystal_root\" && ACYCLIC_FIXTURE_HTTP_ENDPOINT=\"$ACYCLIC_FIXTURE_HTTP_ENDPOINT\" crystal run --path lib qualification.cr"
      client_transport='crystal-generated-client-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
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
import Api
import Api.Data exposing (..)
import Browser
import Html exposing (Html, div, text)
import Html.Attributes exposing (attribute, id)
import Http

type alias Flags =
    { basePath : String
    , token : String
    }

type Model
    = Loading Flags
    | Inspecting
    | Passed
    | Failed String

type Msg
    = Created (Result Http.Error AcyclicActorsV1CreateActorResponse)
    | Inspected (Result Http.Error AcyclicActorsV1InspectActorResponse)

main : Program Flags Model Msg
main =
    Browser.element
        { init = init
        , update = update
        , subscriptions = \_ -> Sub.none
        , view = view
        }

init : Flags -> ( Model, Cmd Msg )
init flags =
    ( Loading flags
    , Api.send Created
        (Api.withBasePath flags.basePath
            (Api.Request.Default.createActor request flags.token))
    )

request : AcyclicActorsV1CreateActorRequest
request =
    { bindings = Nothing
    , codeSha256 = Just "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
    , homeRegion = Just "qualification"
    , idempotencyKey = Just "http-target-elm-qualification"
    , limits = Just
        { checkpointBytes = Just "1048576"
        , handlerTimeoutMillis = Just "1000"
        , memoryBytes = Just "1048576"
        }
    , subscriptions = Nothing
    }

update : Msg -> Model -> ( Model, Cmd Msg )
update msg model =
    case msg of
        Created (Ok response) ->
            case ( model, response.actor ) of
                ( Loading flags, Just actor ) ->
                    ( Inspecting
                    , Api.send Inspected
                        (Api.withBasePath flags.basePath
                            (Api.Request.Default.inspectActor
                                { actorId = actor.actorId }
                                flags.token)) )
                ( _, Nothing ) -> ( Failed "generated Elm client decoded an empty actor response", Cmd.none )
                _ -> ( Failed "generated Elm client returned an actor response in an invalid state", Cmd.none )

        Created (Err error) ->
            ( Failed (Debug.toString error), Cmd.none )

        Inspected (Ok response) ->
            case response.actor of
                Just _ -> ( Passed, Cmd.none )
                Nothing -> ( Failed "generated Elm client decoded an empty inspect response", Cmd.none )

        Inspected (Err error) ->
            ( Failed (Debug.toString error), Cmd.none )

view : Model -> Html Msg
view model =
    case model of
        Loading _ -> div [ id "elm-qualification" ] [ text "loading" ]
        Inspecting -> div [ id "elm-qualification" ] [ text "inspecting" ]
        Passed -> div [ id "elm-qualification", attribute "data-result" "passed" ] [ text "passed" ]
        Failed detail -> div [ id "elm-qualification", attribute "data-result" "failed" ] [ text detail ]
EOF
      run_logged "elm-$(basename "$dir")" bash -c "cd \"$dir\" && elm make src/QualificationMain.elm --output=qualification.js"
      if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
        run_logged "elm-browser-$(basename "$dir")" node "$PWD/scripts/qualify-elm-browser.mjs" "$dir" "$ACYCLIC_FIXTURE_HTTP_ENDPOINT"
      fi
    done
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      client_transport='elm-generated-client-browser-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
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
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      actors_root="$package_root/actors"
      [[ -d "$actors_root" ]] || { echo 'generated Actors GDScript package is missing' >&2; exit 1; }
      transport_dir="$output_root/gdscript-transport"
      mkdir -p "$transport_dir"
      cat >"$actors_root/qualification-transport.gd" <<'EOF'
extends SceneTree

var api

func _init() -> void:
    var endpoint := OS.get_environment("ACYCLIC_FIXTURE_HTTP_ENDPOINT")
    var host_and_port := endpoint.trim_prefix("http://").trim_prefix("https://")
    var separator := host_and_port.rfind(":")
    var host := host_and_port
    var port := 80
    if separator > 0:
        host = host_and_port.substr(0, separator)
        port = int(host_and_port.substr(separator + 1))
    var config := ApiConfig.new()
    config.host = "http://" + host
    config.port = port
    api = DefaultApi.new(config)
    var limits := AcyclicActorsV1ActorLimits.new()
    limits.checkpointBytes = "1048576"
    limits.handlerTimeoutMillis = "1000"
    limits.memoryBytes = "1048576"
    var request := AcyclicActorsV1CreateActorRequest.new()
    request.codeSha256 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE="
    request.homeRegion = "qualification"
    request.idempotencyKey = "http-target-gdscript-qualification"
    request.limits = limits
    api.create_actor(request, Callable(self, "_success"), Callable(self, "_failure"))

func _success(response: ApiResponse) -> void:
    if response.code < 200 or response.code >= 300 or response.data == null or response.data.actor == null:
        push_error("Rust fixture rejected generated GDScript request or response")
        quit(1)
        return
    var inspect_request := AcyclicActorsV1InspectActorRequest.new()
    inspect_request.actorId = response.data.actor.actorId
    api.inspect_actor(inspect_request, Callable(self, "_inspect_success"), Callable(self, "_failure"))

func _inspect_success(response: ApiResponse) -> void:
    if response.code < 200 or response.code >= 300 or response.data == null or response.data.actor == null:
        push_error("Rust fixture rejected generated GDScript inspect request or response")
        quit(1)
        return
    quit(0)

func _failure(error: ApiError) -> void:
    push_error("generated GDScript client transport failed: " + error.message)
    quit(1)
EOF
      cp "$actors_root/qualification-transport.gd" "$transport_dir/qualification-transport.gd"
      run_logged gdscript-transport env ACYCLIC_FIXTURE_HTTP_ENDPOINT="$ACYCLIC_FIXTURE_HTTP_ENDPOINT" godot --headless --path "$actors_root" --script qualification-transport.gd
      client_transport='gdscript-generated-client-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
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
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      # The request and call below are generated-client code. The fixture is
      # Rust-owned, so this proves JSON serialization, route selection, and
      # response decoding without introducing a handwritten protocol client.
      transport_dir="$output_root/nim-transport"
      mkdir -p "$transport_dir"
      cat >"$transport_dir/qualification.nim" <<'EOF'
import httpclient, options
import acyclic_actors_nim
import acyclic_actors_nim/apis/api_default
import acyclic_actors_nim/models/model_acyclic_actors_v1_create_actor_request
import acyclic_actors_nim/models/model_acyclic_actors_v1_actor_limits
import acyclic_actors_nim/models/model_byte_array

let client = newHttpClient()
let request = AcyclicActorsV1CreateActorRequest(
  codeSha256: some(toByteArray("AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=")),
  homeRegion: some("qualification"),
  idempotencyKey: some("http-target-nim-qualification"),
  limits: some(AcyclicActorsV1ActorLimits(
    checkpointBytes: some("1048576"),
    handlerTimeoutMillis: some("1000"),
    memoryBytes: some("1048576"))))
let (decoded, response) = createActor(client, request)
if response.code != Http200:
  quit("Rust fixture rejected generated Nim request: " & $response.status, 1)
if decoded.isNone:
  quit("generated Nim client did not decode the Rust fixture response", 1)
EOF
      run_logged nim-transport env ACYCLIC_BASE_URL="$ACYCLIC_FIXTURE_HTTP_ENDPOINT" nim c -r --hints:off --path:"$package_root/actors" "$transport_dir/qualification.nim"
      client_transport='nim-generated-client-fixture-roundtrip'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
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
    if [[ -n "${ACYCLIC_FIXTURE_HTTP_ENDPOINT:-}" ]]; then
      # Install the generated package and call its generated R6 client. This
      # verifies model serialization, the Rust-owned route, and response
      # deserialization against the Rust fixture.
      transport_dir="$output_root/r-transport"
      library_dir="$transport_dir/library"
      mkdir -p "$library_dir"
      package_dir="$(dirname "${projects[0]}")"
      run_logged r-install R CMD INSTALL --no-multiarch --library="$library_dir" "$package_dir"
      cat >"$transport_dir/qualification.R" <<'EOF'
library(acyclic.actors.r, lib.loc = Sys.getenv("ACYCLIC_R_LIBRARY"))
client <- ApiClient$new(base_path = Sys.getenv("ACYCLIC_FIXTURE_HTTP_ENDPOINT"))
api <- DefaultApi$new(client)
limits <- AcyclicActorsV1ActorLimits$new(
  checkpointBytes = "1048576",
  handlerTimeoutMillis = "1000",
  memoryBytes = "1048576")
request <- AcyclicActorsV1CreateActorRequest$new(
  codeSha256 = "AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE=",
  homeRegion = "qualification",
  idempotencyKey = "http-target-r-qualification",
  limits = limits)
response <- api$CreateActorWithHttpInfo(request)
stopifnot(response$status_code >= 200, response$status_code < 300)
stopifnot(!is.null(response$content))
actor_id <- response$content$actor$actorId
stopifnot(!is.null(actor_id), nzchar(actor_id))
inspect_request <- AcyclicActorsV1InspectActorRequest$new(actorId = actor_id)
inspect_response <- api$InspectActorWithHttpInfo(inspect_request)
stopifnot(inspect_response$status_code >= 200, inspect_response$status_code < 300)
stopifnot(!is.null(inspect_response$content$actor$actorId))
EOF
      run_logged r-transport env ACYCLIC_R_LIBRARY="$library_dir" ACYCLIC_FIXTURE_HTTP_ENDPOINT="$ACYCLIC_FIXTURE_HTTP_ENDPOINT" Rscript "$transport_dir/qualification.R"
      client_transport='r-generated-client-fixture-roundtrip'
      client_operations='actors.create_actor,actors.inspect_actor'
    else
      client_transport='not-run-fixture-endpoint-unset'
    fi
    runtime='R/R CMD check'
    ;;
  *) echo "unsupported HTTP target: $target" >&2; exit 2 ;;
esac

python3 - "$output_root/runtime-qualification.json" "$target" "$runtime" "${ACYCLIC_RUST_SOURCE_REVISION:-unknown}" "$archive" "$archive_sha256" "${client_transport:-not-run}" "${client_operations:-}" <<'PY'
import json, pathlib, sys
path, target, runtime, revision, archive, archive_sha256, client_transport, client_operations = sys.argv[1:]
fixture_roundtrip = client_transport.endswith("fixture-roundtrip")
operations = [item for item in client_operations.split(",") if item]
if fixture_roundtrip and not operations:
    operations = ["actors.create_actor"]
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
    "client_transport": client_transport,
    "semantic_qualification": "fixture-roundtrip" if fixture_roundtrip else "compile-only",
    "qualified_operations": operations if fixture_roundtrip else [],
    "coverage_note": "This receipt proves only the listed generated operation; it does not imply full service or family coverage.",
    "streaming": "not-applicable-to-http-projection",
    "native_grpc": "unqualified",
}
pathlib.Path(path).write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
PY
