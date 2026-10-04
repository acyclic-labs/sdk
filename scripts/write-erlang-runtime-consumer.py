#!/usr/bin/env python3
"""Render a grpcbox consumer from the Rust-owned protobuf products.

The receipt hashes the protobuf bytes produced by the generated gpb modules
and the bytes re-encoded from each observed response. Streaming calls stay
deferred unless a Rust-owned scenario allow-list enables the RPC.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path


def snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def main() -> int:
    if len(sys.argv) < 4:
        print("usage: write-erlang-runtime-consumer.py GENERATED_ROOT PROTO... OUTPUT", file=sys.stderr)
        return 2
    output = Path(sys.argv[-1])
    proto_paths = [Path(path) for path in sys.argv[2:-1]]
    service_pattern = re.compile(r"\bservice\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
    rpc_pattern = re.compile(
        r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?"
        r"([.A-Za-z_][A-Za-z0-9_]*)\s*\)\s*returns\s*\(\s*(stream\s+)?"
        r"([.A-Za-z_][A-Za-z0-9_]*)\s*\)", re.S)
    calls = []
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][A-Za-z0-9_]*)\s*;", source)
        package = package_match.group(1) if package_match else ""
        package_prefix = "_".join(part.lower() for part in package.split("."))
        pb_module = f"{snake(proto_path.stem)}_pb"
        for service_match in service_pattern.finditer(source):
            depth = 1
            cursor = service_match.end()
            while depth and cursor < len(source):
                if source[cursor] == "{": depth += 1
                elif source[cursor] == "}": depth -= 1
                cursor += 1
            body = source[service_match.end():cursor - 1]
            module = "_".join(part for part in (package_prefix, snake(service_match.group(1)), "client") if part)
            for rpc in rpc_pattern.finditer(body):
                name, client_stream, request, server_stream, response = rpc.groups()
                shape = "bidi" if client_stream and server_stream else ("client_stream" if client_stream else "server_stream" if server_stream else "unary")
                request_type = request.lstrip(".").split(".")[-1]
                response_type = response.lstrip(".").split(".")[-1]
                rpc_name = f"{package}.{service_match.group(1)}/{name}" if package else f"{service_match.group(1)}/{name}"
                calls.append((module, snake(name), shape, pb_module, request_type, response_type, rpc_name))
    if len(calls) != 106:
        raise SystemExit(f"Rust proto inventory produced {len(calls)} Erlang calls; expected 106")
    lines = [
        "-module(runtime_smoke).", "-export([run/0]).", "",
        "run() ->",
        "    {ok, _} = application:ensure_all_started(grpcbox),",
        "    {ok, _} = application:ensure_all_started(crypto),",
        "    Results = [call(Module, Method, Shape, Pb, Req, Resp, Rpc) || {Module, Method, Shape, Pb, Req, Resp, Rpc} <- calls()],",
        "    write_receipt(Results),",
        "    case lists:any(fun(#{execution := executed, status := Status}) when Status =/= 0 -> true; (_) -> false end, Results) of",
        "        true -> io:format(standard_error, \"Rust fixture RPC failures recorded in runtime-consumer-receipt.json~n\"), {failed, Results};",
        "        false -> io:format(\"Erlang generated stubs recorded all 106 Rust RPC observations~n\"), ok",
        "    end.", "",
        "call(Module, Method, Shape, Pb, Req, Resp, Rpc) ->",
        "    Request = #{}, RequestBytes = Pb:encode_msg(Request, list_to_atom(Req)),",
        "    case {Shape, stream_allowed(Rpc)} of",
        "        {Shape, false} when Shape =/= unary -> observation(Rpc, Shape, RequestBytes, [], undefined, deferred, undefined);",
        "        {unary, _} -> expect_unary(Module, Method, Pb, Resp, Rpc, RequestBytes);",
        "        {server_stream, _} -> expect_server_stream(Module, Method, Pb, Resp, Rpc, RequestBytes);",
        "        {client_stream, _} -> expect_client_stream(Module, Method, Pb, Resp, Rpc, RequestBytes);",
        "        {bidi, _} -> expect_bidi(Module, Method, Pb, Resp, Rpc, RequestBytes)",
        "    end.", "",
        "expect_unary(Module, Method, Pb, Resp, Rpc, RequestBytes) ->",
        "    case apply(Module, Method, [#{}]) of",
        "        {ok, Response, _Headers} -> observation(Rpc, unary, RequestBytes, [Pb:encode_msg(Response, list_to_atom(Resp))], 0, executed, undefined);",
        "        {error, Error} -> observation(Rpc, unary, RequestBytes, [], status(Error), executed, Error);",
        "        Other -> observation(Rpc, unary, RequestBytes, [], status(Other), executed, Other)",
        "    end.", "",
        "expect_server_stream(Module, Method, Pb, Resp, Rpc, RequestBytes) ->",
        "    case apply(Module, Method, [#{}]) of",
        "        {ok, Stream} -> Responses = receive_stream(Stream, Pb, Resp, []), observation(Rpc, server_stream, RequestBytes, Responses, 0, executed, undefined);",
        "        {error, Error} -> observation(Rpc, server_stream, RequestBytes, [], status(Error), executed, Error);",
        "        Other -> observation(Rpc, server_stream, RequestBytes, [], status(Other), executed, Other)",
        "    end.", "",
        "expect_client_stream(Module, Method, Pb, Resp, Rpc, RequestBytes) ->",
        "    case apply(Module, Method, []) of",
        "        {ok, Stream} -> ok = grpcbox_client:send(Stream, #{}), ok = grpcbox_client:close_send(Stream), Responses = receive_stream(Stream, Pb, Resp, []), observation(Rpc, client_stream, RequestBytes, Responses, 0, executed, undefined);",
        "        {error, Error} -> observation(Rpc, client_stream, RequestBytes, [], status(Error), executed, Error);",
        "        Other -> observation(Rpc, client_stream, RequestBytes, [], status(Other), executed, Other)",
        "    end.", "",
        "expect_bidi(Module, Method, Pb, Resp, Rpc, RequestBytes) ->",
        "    case apply(Module, Method, []) of",
        "        {ok, Stream} -> ok = grpcbox_client:send(Stream, #{}), ok = grpcbox_client:close_send(Stream), Responses = receive_stream(Stream, Pb, Resp, []), observation(Rpc, bidi, RequestBytes, Responses, 0, executed, undefined);",
        "        {error, Error} -> observation(Rpc, bidi, RequestBytes, [], status(Error), executed, Error);",
        "        Other -> observation(Rpc, bidi, RequestBytes, [], status(Other), executed, Other)",
        "    end.", "",
        "receive_stream(Stream, Pb, Resp, Acc) ->",
        "    case grpcbox_client:recv_data(Stream) of",
        "        {ok, Response} -> receive_stream(Stream, Pb, Resp, [Pb:encode_msg(Response, list_to_atom(Resp)) | Acc]);",
        "        {error, closed} -> lists:reverse(Acc);",
        "        {error, _} -> lists:reverse(Acc);",
        "        _ -> lists:reverse(Acc)",
        "    end.", "",
        "observation(Rpc, Shape, RequestBytes, ResponseBytes, Status, Execution, Error) ->",
        "    #{family => hd(string:split(Rpc, \".\", all)), rpc => Rpc, shape => Shape, request_base64 => binary_to_list(base64:encode(RequestBytes)), request_frames_base64 => [binary_to_list(base64:encode(RequestBytes))], request_sha256 => hash_hex(RequestBytes), response_sha256 => first_hash(ResponseBytes), response_frames => length(ResponseBytes), response_frame_base64 => [binary_to_list(base64:encode(B)) || B <- ResponseBytes], response_frame_sha256 => [hash_hex(B) || B <- ResponseBytes], status => Status, terminal_status => terminal_status(Status, Execution), terminal_code => Status, execution => Execution, error => Error}.",
        "hash_hex(Bytes) -> lists:flatten([io_lib:format(\"~2.16.0B\", [Byte]) || <<Byte:8>> <= crypto:hash(sha256, Bytes)]).",
        "first_hash([]) -> undefined; first_hash([Bytes | _]) -> hash_hex(Bytes).",
        "status(#{status := Value}) when is_integer(Value) -> Value; status(#{grpc_status := Value}) when is_integer(Value) -> Value; status(_) -> undefined.",
        "terminal_status(_Status, deferred) -> deferred; terminal_status(0, _) -> ok; terminal_status(Status, _) when is_integer(Status) -> error; terminal_status(_, _) -> unknown.",
        "stream_allowed(Rpc) -> case os:getenv(\"ACYCLIC_STREAM_SCENARIO_RPCS\") of false -> false; Value -> lists:member(Rpc, string:lexemes(Value, \",\")) end.",
        "json_string(undefined) -> \"null\"; json_string(Value) when is_atom(Value) -> json_string(atom_to_list(Value)); json_string(Value) when is_integer(Value) -> integer_to_list(Value); json_string(Value) when is_list(Value) -> [\"\\\"\", Value, \"\\\"\"]; json_string(Value) -> json_string(io_lib:print(Value)).",
        "json_list(Values) -> [\"[\", string:join([json_string(Value) || Value <- Values], \",\"), \"]\"].",
        "json_observation(#{family := Family, rpc := Rpc, shape := Shape, request_base64 := RequestBase64, request_frames_base64 := RequestFrames, request_sha256 := Req, response_sha256 := Response, response_frames := Frames, response_frame_base64 := FrameBytes, response_frame_sha256 := FrameHashes, status := Status, terminal_status := Terminal, terminal_code := TerminalCode, execution := Execution, error := Error}) -> io_lib:format(\"{\\\"family\\\":~s,\\\"rpc\\\":~s,\\\"shape\\\":~s,\\\"execution\\\":~s,\\\"status\\\":~s,\\\"terminal_status\\\":~s,\\\"terminal_code\\\":~s,\\\"request_base64\\\":~s,\\\"request_frames_base64\\\":~s,\\\"request_sha256\\\":~s,\\\"response_sha256\\\":~s,\\\"response_frame_count\\\":~p,\\\"response_frame_base64\\\":~s,\\\"response_frame_sha256\\\":~s,\\\"error\\\":~s}\", [json_string(Family), json_string(Rpc), json_string(Shape), json_string(Execution), json_string(Status), json_string(Terminal), json_string(TerminalCode), json_string(RequestBase64), json_list(RequestFrames), json_string(Req), json_string(Response), Frames, json_list(FrameBytes), json_list(FrameHashes), json_string(Error)]).",
        "write_receipt(Results) -> Source = case os:getenv(\"ACYCLIC_RUST_SOURCE_REVISION\") of false -> \"unknown\"; Value -> Value end, Manifest = case os:getenv(\"ACYCLIC_RUST_AUTHORITY_MANIFEST_SHA256\") of false -> \"unknown\"; Value -> Value end, Body = string:join([lists:flatten(json_observation(Result)) || Result <- Results], \",\"), Json = io_lib:format(\"{\\\"schema\\\":\\\"acyclic.runtime-consumer-receipt.v1\\\",\\\"language\\\":\\\"erlang\\\",\\\"source_revision\\\":~s,\\\"rust_authority_manifest_sha256\\\":~s,\\\"observations\\\":[~s],\\\"rpc_count\\\":~p}\", [json_string(Source), json_string(Manifest), Body, length(Results)]), ok = file:write_file(\"runtime-consumer-receipt.json\", iolist_to_binary(Json)).",
        "calls() -> [",
        *[f"    {{{module}, {method}, {shape}, {pb}, \"{req}\", \"{resp}\", \"{rpc}\"}}{',' if index + 1 < len(calls) else ''}" for index, (module, method, shape, pb, req, resp, rpc) in enumerate(calls)],
        "] .",
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
