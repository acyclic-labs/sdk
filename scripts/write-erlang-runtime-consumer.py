#!/usr/bin/env python3
"""Render an Erlang grpcbox consumer from Rust-owned protobuf services."""
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
    rpc_pattern = re.compile(r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*(stream\s+)?[.A-Za-z_][.A-Za-z0-9_]*\s*\)\s*returns\s*\(\s*(stream\s+)?[.A-Za-z_][.A-Za-z0-9_]*\s*\)", re.S)
    calls = []
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][.A-Za-z0-9_]*)\s*;", source)
        package = package_match.group(1) if package_match else ""
        package_prefix = "_".join(part.lower() for part in package.split("."))
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
                name, client_stream, server_stream = rpc.groups()
                shape = "bidi" if client_stream and server_stream else ("client_stream" if client_stream else "server_stream" if server_stream else "unary")
                calls.append((module, snake(name), shape))
    if len(calls) != 106:
        raise SystemExit(f"Rust proto inventory produced {len(calls)} Erlang calls; expected 106")
    lines = [
        "-module(runtime_smoke).", "-export([run/0]).", "", "run() ->",
        "    {ok, _} = application:ensure_all_started(grpcbox),",
        "    Results = [call(Module, Method, Shape) || {Module, Method, Shape} <- calls()],",
        "    case lists:all(fun(ok) -> true; (_) -> false end, Results) of",
        "        true -> io:format(\"Erlang generated stubs invoked all 106 Rust RPCs~n\"), ok;",
        "        false -> io:format(standard_error, \"Erlang generated stub call failed: ~p~n\", [Results]), halt(1)",
        "    end.", "",
        "call(Module, Method, unary) -> expect_unary(apply(Module, Method, [#{}]));",
        "call(Module, Method, server_stream) ->",
        "    case apply(Module, Method, [#{}]) of",
        "        {ok, Stream} -> _ = grpcbox_client:recv_headers(Stream), _ = grpcbox_client:recv_data(Stream), _ = grpcbox_client:recv_trailers(Stream), ok;",
        "        Other -> {failed, Module, Method, Other}", "    end;",
        "call(Module, Method, client_stream) ->",
        "    case apply(Module, Method, []) of",
        "        {ok, Stream} -> ok = grpcbox_client:send(Stream, #{}), ok = grpcbox_client:close_send(Stream), _ = grpcbox_client:recv_data(Stream), ok;",
        "        Other -> {failed, Module, Method, Other}", "    end;",
        "call(Module, Method, bidi) ->",
        "    case apply(Module, Method, []) of",
        "        {ok, Stream} -> ok = grpcbox_client:send(Stream, #{}), _ = grpcbox_client:recv_data(Stream), _ = grpcbox_client:close_and_recv(Stream), ok;",
        "        Other -> {failed, Module, Method, Other}", "    end.", "",
        "expect_unary({ok, _Response, _Headers}) -> ok;", "expect_unary(Other) -> {failed, unary, Other}.", "",
        "calls() -> [",
        *[
            f"    {{{module}, {method}, {shape}}}{',' if index + 1 < len(calls) else ''}"
            for index, (module, method, shape) in enumerate(calls)
        ],
        "] .",
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
