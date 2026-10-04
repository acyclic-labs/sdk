#!/usr/bin/env python3
"""Render an Elixir consumer directly from Rust-owned protobuf services."""
from __future__ import annotations

import re
import sys
from pathlib import Path


def module_name(value: str) -> str:
    return ".".join(part[:1].upper() + part[1:] for part in value.lstrip(".").split("."))


def snake(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", "_", value).lower()


def main() -> int:
    if len(sys.argv) < 4:
        print("usage: write-elixir-runtime-consumer.py GENERATED_ROOT PROTO... OUTPUT", file=sys.stderr)
        return 2
    output = Path(sys.argv[-1])
    proto_paths = [Path(path) for path in sys.argv[2:-1]]
    calls: list[tuple[str, str, str, str]] = []
    rpc_pattern = re.compile(
        r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*"
        r"(stream\s+)?([.A-Za-z_][.A-Za-z0-9_]*)\s*\)\s*returns\s*\(\s*"
        r"(stream\s+)?([.A-Za-z_][.A-Za-z0-9_]*)\s*\)",
        re.S,
    )
    service_pattern = re.compile(r"\bservice\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][.A-Za-z0-9_]*)\s*;", source)
        package = package_match.group(1) if package_match else ""
        package_module = module_name(package)
        for service_match in service_pattern.finditer(source):
            depth = 1
            cursor = service_match.end()
            while depth and cursor < len(source):
                if source[cursor] == "{":
                    depth += 1
                elif source[cursor] == "}":
                    depth -= 1
                cursor += 1
            service_body = source[service_match.end():cursor - 1]
            service = service_match.group(1)
            service_module = ".".join(part for part in (package_module, service, "Stub") if part)
            for rpc in rpc_pattern.finditer(service_body):
                name, client_stream, request, server_stream, _response = rpc.groups()
                request_name = request if "." in request else ".".join(part for part in (package, request) if part)
                request_module = module_name(request_name)
                shape = "bidi" if client_stream and server_stream else (
                    "client_stream" if client_stream else "server_stream" if server_stream else "unary"
                )
                calls.append((service_module, snake(name), request_module, shape))
    if len(calls) != 106:
        raise SystemExit(f"Rust proto inventory produced {len(calls)} Elixir calls; expected 106")
    lines = [
        "calls = [",
        *[
            f"  {{{stub}, :{method}, {request}, :{shape}}},"
            for stub, method, request, shape in calls
        ],
        "]",
        "endpoint = List.first(System.argv) || System.get_env(\"ACYCLIC_FIXTURE_GRPC_ENDPOINT\") || \"127.0.0.1:50051\"",
        "{:ok, _supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one, name: GRPC.Client.Supervisor)",
        "{:ok, channel} = GRPC.Stub.connect(endpoint)",
        "try do",
        "  Enum.each(calls, fn {stub, method, request_module, shape} ->",
        "    request = if function_exported?(request_module, :new, 1), do: apply(request_module, :new, [[]]), else: struct(request_module)",
        "    input = if shape in [:client_stream, :bidi], do: Stream.map([request], & &1), else: request",
        "    result = apply(stub, method, [channel, input, []])",
        "    case {shape, result} do",
        "      {shape, {:ok, stream}} when shape in [:server_stream, :bidi] ->",
        "        stream |> Enum.take(1) |> Enum.to_list()",
        "      {_, {:ok, _reply}} -> :ok",
        "      {_, other} -> raise \"Rust fixture RPC failed: #{inspect(stub)}.#{method}: #{inspect(other)}\"",
        "    end",
        "  end)",
        "  IO.puts(\"Elixir generated stubs invoked all 106 Rust RPCs\")",
        "after",
        "  GRPC.Stub.disconnect(channel)",
        "end",
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())