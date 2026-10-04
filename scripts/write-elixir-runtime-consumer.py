#!/usr/bin/env python3
"""Render an Elixir consumer from the Rust-owned protobuf products.

The generated program records the bytes it sends and the bytes it observes
after decoding. Streaming RPCs are inventory entries until a Rust-owned
scenario list enables them, so qualification never replays mutating methods
with empty requests.
"""
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
    calls: list[tuple[str, str, str, str, str, str]] = []
    rpc_pattern = re.compile(
        r"\brpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\(\s*"
        r"(stream\s+)?([.A-Za-z_][A-Za-z0-9_]*)\s*\)\s*returns\s*\(\s*"
        r"(stream\s+)?([.A-Za-z_][A-Za-z0-9_]*)\s*\)",
        re.S,
    )
    service_pattern = re.compile(r"\bservice\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{")
    for proto_path in sorted(proto_paths):
        source = proto_path.read_text(encoding="utf-8")
        package_match = re.search(r"\bpackage\s+([.A-Za-z_][A-Za-z0-9_]*)\s*;", source)
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
            service_body = source[service_match.end() : cursor - 1]
            service = service_match.group(1)
            service_module = ".".join(part for part in (package_module, service, "Stub") if part)
            for rpc in rpc_pattern.finditer(service_body):
                name, client_stream, request, server_stream, response = rpc.groups()
                request_name = request if "." in request else ".".join(part for part in (package, request) if part)
                response_name = response if "." in response else ".".join(part for part in (package, response) if part)
                shape = "bidi" if client_stream and server_stream else (
                    "client_stream" if client_stream else "server_stream" if server_stream else "unary"
                )
                rpc_name = f"{package}.{service}/{name}" if package else f"{service}/{name}"
                calls.append((service_module, snake(name), module_name(request_name), module_name(response_name), shape, rpc_name))
    if len(calls) != 106:
        raise SystemExit(f"Rust proto inventory produced {len(calls)} Elixir calls; expected 106")

    lines = [
        "defmodule Acyclic.GeneratedRuntimeReceipt do",
        r'  def json_escape(value), do: value |> to_string() |> String.replace("\\", "\\\\") |> String.replace("\"", "\\\"")',
        "  def hex(bytes), do: Base.encode16(bytes, case: :lower)",
        "  def base64(bytes), do: Base.encode64(bytes)",
        "  def decode_request(module, bytes) do",
        "    case apply(module, :decode, [bytes]) do {:ok, value} -> value; value -> value end",
        "  end",
        "  def configured_request_frames(rpc, module, fallback) do",
        "    path = System.get_env(\"ACYCLIC_RUST_STREAM_REQUEST_FRAMES\")",
        "    if is_nil(path) or not File.exists?(path), do: [fallback], else: stream_request_frames(path, rpc, module, fallback)",
        "  end",
        "  def stream_request_frames(path, rpc, module, fallback) do",
        "    key = rpc",
        "    line = File.read!(path) |> String.split(\"\\n\", trim: true) |> Enum.find(fn value -> String.starts_with?(value, key <> \"\\t\") end)",
        "    case line do nil -> [fallback]; value -> value |> String.split(\"\\t\", parts: 2) |> List.last() |> String.split(\",\", trim: true) |> Enum.map(fn encoded -> encoded |> Base.decode64!() |> decode_request(module) end) end",
        "  end",
        "  def send_client_frames(stream, request_module, requests) do",
        "    Enum.map(requests, fn request -> GRPC.Stub.send_request(stream, request); wire_bytes(request_module, request) end)",
        "  end",
        "  def wire_bytes(module, value) do",
        "    raw = apply(module, :encode, [value])",
        "    case raw do {:ok, bytes} -> IO.iodata_to_binary(bytes); bytes -> IO.iodata_to_binary(bytes) end",
        "  end",
        "  def request(module), do: if(function_exported?(module, :new, 1), do: apply(module, :new, [[]]), else: struct(module))",
        "  def status(error) do",
        "    cond do",
        "      is_map(error) and is_integer(Map.get(error, :status)) -> Map.get(error, :status)",
        "      is_map(error) and is_integer(Map.get(error, :grpc_status)) -> Map.get(error, :grpc_status)",
        "      true -> nil",
        "    end",
        "  end",
        "  def terminal_status(_status, \"deferred-rust-scenario\"), do: \"deferred\"",
        "  def terminal_status(status, _execution) when status == 0, do: \"ok\"",
        "  def terminal_status(status, _execution) when is_integer(status), do: \"error\"",
        "  def terminal_status(_status, _execution), do: \"unknown\"",
        "  def response_bytes(module, response), do: wire_bytes(module, response)",
        r"  def recv_stream(stream, response_module, acc \\ []) do",
        "    case GRPC.Stub.recv(stream) do",
        "      {:ok, response} -> recv_stream(stream, response_module, [response_bytes(response_module, response) | acc])",
        "      {:error, :eof} -> {:ok, Enum.reverse(acc)}",
        "      {:error, error} -> {:error, status(error), Enum.reverse(acc)}",
        "      other -> {:error, nil, Enum.reverse(acc), other}",
        "    end",
        "  end",
        r"  def obs(rpc, shape, request_bytes, responses, status, execution, error \\ nil, request_frames \\ nil) do",
        "    response_hashes = Enum.map(responses, fn bytes -> \"sha256:\" <> hex(:crypto.hash(:sha256, bytes)) end)",
        "    %{\"family\" => rpc |> String.split(\".\") |> List.first(), \"rpc\" => rpc, \"shape\" => to_string(shape), \"execution\" => execution, \"status\" => status, \"terminal_status\" => terminal_status(status, execution), \"terminal_code\" => status, \"request_base64\" => base64(request_bytes), \"request_frames_base64\" => Enum.map(request_frames || [request_bytes], &base64/1), \"request_sha256\" => \"sha256:\" <> hex(:crypto.hash(:sha256, request_bytes)), \"response_sha256\" => List.first(response_hashes), \"response_frames\" => length(response_hashes), \"response_frame_base64\" => Enum.map(responses, &base64/1), \"response_frame_sha256\" => response_hashes, \"error\" => if(error, do: inspect(error), else: nil)}",
        "  end",
        "  def json_observation(observation) do",
        "    status = if is_integer(observation[\"status\"]), do: Integer.to_string(observation[\"status\"]), else: \"null\"",
        "    error = if observation[\"error\"], do: \"\\\"\" <> json_escape(observation[\"error\"]) <> \"\\\"\", else: \"null\"",
        "    frames = observation[\"response_frame_sha256\"] |> Enum.map(fn value -> \"\\\"\" <> json_escape(value) <> \"\\\"\" end) |> Enum.join(\",\")",
        "    terminal = \"\\\"\" <> observation[\"terminal_status\"] <> \"\\\"\"",
        "    \"{\\\"rpc\\\":\\\"\" <> json_escape(observation[\"rpc\"]) <> \"\\\",\\\"shape\\\":\\\"\" <> json_escape(observation[\"shape\"]) <> \"\\\",\\\"execution\\\":\\\"\" <> json_escape(observation[\"execution\"]) <> \"\\\",\\\"status\\\":\" <> status <> \",\\\"terminal_status\\\":\" <> terminal <> \",\\\"terminal_code\\\":\" <> status <> \",\\\"request_sha256\\\":\\\"\" <> observation[\"request_sha256\"] <> \"\\\",\\\"response_sha256\\\":\" <> if(observation[\"response_sha256\"], do: \"\\\"\" <> observation[\"response_sha256\"] <> \"\\\"\", else: \"null\") <> \",\\\"response_frames\\\":\" <> Integer.to_string(observation[\"response_frames\"]) <> \",\\\"response_frame_sha256\\\":[\" <> frames <> \"],\\\"error\\\":\" <> error <> \"}\"",
        "  end",
        "  def json_observation_with_bytes(observation) do",
        "    frame_bytes = observation[\"response_frame_base64\"] |> Enum.map(fn value -> \"\\\"\" <> json_escape(value) <> \"\\\"\" end) |> Enum.join(\",\")",
        "    request_frame_bytes = observation[\"request_frames_base64\"] |> Enum.map(fn value -> \"\\\"\" <> json_escape(value) <> \"\\\"\" end) |> Enum.join(\",\")",
        "    fields = \",\\\"family\\\":\\\"\" <> json_escape(observation[\"family\"]) <> \"\\\",\\\"request_base64\\\":\\\"\" <> observation[\"request_base64\"] <> \"\\\",\\\"response_frame_base64\\\":[\" <> frame_bytes <> \"],\"",
        "    json_observation(observation) |> String.replace(~r/,\\\"response_frames\\\":([0-9]+),/, \",\\\"response_frame_count\\\":\\\\1,\") |> String.replace(\",\\\"request_sha256\\\":\" , fields <> \"\\\"request_sha256\\\":\") |> String.replace(\",\\\"request_base64\\\":\\\"\", \",\\\"request_frames_base64\\\":[\" <> request_frame_bytes <> \"],\\\"request_base64\\\":\\\"\")",
        "  end",
        "  def write_receipt(path, observations) do",
        "    source = System.get_env(\"ACYCLIC_RUST_SOURCE_REVISION\") || \"unknown\"",
        "    manifest = System.get_env(\"ACYCLIC_RUST_AUTHORITY_MANIFEST_SHA256\") || \"unknown\"",
        "    body = observations |> Enum.map(&json_observation_with_bytes/1) |> Enum.join(\",\")",
        "    File.write!(path, \"{\\\"schema\\\":\\\"acyclic.runtime-consumer-receipt.v1\\\",\\\"language\\\":\\\"elixir\\\",\\\"source_revision\\\":\\\"\" <> json_escape(source) <> \"\\\",\\\"rust_authority_manifest_sha256\\\":\\\"\" <> json_escape(manifest) <> \"\\\",\\\"observations\\\":[\" <> body <> \"],\\\"rpc_count\\\":\" <> Integer.to_string(length(observations)) <> \"}\")",
        "  end",
        "end",
        "",
        "calls = [",
        *[f"  {{{stub}, :{method}, {request}, {response}, :{shape}, \"{rpc}\"}}," for stub, method, request, response, shape, rpc in calls],
        "]",
        "endpoint = List.first(System.argv) || System.get_env(\"ACYCLIC_FIXTURE_GRPC_ENDPOINT\") || \"127.0.0.1:50051\"",
        "receipt = Path.join(Path.dirname(__ENV__.file), \"runtime-consumer-receipt.json\")",
        "{:ok, _supervisor} = DynamicSupervisor.start_link(strategy: :one_for_one, name: GRPC.Client.Supervisor)",
        "{:ok, channel} = GRPC.Stub.connect(endpoint)",
        "stream_allow = System.get_env(\"ACYCLIC_STREAM_SCENARIO_RPCS\", \"\") |> String.split(\",\", trim: true) |> MapSet.new()",
        "try do",
        "  observations = Enum.map(calls, fn {stub, method, request_module, response_module, shape, rpc} ->",
        "    request = Acyclic.GeneratedRuntimeReceipt.request(request_module)",
        "    request_bytes = Acyclic.GeneratedRuntimeReceipt.wire_bytes(request_module, request)",
        "    if shape != :unary and not MapSet.member?(stream_allow, rpc) do",
        "      Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, [], nil, \"deferred-rust-scenario\")",
        "    else",
        "      result = case shape do",
        "        :unary -> apply(stub, method, [channel, request, []])",
        "        :server_stream -> apply(stub, method, [channel, request, []])",
        "        _ -> case apply(stub, method, [channel, []]) do {:ok, stream} -> request_frames = Acyclic.GeneratedRuntimeReceipt.configured_request_frames(rpc, request_module, request); sent_frames = Acyclic.GeneratedRuntimeReceipt.send_client_frames(stream, request_module, request_frames); GRPC.Stub.end_stream(stream); {Acyclic.GeneratedRuntimeReceipt.recv_stream(stream, response_module), sent_frames}; other -> other end",
        "      end",
        "      case {shape, result} do",
        "        {:unary, {:ok, reply}} -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, [Acyclic.GeneratedRuntimeReceipt.response_bytes(response_module, reply)], 0, \"executed\")",
        "        {:server_stream, {:ok, stream}} -> responses = Enum.map(stream, &Acyclic.GeneratedRuntimeReceipt.response_bytes(response_module, &1)); Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, responses, 0, \"executed\")",
        "        {_, {{:ok, responses}, request_frames}} when is_list(responses) -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, responses, 0, \"executed\", nil, request_frames)",
        "        {_, {{:error, code, responses}, request_frames}} -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, responses, code, \"executed\", result, request_frames)",
        "        {_, {:ok, responses}} when is_list(responses) -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, responses, 0, \"executed\")",
        "        {_, {:error, code, responses}} -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, responses, code, \"executed\", result)",
        "        {_, {:error, error}} -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, [], Acyclic.GeneratedRuntimeReceipt.status(error), \"executed\", error)",
        "        {_, other} -> Acyclic.GeneratedRuntimeReceipt.obs(rpc, shape, request_bytes, [], Acyclic.GeneratedRuntimeReceipt.status(other), \"executed\", other)",
        "      end",
        "    end",
        "  end)",
        "  Acyclic.GeneratedRuntimeReceipt.write_receipt(receipt, observations)",
        "  if Enum.any?(observations, &(&1[\"execution\"] == \"executed\" and &1[\"status\"] not in [0, nil])) do raise \"one or more executed Rust fixture RPCs failed; see runtime-consumer-receipt.json\" end",
        "  IO.puts(\"Elixir generated stubs recorded all 106 Rust RPC observations\")",
        "after",
        "  GRPC.Stub.disconnect(channel)",
        "end",
    ]
    output.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
