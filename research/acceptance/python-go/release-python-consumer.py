"""Exercise every Rust-authority RPC with the installed Python package.

This is deliberately descriptor driven.  It does not maintain a second RPC
inventory or a language-specific contract: the authority manifest supplies
the 106 identities and the generated protobuf descriptors supply message
types and streaming flags.  Every call records the bytes sent and received;
the Rust semantic verifier is the gate for meaning after this transport run.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import importlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import grpc
from google.protobuf import descriptor as descriptor_api
from google.protobuf import message_factory


MODULES = {
    "actors": ("acyclic_sdk.generated.actors.v1.actors_pb2", "acyclic_sdk.generated.actors.v1.actors_pb2_grpc"),
    "workers": ("acyclic_sdk.generated.workers.v1.workers_pb2", "acyclic_sdk.generated.workers.v1.workers_pb2_grpc"),
    "objects": ("acyclic_sdk.generated.objects.v2.objects_pb2", "acyclic_sdk.generated.objects.v2.objects_pb2_grpc"),
    "stream": ("acyclic_sdk.generated.stream.v2.stream_pb2", "acyclic_sdk.generated.stream.v2.stream_pb2_grpc"),
    "filesystem": ("acyclic_sdk.generated.filesystem.v2.filesystem_pb2", "acyclic_sdk.generated.filesystem.v2.filesystem_pb2_grpc"),
    "harness": ("acyclic_sdk.generated.harness.v2.harness_pb2", "acyclic_sdk.generated.harness.v2.harness_pb2_grpc"),
    "inference": ("acyclic_sdk.generated.inference.v1.inference_pb2", "acyclic_sdk.generated.inference.v1.inference_pb2_grpc"),
    "machines": ("acyclic_sdk.generated.machines.v1.machines_pb2", "acyclic_sdk.generated.machines.v1.machines_pb2_grpc"),
}


def digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def frame(message: Any, message_type: str, sequence: int) -> dict[str, Any]:
    encoded = message.SerializeToString(deterministic=True)
    return {
        "sequence": sequence,
        "type": message_type,
        "bytes_base64": base64.b64encode(encoded).decode("ascii"),
        "sha256": digest(encoded),
    }


def scalar(field: Any, seed: int) -> Any:
    types = descriptor_api.FieldDescriptor
    if field.type == types.TYPE_BOOL:
        return True
    if field.type in (types.TYPE_INT32, types.TYPE_SINT32, types.TYPE_SFIXED32, types.TYPE_INT64, types.TYPE_SINT64, types.TYPE_SFIXED64):
        return seed
    if field.type in (types.TYPE_UINT32, types.TYPE_FIXED32):
        return seed + 1
    if field.type in (types.TYPE_UINT64, types.TYPE_FIXED64):
        return 2**63 + seed
    if field.type == types.TYPE_FLOAT:
        return 1.25
    if field.type == types.TYPE_DOUBLE:
        return 2.5
    if field.type == types.TYPE_STRING:
        if "path" in field.name:
            return "fixture/path"
        if "bucket" in field.name:
            return "fixture-bucket"
        if "key" in field.name:
            return "fixture-key"
        return f"fixture-{field.name}"
    if field.type == types.TYPE_BYTES:
        return bytes([seed % 251 or 1]) * (32 if "digest" in field.name or "sha" in field.name else 16)
    if field.type == types.TYPE_ENUM:
        values = list(field.enum_type.values)
        return values[1].number if len(values) > 1 else values[0].number
    raise TypeError(f"unsupported protobuf field {field.full_name}: {field.type}")


def populate(message: Any, descriptor: Any, seed: int, depth: int = 0) -> None:
    if depth > 3:
        return
    oneofs: set[str] = set()
    for index, field in enumerate(descriptor.fields):
        if field.containing_oneof is not None:
            if field.containing_oneof.name in oneofs:
                continue
            oneofs.add(field.containing_oneof.name)
        value_seed = seed + index + 1
        if field.message_type is not None and field.message_type.GetOptions().map_entry:
            continue
        if field.is_repeated:
            container = getattr(message, field.name)
            if field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
                child = container.add()
                populate(child, field.message_type, value_seed, depth + 1)
            else:
                container.append(scalar(field, value_seed))
        elif field.type == descriptor_api.FieldDescriptor.TYPE_MESSAGE:
            populate(getattr(message, field.name), field.message_type, value_seed, depth + 1)
        else:
            setattr(message, field.name, scalar(field, value_seed))


def load_authority(path: Path) -> tuple[dict[str, Any], list[tuple[str, str]]]:
    authority = json.loads(path.read_text(encoding="utf-8"))
    methods: list[tuple[str, str]] = []
    for family in authority.get("families", []):
        for method in family.get("rpc_methods", []):
            methods.append((family["source"].split("/", 1)[0], method["rpc"]))
    if len(methods) != 106 or len({rpc for _, rpc in methods}) != 106:
        raise RuntimeError(f"Rust authority must contain 106 unique RPCs, got {len(methods)}")
    return authority, methods


def generated_services() -> dict[str, tuple[Any, Any]]:
    services: dict[str, tuple[Any, Any]] = {}
    for pb_name, grpc_name in MODULES.values():
        pb = importlib.import_module(pb_name)
        grpc_module = importlib.import_module(grpc_name)
        for service in pb.DESCRIPTOR.services_by_name.values():
            stub_type = getattr(grpc_module, service.name + "Stub")
            services[service.full_name] = (service, stub_type)
    return services


def call_rpc(stub: Any, method: Any, request: Any, timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    call = getattr(stub, method.name)
    requests = [request]
    if method.client_streaming:
        requests = [request, request.__class__()]
        populate(requests[1], method.input_type, 97)
        result = call(iter(requests), timeout=timeout)
    else:
        result = call(request, timeout=timeout)
    responses: list[dict[str, Any]] = []
    if method.server_streaming:
        for index, response in enumerate(result):
            responses.append(frame(response, method.output_type.full_name, index))
    else:
        responses.append(frame(result, method.output_type.full_name, 0))
    return responses, {"code": "OK", "details": ""}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority", required=True, type=Path)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--package-root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--source-revision", default=None)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    sys.path.insert(0, str(args.package_root.resolve()))
    authority, inventory = load_authority(args.authority)
    services = generated_services()
    revision = args.source_revision or authority.get("source_revision") or "unknown"
    scenarios: list[dict[str, Any]] = []
    with grpc.insecure_channel(args.endpoint) as channel:
        for family, rpc in inventory:
            service_name, method_name = rpc.split("/", 1)
            service, stub_type = services[service_name]
            method = service.methods_by_name[method_name]
            stub = stub_type(channel)
            request_type = message_factory.GetMessageClass(method.input_type)
            request = request_type()
            populate(request, method.input_type, 17)
            started = time.monotonic()
            request_frames = [frame(request, method.input_type.full_name, 0)]
            if method.client_streaming:
                second = request_type()
                populate(second, method.input_type, 97)
                request_frames.append(frame(second, method.input_type.full_name, 1))
            try:
                responses, terminal = call_rpc(stub, method, request, args.timeout)
                status = "passed"
                error = None
            except grpc.RpcError as exc:
                responses = []
                terminal = {"code": exc.code().name, "details": exc.details() or ""}
                status = "failed"
                error = str(exc)
            result = {
                "schema": "acyclic.sdk.rpc-scenario-result.v2",
                "source_revision": revision,
                "family": family,
                "rpc": rpc,
                "shape": "client" if method.client_streaming else "server" if method.server_streaming else "unary",
                "invoked": True,
                "execution_mode": "remote",
                "transport": "grpc",
                "status": status,
                "exit_code": 0 if status == "passed" else 1,
                "request_frames": request_frames,
                "response_frames": responses,
                "terminal": terminal,
                "response_count": len(responses),
                "elapsed_ms": round((time.monotonic() - started) * 1000, 3),
                "checks": ["invocation", "transport", "serialization", "request-frames", "terminal-status"],
            }
            if error:
                result["error"] = error
            scenarios.append(result)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps({
        "schema": "acyclic.sdk.rpc-scenario-log.v2",
        "consumer": "python-rust-authority-release",
        "source_revision": revision,
        "execution_mode": "remote",
        "artifact_root": str(args.package_root),
        "scenarios": scenarios,
    }, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
