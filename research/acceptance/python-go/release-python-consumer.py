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


def tree_hash(root: Path) -> tuple[str, dict[str, str]]:
    """Return a deterministic digest and per-file hashes for an installed tree."""
    files: dict[str, str] = {}
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        relative = path.relative_to(root).as_posix()
        files[relative] = digest(path.read_bytes())
    payload = "\n".join(f"{name}\0{files[name]}" for name in sorted(files)).encode()
    return digest(payload), files


def authority_binding(authority: dict[str, Any]) -> dict[str, Any]:
    source_git_sha = authority.get("source_git_sha")
    model = authority.get("source_revision")
    source_hashes = authority.get("source_file_hashes")
    if not isinstance(source_git_sha, str) or len(source_git_sha) != 40:
        raise RuntimeError("Rust authority source_git_sha is required")
    if not isinstance(model, str) or len(model) != 64:
        raise RuntimeError("Rust authority source_revision model digest is required")
    if not isinstance(source_hashes, dict) or not source_hashes:
        raise RuntimeError("Rust authority source_file_hashes are required")
    return {"source_git_sha": source_git_sha, "model_digest": model if model.startswith("sha256:") else "sha256:" + model, "source_file_hashes": source_hashes}


def load_typed_manifest(path: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    plan = manifest.get("execution_plan")
    if manifest.get("complete") is not True or not isinstance(plan, list) or not plan:
        raise RuntimeError("Rust typed manifest must contain a complete non-empty execution_plan")
    if manifest.get("execution_plan_count") != len(plan):
        raise RuntimeError("Rust typed manifest execution_plan_count does not match execution_plan")
    if not isinstance(manifest.get("source_revision"), str) or not manifest["source_revision"]:
        raise RuntimeError("Rust typed manifest source_revision is required")
    for index, record in enumerate(plan):
        if record.get("execution_step") != index:
            raise RuntimeError(f"Rust typed manifest execution_step {record.get('execution_step')} is not {index}")
        if not isinstance(record.get("rpc"), str) or not record["rpc"]:
            raise RuntimeError("Rust typed manifest RPC identity is missing")
    return manifest, plan


def validate_manifest_binding(manifest: dict[str, Any], authority: dict[str, Any]) -> None:
    """Require the typed request plan and authority to come from one Rust revision."""
    source_revision = manifest.get("source_revision")
    authority_revision = authority.get("source_git_sha")
    if source_revision != authority_revision:
        raise RuntimeError(
            "Rust typed manifest source_revision does not match authority source_git_sha"
        )
    authority_digest = manifest.get("authority_sha256")
    if not isinstance(authority_digest, str) or not authority_digest.startswith("sha256:"):
        raise RuntimeError("Rust typed manifest authority_sha256 is required")


def request_messages(record: dict[str, Any], request_type: Any) -> list[Any]:
    frames = record.get("request_frames") or []
    if not frames:
        frames = [{"request_base64": record.get("request_base64", ""), "sequence": 0}]
    messages = []
    for index, frame_record in enumerate(frames):
        payload = base64.b64decode(frame_record.get("request_base64", frame_record.get("bytes_base64", "")), validate=True)
        message = request_type()
        message.ParseFromString(payload)
        actual = digest(message.SerializeToString(deterministic=True))
        expected = frame_record.get("request_sha256") or frame_record.get("sha256")
        if expected and actual != expected:
            raise RuntimeError(f"Rust request frame {record['rpc']}[{index}] differs from its declared digest")
        messages.append(message)
    return messages


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


def call_rpc(stub: Any, method: Any, requests: list[Any], timeout: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    call = getattr(stub, method.name)
    if method.client_streaming:
        result = call(iter(requests), timeout=timeout)
    else:
        result = call(requests[0], timeout=timeout)
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
    parser.add_argument("--manifest", required=False, type=Path, default=None, help="Rust typed-request manifest")
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--package-root", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--source-revision", default=None)
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args()
    sys.path.insert(0, str(args.package_root.resolve()))
    authority, authority_inventory = load_authority(args.authority)
    manifest = None
    if args.manifest is not None:
        manifest, plan = load_typed_manifest(args.manifest)
        validate_manifest_binding(manifest, authority)
        inventory = [(record.get("family", "unknown"), record["rpc"]) for record in plan]
    else:
        # Keep the legacy authority-only invocation available for local diagnostics;
        # release qualification must pass --manifest so requests come exclusively
        # from the Rust executable producer.
        inventory = authority_inventory
    services = generated_services()
    revision = args.source_revision or (manifest or authority).get("source_revision") or "unknown"
    scenarios: list[dict[str, Any]] = []
    with grpc.insecure_channel(args.endpoint) as channel:
        for index, (family, rpc) in enumerate(inventory):
            service_name, method_name = rpc.split("/", 1)
            service, stub_type = services[service_name]
            method = service.methods_by_name[method_name]
            stub = stub_type(channel)
            request_type = message_factory.GetMessageClass(method.input_type)
            record = plan[index] if manifest is not None else None
            requests = request_messages(record, request_type) if record is not None else [request_type()]
            if record is None:
                populate(requests[0], method.input_type, 17)
                if method.client_streaming:
                    second = request_type()
                    populate(second, method.input_type, 97)
                    requests.append(second)
            started = time.monotonic()
            request_frames = [frame(request, method.input_type.full_name, frame_index) for frame_index, request in enumerate(requests)]
            expected = (record or {}).get("expected_outcome") or {}
            try:
                responses, terminal = call_rpc(stub, method, requests, args.timeout)
                status = "passed" if expected.get("grpc_code", "OK") == "OK" else "failed"
                error = None
            except grpc.RpcError as exc:
                responses = []
                terminal = {"code": exc.code().name, "details": exc.details() or ""}
                status = "passed" if exc.code().name == expected.get("grpc_code") else "failed"
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
    authority_id = authority_binding(authority)
    package_tree, package_files = tree_hash(args.package_root)
    package = {
        "language": "python",
        **authority_id,
        "package_tree_sha256": package_tree,
        "package_source_file_hashes": package_files,
        "runtime_executable": sys.executable,
        "runtime_executable_sha256": digest(Path(sys.executable).read_bytes()) if Path(sys.executable).is_file() else None,
    }
    manifest_bytes = args.manifest.read_bytes() if args.manifest else b""
    payload = {
        "schema": "acyclic.sdk.rpc-scenario-log.v2",
        "consumer": "python-rust-authority-release",
        "source_revision": revision,
        "rust_authority_manifest_sha256": digest(manifest_bytes) if manifest_bytes else None,
        "authority": authority_id,
        "executed_package": package,
        "execution_mode": "remote",
        "artifact_root": str(args.package_root),
        "scenarios": scenarios,
        "observations": scenarios,
    }
    args.out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
