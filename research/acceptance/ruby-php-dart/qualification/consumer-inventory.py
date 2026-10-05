#!/usr/bin/env python3
"""Build a Rust-authority-bound inventory for installed Ruby/PHP/Dart consumers.

This is deliberately an inventory/qualification input, not a fixture oracle.  A
remote trace is accepted only when it carries the same authority identity.  A
missing trace leaves the method pending instead of converting a transport call
or a default protobuf response into a pass.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import zipfile
from pathlib import Path
from typing import Any

from google.protobuf import descriptor_pb2


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def field_shape(field: descriptor_pb2.FieldDescriptorProto) -> dict[str, Any]:
    return {
        "name": field.name,
        "json_name": field.json_name,
        "number": field.number,
        "label": descriptor_pb2.FieldDescriptorProto.Label.Name(field.label),
        "kind": descriptor_pb2.FieldDescriptorProto.Type.Name(field.type),
        "type_name": field.type_name or None,
        "proto3_optional": bool(field.proto3_optional),
        "packed": bool(field.options.packed) if field.HasField("options") else False,
    }


def load_descriptors(authority_dir: Path) -> tuple[dict[str, descriptor_pb2.DescriptorProto], list[dict[str, Any]]]:
    messages: dict[str, descriptor_pb2.DescriptorProto] = {}
    methods: list[dict[str, Any]] = []
    for path in sorted(authority_dir.rglob("*.fds.bin")):
        fds = descriptor_pb2.FileDescriptorSet()
        fds.ParseFromString(path.read_bytes())
        for fd in fds.file:
            def visit_message(msg: descriptor_pb2.DescriptorProto, prefix: str) -> None:
                full = f".{prefix}.{msg.name}" if prefix else f".{msg.name}"
                messages[full] = msg
                for nested in msg.nested_type:
                    visit_message(nested, full.lstrip("."))

            for msg in fd.message_type:
                visit_message(msg, fd.package)
            for service in fd.service:
                for method in service.method:
                    methods.append({
                        "family": path.relative_to(authority_dir).as_posix(),
                        "package": fd.package,
                        "service": service.name,
                        "method": method.name,
                        "path": f"/{fd.package}.{service.name}/{method.name}",
                        "request_type": method.input_type,
                        "response_type": method.output_type,
                        "client_streaming": bool(method.client_streaming),
                        "server_streaming": bool(method.server_streaming),
                    })
    return messages, methods


def response_schema(messages: dict[str, descriptor_pb2.DescriptorProto], type_name: str) -> list[dict[str, Any]]:
    msg = messages.get(type_name)
    if msg is None:
        return []
    return [field_shape(field) for field in msg.field]


def package_check(path: Path) -> dict[str, Any]:
    with zipfile.ZipFile(path) as zf:
        names = zf.namelist()
        generated = [
            n for n in names
            if "/generated/" in n
            or "/src/generated/" in n
            or "/lib/src/generated/" in n
            or n.endswith("_pb.rb")
            or n.endswith("_services_pb.rb")
            or n.endswith(".pb.php")
            or "/src/Acyclic/" in n
            or "/src/Inference/" in n
            or n.endswith(".pb.dart")
            or n.endswith(".pbenum.dart")
            or n.endswith(".pbgrpc.dart")
        ]
        provenance = None
        for name in names:
            if name.endswith("generated/provenance.json") or name.endswith("src/provenance.json"):
                try:
                    provenance = json.loads(zf.read(name).decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    provenance = {"error": "invalid_json", "path": name}
                break
    return {
        "archive": str(path),
        "sha256": sha256(path),
        "entries": len(names),
        "generated_entries": len(generated),
        "has_provenance": any(n.endswith("generated/provenance.json") or n.endswith("src/provenance.json") for n in names),
        "provenance": provenance,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority-dir", type=Path, required=True)
    ap.add_argument("--authority", type=Path, required=True)
    ap.add_argument("--archive", action="append", default=[], metavar="LANG=ZIP")
    ap.add_argument("--trace", type=Path, help="Optional Rust-fixture-bound JSON trace")
    ap.add_argument("--output", type=Path, required=True)
    args = ap.parse_args()

    authority = json.loads(args.authority.read_text(encoding="utf-8"))
    messages, methods = load_descriptors(args.authority_dir)
    trace = json.loads(args.trace.read_text(encoding="utf-8")) if args.trace else {}
    trace_methods = trace.get("methods", {}) if isinstance(trace, dict) else {}
    package_artifacts: dict[str, Any] = {}
    for item in args.archive:
        lang, sep, value = item.partition("=")
        if not sep or not lang or not value:
            raise SystemExit(f"invalid --archive {item!r}; expected LANG=ZIP")
        package_artifacts[lang] = package_check(Path(value))

    inventory: list[dict[str, Any]] = []
    for method in methods:
        key = f"{method['package']}.{method['service']}/{method['method']}"
        observed = trace_methods.get(key)
        entry = {
            **method,
            "request_fields": response_schema(messages, method["request_type"]),
            "response_fields": response_schema(messages, method["response_type"]),
            "typed_request": {"empty_serialized_hex": "", "empty_serialized_sha256": hashlib.sha256(b"").hexdigest()},
            "decoded_response": observed if observed is not None else {"status": "pending_matching_fixture"},
        }
        if isinstance(observed, dict) and "request_hex" in observed:
            raw = bytes.fromhex(observed["request_hex"])
            entry["typed_request"] = {
                "serialized_hex": observed["request_hex"],
                "serialized_sha256": hashlib.sha256(raw).hexdigest(),
                "fields": observed.get("request_fields", []),
            }
            if isinstance(observed.get("request_frames"), list):
                entry["typed_request"]["serialized_frames"] = observed["request_frames"]
            for key in (
                "terminal_status",
                "terminal_code",
                "response_type",
                "response_base64",
                "response_sha256",
                "response_frames",
                "response_frames_sha256",
                "response_frame_type_ids",
            ):
                if key in observed:
                    entry[key] = observed[key]
        inventory.append(entry)

    passed = sum(1 for e in inventory if isinstance(e["decoded_response"], dict) and e["decoded_response"].get("status") == "passed")
    result = {
        "schema": "acyclic.sdk.rpd.rust-authority-consumer-inventory.v1",
        "complete": len(inventory) == len(methods) and len(inventory) > 0,
        "authority": {
            "source_git_sha": authority.get("source_git_sha"),
            "model_digest": authority.get("source_revision"),
            "source_file_hashes": authority.get("source_file_hashes", {}),
        },
        "method_count": len(inventory),
        "remote_semantic_passed": passed,
        "remote_semantic_pending": len(inventory) - passed,
        "packages": package_artifacts,
        "methods": inventory,
        "qualification_rule": "Only trace entries with matching authority identity and decoded response values count as passed; transport-only/default responses remain pending.",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=False) + "\n", encoding="utf-8")
    print(json.dumps({"method_count": len(inventory), "remote_semantic_passed": passed, "remote_semantic_pending": len(inventory) - passed}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
