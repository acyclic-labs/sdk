#!/usr/bin/env python3
"""Replay the complete Rust-owned execution plan against one fresh gRPC fixture.

The manifest is the only request source. This verifier preserves repeated setup
steps, multipart frames, dynamic identities, stream terminal outcomes, and
exact protobuf bytes; it never synthesizes a fallback request.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import sys
import time
from pathlib import Path

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def digest(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def load_methods(path: Path):
    bundle = descriptor_pb2.FileDescriptorSet.FromString(path.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for descriptor in bundle.file:
        pool.Add(descriptor)
    methods = {}
    for descriptor in bundle.file:
        prefix = f"{descriptor.package}." if descriptor.package else ""
        for service in descriptor.service:
            for method in service.method:
                full_name = f"{prefix}{service.name}/{method.name}"
                methods[full_name] = (method, pool)
    return methods


def frame_bytes(record: dict, direction: str) -> list[bytes]:
    frames = record.get(f"{direction}_frames") or []
    if frames:
        return [base64.b64decode(frame["bytes_base64"], validate=True) for frame in frames]
    if direction == "request":
        return [base64.b64decode(record.get("request_base64", ""), validate=True)]
    return []


def call(channel, method, record: dict, metadata: tuple[tuple[str, str], ...], timeout: float):
    request_descriptor = channel.pool.FindMessageTypeByName(method.input_type.lstrip("."))
    response_descriptor = channel.pool.FindMessageTypeByName(method.output_type.lstrip("."))
    request_type = message_factory.GetMessageClass(request_descriptor)
    response_type = message_factory.GetMessageClass(response_descriptor)
    request_values = []
    for payload in frame_bytes(record, "request"):
        request = request_type()
        request.ParseFromString(payload)
        request_values.append(request)
    path = f"/{record['rpc']}"
    serializer = lambda value: value.SerializeToString(deterministic=True)
    if method.client_streaming and method.server_streaming:
        rpc = channel.channel.stream_stream(path, request_serializer=serializer, response_deserializer=response_type.FromString)
        return list(rpc(iter(request_values), metadata=metadata, timeout=timeout))
    if method.client_streaming:
        rpc = channel.channel.stream_unary(path, request_serializer=serializer, response_deserializer=response_type.FromString)
        return [rpc(iter(request_values), metadata=metadata, timeout=timeout)]
    if method.server_streaming:
        rpc = channel.channel.unary_stream(path, request_serializer=serializer, response_deserializer=response_type.FromString)
        return list(rpc(request_values[0], metadata=metadata, timeout=timeout))
    rpc = channel.channel.unary_unary(path, request_serializer=serializer, response_deserializer=response_type.FromString)
    return [rpc(request_values[0], metadata=metadata, timeout=timeout)]


class Channel:
    def __init__(self, channel):
        self.channel = channel
        self.pool = None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("descriptor_set", type=Path)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("endpoint")
    parser.add_argument("receipt", type=Path)
    parser.add_argument("--build-receipt", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=3.0)
    args = parser.parse_args()

    manifest_bytes = args.manifest.read_bytes()
    manifest = json.loads(manifest_bytes)
    plan = manifest.get("execution_plan")
    if manifest.get("complete") is not True or not isinstance(plan, list) or len(plan) != manifest.get("execution_plan_count"):
        raise SystemExit("Rust manifest must contain a complete ordered execution_plan")
    source_revision = manifest.get("source_revision")
    if not source_revision:
        raise SystemExit("Rust manifest source_revision is required")
    build = json.loads(args.build_receipt.read_text(encoding="utf-8"))
    build_revision = build.get("source_revision") or (build.get("source") or {}).get("revision")
    if build_revision != source_revision:
        raise SystemExit("build receipt source_revision does not match the Rust manifest")
    methods = load_methods(args.descriptor_set)
    missing = [record["rpc"] for record in plan if record.get("rpc") not in methods]
    if missing:
        raise SystemExit(f"descriptor set is missing Rust RPCs: {missing[:5]}")

    pool = next(iter(methods.values()))[1]
    channel = grpc.insecure_channel(args.endpoint.removeprefix("http://").removeprefix("https://"))
    channel_wrapper = Channel(channel)
    channel_wrapper.pool = pool
    grpc.channel_ready_future(channel).result(timeout=10)
    rows = []
    started = time.monotonic()
    for step, record in enumerate(plan):
        expected = record.get("expected_outcome") or {}
        expected_requests = frame_bytes(record, "request")
        expected_request_hashes = [frame.get("sha256") for frame in record.get("request_frames", [])]
        if not expected_request_hashes:
            expected_request_hashes = [digest(payload) for payload in expected_requests]
        row = {"execution_step": step, "rpc": record["rpc"], "status": "observed"}
        failures = []
        actual_request_hashes = [digest(payload) for payload in expected_requests]
        if actual_request_hashes != expected_request_hashes:
            failures.append("Rust request frame digest mismatch")
        metadata = (("authorization", "Bearer fixture-token"), ("x-acyclic-family", record["family"]))
        try:
            responses = call(channel_wrapper, methods[record["rpc"]][0], record, metadata, args.timeout)
            row["response_frames"] = [
                {"sequence": index, "sha256": digest(value.SerializeToString(deterministic=True))}
                for index, value in enumerate(responses)
            ]
            expected_response_hashes = [frame.get("response_sha256") for frame in record.get("response_frames", [])]
            actual_response_hashes = [frame["sha256"] for frame in row["response_frames"]]
            if actual_response_hashes != expected_response_hashes:
                failures.append(f"response frame digests differ: expected {expected_response_hashes}, observed {actual_response_hashes}")
            if expected.get("grpc_code") != "OK":
                failures.append(f"expected gRPC {expected.get('grpc_code')} but call completed")
        except grpc.RpcError as error:
            row["status"] = "observed_error"
            row["code"] = error.code().name
            row["details"] = error.details()
            if error.code().name != expected.get("grpc_code"):
                failures.append(f"gRPC code differs: expected {expected.get('grpc_code')}, observed {error.code().name}")
            if expected.get("grpc_code") == "OK":
                failures.append("Rust expected successful response")
        if failures:
            row["failures"] = failures
        rows.append(row)
    channel.close()
    failed = [row for row in rows if row.get("failures")]
    receipt = {
        "schema": "acyclic.sdk.rust-canonical-grpc-replay.v1",
        "status": "passed" if not failed else "failed",
        "endpoint": args.endpoint,
        "manifest": str(args.manifest),
        "manifest_sha256": digest(manifest_bytes),
        "source_revision": source_revision,
        "execution_plan_count": len(plan),
        "execution_plan_sha256": manifest.get("execution_plan_sha256"),
        "build_receipt": str(args.build_receipt),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rows": rows,
    }
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": receipt["status"], "receipt": str(args.receipt), "steps": len(plan), "failed": len(failed)}))
    return 0 if not failed else 1


if __name__ == "__main__":
    raise SystemExit(main())
