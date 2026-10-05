#!/usr/bin/env python3
"""Transport diagnostic for the Rust-owned ordered execution plan.

This helper consumes the exact request frames and expected outcomes emitted by
the Rust typed manifest. It has no fallback request synthesis and is not a
semantic contract or RPC inventory.
"""
from __future__ import annotations

import json
import sys
import time
import base64
import hashlib
from pathlib import Path

import grpc
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory


def load_methods(path: Path):
    bundle = descriptor_pb2.FileDescriptorSet.FromString(path.read_bytes())
    pool = descriptor_pool.DescriptorPool()
    for file_descriptor in bundle.file:
        pool.Add(file_descriptor)
    methods = []
    for file_descriptor in bundle.file:
        for service in file_descriptor.service:
            package = file_descriptor.package
            prefix = f"{package}." if package else ""
            for method in service.method:
                methods.append((
                    f"{prefix}{service.name}/{method.name}",
                    method,
                    pool,
                ))
    return methods


def frame(value, message_type, sequence):
    payload = value.SerializeToString(deterministic=True)
    return {
        "sequence": sequence,
        "type": message_type,
        "bytes_base64": base64.b64encode(payload).decode("ascii"),
        "sha256": "sha256:" + hashlib.sha256(payload).hexdigest(),
    }


def call(channel, full_name, method, pool, record):
    request_descriptor = pool.FindMessageTypeByName(method.input_type.lstrip("."))
    response_descriptor = pool.FindMessageTypeByName(method.output_type.lstrip("."))
    request_type = message_factory.GetMessageClass(request_descriptor)
    response_type = message_factory.GetMessageClass(response_descriptor)
    path = f"/{full_name}"
    frames = record.get("request_frames") or [{"bytes_base64": record["request_base64"]}]
    requests = []
    for index, request_frame in enumerate(frames):
        if request_frame.get("sequence", index) != index:
            raise RuntimeError(f"{full_name} request frames are out of order")
        request = request_type()
        request.ParseFromString(base64.b64decode(request_frame["bytes_base64"], validate=True))
        requests.append(request)
    if not requests:
        raise RuntimeError(f"{full_name} has no Rust-owned request frames")
    if method.client_streaming and method.server_streaming:
        rpc = channel.stream_stream(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        responses = list(rpc(iter(requests), timeout=10))
        return [frame(value, response_descriptor.full_name, index) for index, value in enumerate(responses)], "bidi"
    if method.client_streaming:
        rpc = channel.stream_unary(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        response = rpc(iter(requests), timeout=10)
        return [frame(response, response_descriptor.full_name, 0)], "client_stream"
    if method.server_streaming:
        rpc = channel.unary_stream(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        responses = list(rpc(requests[0], timeout=10))
        return [frame(value, response_descriptor.full_name, index) for index, value in enumerate(responses)], "server_stream"
    rpc = channel.unary_unary(
        path,
        request_serializer=lambda value: value.SerializeToString(),
        response_deserializer=response_type.FromString,
    )
    response = rpc(requests[0], timeout=10)
    return [frame(response, response_descriptor.full_name, 0)], "unary"


def main() -> int:
    if len(sys.argv) != 5:
        print("usage: run-rust-authority-grpc-probe.py DESCRIPTOR_SET TYPED_MANIFEST ENDPOINT RECEIPT", file=sys.stderr)
        return 2
    descriptor_path, manifest_path, endpoint, receipt_path = map(Path, sys.argv[1:])
    methods = load_methods(descriptor_path)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    plan = manifest.get("execution_plan")
    if manifest.get("complete") is not True or not isinstance(plan, list) or not plan:
        raise SystemExit("Rust typed manifest must contain a complete ordered execution_plan")
    by_rpc = {full_name: (method, pool) for full_name, method, pool in methods}
    if any(row.get("rpc") not in by_rpc for row in plan):
        raise SystemExit("Rust execution plan contains an RPC absent from the descriptor set")
    rows = []
    started = time.monotonic()
    with grpc.insecure_channel(str(endpoint)) as channel:
        grpc.channel_ready_future(channel).result(timeout=10)
        for step, record in enumerate(plan):
            full_name = record["rpc"]
            method, pool = by_rpc[full_name]
            row = {"rpc": full_name, "shape": "unknown", "status": "pending"}
            try:
                response_frames, shape = call(channel, full_name, method, pool, record)
                row.update({
                    "shape": shape,
                    "status": "observed",
                    "response_count": len(response_frames),
                    "response_frames": response_frames,
                })
            except grpc.RpcError as error:
                row.update({"shape": row["shape"], "status": "observed_error", "code": error.code().name, "details": error.details()})
            except Exception as error:
                row.update({"status": "transport_failed", "details": str(error)})
            row["execution_step"] = step
            rows.append(row)
    failed = sum(row["status"] == "transport_failed" for row in rows)
    receipt = {
        "schema": "acyclic.sdk.rust-authority-grpc-transport-observations.v1",
        "endpoint": str(endpoint),
        "rpc_count": len(plan),
        "observed_count": len(rows) - failed,
        "status": "observed" if failed == 0 else "transport_failed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rows": rows,
    }
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
