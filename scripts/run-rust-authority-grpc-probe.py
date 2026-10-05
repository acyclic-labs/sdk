#!/usr/bin/env python3
"""Exercise every Rust-owned RPC in a generated descriptor bundle.

The probe uses protobuf descriptors for request/response construction and the
standard gRPC channel API. It intentionally sends default messages; fixture
servers use those messages to prove transport, serialization, streaming, and
cancellation plumbing without inventing a second request contract.
"""
from __future__ import annotations

import json
import sys
import time
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


def call(channel, full_name, method, pool):
    request_descriptor = pool.FindMessageTypeByName(method.input_type.lstrip("."))
    response_descriptor = pool.FindMessageTypeByName(method.output_type.lstrip("."))
    request_type = message_factory.GetMessageClass(request_descriptor)
    response_type = message_factory.GetMessageClass(response_descriptor)
    path = f"/{full_name}"
    if method.client_streaming and method.server_streaming:
        rpc = channel.stream_stream(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        responses = list(rpc(iter((request_type(),)), timeout=5))
        return len(responses), "bidi"
    if method.client_streaming:
        rpc = channel.stream_unary(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        rpc(iter((request_type(),)), timeout=5)
        return 1, "client_stream"
    if method.server_streaming:
        rpc = channel.unary_stream(
            path,
            request_serializer=lambda value: value.SerializeToString(),
            response_deserializer=response_type.FromString,
        )
        return sum(1 for _ in rpc(request_type(), timeout=5)), "server_stream"
    rpc = channel.unary_unary(
        path,
        request_serializer=lambda value: value.SerializeToString(),
        response_deserializer=response_type.FromString,
    )
    rpc(request_type(), timeout=5)
    return 1, "unary"


def main() -> int:
    if len(sys.argv) != 4:
        print("usage: run-rust-authority-grpc-probe.py DESCRIPTOR_SET ENDPOINT RECEIPT", file=sys.stderr)
        return 2
    descriptor_path, endpoint, receipt_path = map(Path, sys.argv[1:])
    methods = load_methods(descriptor_path)
    if len(methods) != 106:
        raise SystemExit(f"Rust descriptor bundle has {len(methods)} RPCs; expected 106")
    rows = []
    started = time.monotonic()
    with grpc.insecure_channel(str(endpoint)) as channel:
        grpc.channel_ready_future(channel).result(timeout=10)
        for full_name, method, pool in methods:
            row = {"rpc": full_name, "shape": "unknown", "status": "pending"}
            try:
                count, shape = call(channel, full_name, method, pool)
                row.update({"shape": shape, "status": "passed", "response_count": count})
            except grpc.RpcError as error:
                row.update({
                    "shape": row["shape"],
                    "status": "failed",
                    "code": error.code().name,
                    "details": error.details(),
                })
                rows.append(row)
                break
            rows.append(row)
    passed = sum(row["status"] == "passed" for row in rows)
    receipt = {
        "schema": "acyclic.sdk.rust-authority-grpc-probe.v1",
        "endpoint": str(endpoint),
        "rpc_count": len(methods),
        "passed": passed,
        "status": "passed" if passed == len(methods) else "failed",
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "rows": rows,
    }
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return 0 if passed == len(methods) else 1


if __name__ == "__main__":
    raise SystemExit(main())