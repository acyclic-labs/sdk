#!/usr/bin/env python3
"""Adapt additional-language byte receipts to the canonical RPD schema.

This module performs schema adaptation only. The Rust sdk-generation verifier
remains the authority for RPC identity, protobuf semantics, frame order, and
terminal status. Missing bytes, frame sequences, or numeric status codes are
rejected here so the adapter cannot manufacture qualification evidence.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return value


def rpc_key(value: dict[str, Any]) -> str:
    rpc = value.get("rpc") or value.get("path")
    if not isinstance(rpc, str) or not rpc:
        raise ValueError("manifest method is missing rpc/path")
    return rpc.lstrip("/")


def b64_hex(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be base64 text")
    try:
        return base64.b64decode(value, validate=True).hex()
    except (ValueError, TypeError) as error:
        raise ValueError(f"{label} is not valid base64: {error}") from error


def sha256_hex(value: str, label: str) -> str:
    try:
        data = bytes.fromhex(value)
    except ValueError as error:
        raise ValueError(f"{label} is not hexadecimal: {error}") from error
    return hashlib.sha256(data).hexdigest()


def terminal(observation: dict[str, Any], label: str) -> tuple[str, int]:
    status = observation.get("terminal_status")
    code = observation.get("terminal_code")
    if not isinstance(status, str) or not status:
        raise ValueError(f"{label}.terminal_status is missing")
    if isinstance(code, bool) or not isinstance(code, int):
        raise ValueError(f"{label}.terminal_code must be a numeric gRPC status")
    if not 0 <= code <= 16:
        raise ValueError(f"{label}.terminal_code must be in 0..16")
    return ("canceled" if status == "cancelled" else status, code)


def frame_hexes(observation: dict[str, Any], label: str) -> list[str]:
    frames = observation.get("response_frame_base64")
    if not isinstance(frames, list):
        raise ValueError(f"{label}.response_frame_base64 must be an array")
    return [b64_hex(frame, f"{label}.response_frame_base64[{index}]") for index, frame in enumerate(frames)]


def normalize(inventory: dict[str, Any], receipt: dict[str, Any], language: str) -> dict[str, Any]:
    methods = inventory.get("methods")
    authority = inventory.get("authority")
    observations = receipt.get("observations")
    if not isinstance(methods, list) or len(methods) != 106:
        raise ValueError("Rust RPD inventory must contain exactly 106 methods")
    if not isinstance(authority, dict):
        raise ValueError("Rust RPD inventory authority is missing")
    if not isinstance(observations, list):
        raise ValueError("runtime receipt observations are missing")
    source_revision = receipt.get("source_revision")
    source_git_sha = authority.get("source_git_sha")
    if not isinstance(source_revision, str) or source_revision != source_git_sha:
        raise ValueError("runtime receipt source_revision does not match the Rust authority")
    observed = {}
    for item in observations:
        if not isinstance(item, dict):
            raise ValueError("runtime receipt observation is not an object")
        key = rpc_key(item)
        if key in observed:
            raise ValueError(f"duplicate runtime RPC {key}")
        observed[key] = item

    output_methods: list[dict[str, Any]] = []
    for index, raw in enumerate(methods):
        if not isinstance(raw, dict):
            raise ValueError(f"inventory method {index} is not an object")
        key = rpc_key(raw)
        item = observed.get(key)
        if item is None:
            raise ValueError(f"runtime receipt is missing {key}")
        typed = raw.get("typed_request")
        if not isinstance(typed, dict) or not isinstance(typed.get("serialized_hex"), str):
            raise ValueError(
                f"{key}: inventory lacks canonical typed_request.serialized_hex; use the current Rust assembler"
            )
        status, code = terminal(item, key)
        request_b64 = item.get("request_base64")
        request_hex = b64_hex(request_b64, f"{key}.request_base64")
        request_digest = item.get("request_sha256")
        if not isinstance(request_digest, str):
            request_digest = f"sha256:{sha256_hex(request_hex, f'{key}.request_base64')}"
        expected = dict(raw)
        expected["request_sha256"] = request_digest
        expected["status"] = "semantic_passed" if status in {"ok", "canceled"} else "error"
        expected["terminal_status"] = status
        expected["terminal_code"] = code
        expected["request_bytes_hex"] = request_hex
        expected["response_type_observed"] = raw.get("response_type")
        expected["response_type_id_observed"] = raw.get("response_type")

        if isinstance(typed, dict) and isinstance(typed.get("serialized_frames"), list):
            # The additional-language receipts currently expose one aggregate
            # request. Let the canonical verifier reject client streams until
            # the generated consumer records each input frame separately.
            request_frames = item.get("request_frames_base64")
            if isinstance(request_frames, list):
                expected["request_frames_hex"] = [
                    b64_hex(frame, f"{key}.request_frames_base64[{frame_index}]")
                    for frame_index, frame in enumerate(request_frames)
                ]

        response_hexes = frame_hexes(item, key)
        response_digests = item.get("response_sha256")
        if not isinstance(response_digests, list):
            response_digests = [f"sha256:{hashlib.sha256(bytes.fromhex(frame)).hexdigest()}" for frame in response_hexes]
        expected["response_frames_sha256"] = response_digests
        if raw.get("server_streaming"):
            expected["response_frames_hex"] = response_hexes
            expected["response_frames"] = [{} for _ in response_hexes]
            expected["response_frame_types"] = [raw.get("response_type")] * len(response_hexes)
            expected["response_frame_type_ids"] = [raw.get("response_type")] * len(response_hexes)
        else:
            if len(response_hexes) != 1:
                raise ValueError(f"{key}: unary RPC must produce exactly one response frame")
            expected["response_bytes_hex"] = response_hexes[0]
            expected["response"] = {}
            expected["response_sha256"] = response_digests[0] if response_digests else f"sha256:{hashlib.sha256(bytes.fromhex(response_hexes[0])).hexdigest()}"
        output_methods.append(expected)

    if set(observed) != {rpc_key(method) for method in methods}:
        extra = sorted(set(observed) - {rpc_key(method) for method in methods})
        raise ValueError(f"runtime receipt contains unexpected RPCs: {extra}")
    return {
        "schema": f"acyclic.sdk.rpd.{language}-live-receipt.v1",
        "authority": authority,
        "method_count": len(output_methods),
        "methods": output_methods,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--language", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    value = normalize(load(args.inventory), load(args.receipt), args.language)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(value, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
