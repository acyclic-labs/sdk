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
import re
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


def normalized_digest(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{label} must be a SHA-256 digest")
    digest = value.removeprefix("sha256:").lower()
    if not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{label} must be a SHA-256 digest")
    return digest


def authority_binding(inventory: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    expected = inventory.get("authority")
    actual = receipt.get("authority")
    if not isinstance(expected, dict):
        raise ValueError("Rust RPD inventory authority is missing")
    if not isinstance(actual, dict):
        raise ValueError("runtime receipt authority is missing; use the Rust-owned receipt emitter")
    for field in ("source_git_sha", "model_digest", "source_file_hashes"):
        if actual.get(field) != expected.get(field):
            raise ValueError(f"runtime receipt authority {field} does not match the Rust producer authority")
    source_git_sha = expected.get("source_git_sha")
    if not isinstance(source_git_sha, str) or not re.fullmatch(r"[0-9a-f]{40}", source_git_sha):
        raise ValueError("Rust authority source_git_sha must be a 40-digit Git revision")
    hashes = expected.get("source_file_hashes")
    if not isinstance(hashes, dict) or not hashes:
        raise ValueError("Rust authority source_file_hashes must be a non-empty producer closure")
    for path, digest in hashes.items():
        normalized_digest(digest, f"Rust authority source_file_hashes[{path!r}]")
    source_revision = receipt.get("source_revision")
    if source_revision is not None and source_revision != source_git_sha:
        raise ValueError("runtime receipt source_revision does not match the Rust authority")
    manifest_digest = receipt.get("rust_authority_manifest_sha256")
    if manifest_digest is not None:
        normalized_digest(manifest_digest, "runtime receipt rust_authority_manifest_sha256")
    return expected


def executed_package_binding(receipt: dict[str, Any], language: str, authority: dict[str, Any]) -> dict[str, Any]:
    binding = receipt.get("executed_package")
    if binding is None:
        binding = receipt.get("package_binding")
    if binding is None:
        binding = receipt.get("package_provenance")
    if not isinstance(binding, dict):
        raise ValueError(f"{language}: runtime receipt executed_package metadata is missing")
    declared_language = binding.get("language")
    if declared_language is not None and declared_language != language:
        raise ValueError(f"{language}: executed package language does not match receipt language")
    if binding.get("source_git_sha") != authority.get("source_git_sha"):
        raise ValueError(f"{language}: executed package source_git_sha does not match Rust authority")
    package_model = binding.get("model_digest", binding.get("rust_model_digest"))
    if package_model != authority.get("model_digest"):
        raise ValueError(f"{language}: executed package model digest does not match Rust authority")
    # A generated package may have a larger generated-input closure than the
    # Rust producer closure. Keep those roles separate: only an explicitly
    # named producer closure is required to equal the authority closure.
    producer_closure = binding.get("producer_source_file_hashes")
    if producer_closure is not None and producer_closure != authority.get("source_file_hashes"):
        raise ValueError(f"{language}: executed package producer source closure does not match Rust authority")
    for field in ("source_file_hashes", "generated_source_file_hashes", "package_source_file_hashes"):
        hashes = binding.get(field)
        if hashes is None:
            continue
        if not isinstance(hashes, dict) or not hashes:
            raise ValueError(f"{language}: executed package {field} must be a non-empty hash map")
        for path, digest in hashes.items():
            normalized_digest(digest, f"{language}: executed package {field}[{path!r}]")
    for field in ("artifact_sha256", "package_artifact_sha256", "provenance_sha256"):
        if field in binding and binding[field] is not None:
            normalized_digest(binding[field], f"{language}: executed package {field}")
    return binding


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
    if frames is None:
        frames = observation.get("response_frames_base64")
    if frames is None:
        frames = observation.get("response_frames_hex")
        if not isinstance(frames, list):
            raise ValueError(f"{label}.response_frame_base64 or response_frames_hex must be an array")
        for index, frame in enumerate(frames):
            if not isinstance(frame, str):
                raise ValueError(f"{label}.response_frames_hex[{index}] must be hexadecimal text")
            try:
                bytes.fromhex(frame)
            except ValueError as error:
                raise ValueError(f"{label}.response_frames_hex[{index}] is not hexadecimal: {error}") from error
        return [frame.lower() for frame in frames]
    if not isinstance(frames, list):
        raise ValueError(f"{label}.response_frame_base64 must be an array")
    return [b64_hex(frame, f"{label}.response_frame_base64[{index}]") for index, frame in enumerate(frames)]


def normalize(inventory: dict[str, Any], receipt: dict[str, Any], language: str) -> dict[str, Any]:
    methods = inventory.get("methods")
    observations = receipt.get("observations")
    if not isinstance(methods, list) or len(methods) != 106:
        raise ValueError("Rust RPD inventory must contain exactly 106 methods")
    if not isinstance(observations, list):
        raise ValueError("runtime receipt observations are missing")
    authority = authority_binding(inventory, receipt)
    package_binding = executed_package_binding(receipt, language, authority)
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
        if request_b64 is not None:
            request_hex = b64_hex(request_b64, f"{key}.request_base64")
        elif isinstance(item.get("request_bytes_hex"), str):
            request_hex = item["request_bytes_hex"]
            # Validate the producer's hex before using it as evidence.
            bytes.fromhex(request_hex)
        else:
            raise ValueError(f"{key}.request_base64 or request_bytes_hex is missing")
        request_digest = item.get("request_sha256")
        if not isinstance(request_digest, str):
            request_digest = f"sha256:{sha256_hex(request_hex, f'{key}.request_base64')}"
        if request_digest.removeprefix("sha256:").lower() != sha256_hex(request_hex, f"{key}.request"):
            raise ValueError(f"{key}: request digest does not match request bytes")
        expected = dict(raw)
        expected["request_sha256"] = request_digest
        semantic_status = item.get("semantic_status")
        semantic_passed = item.get("semantic_passed") is True
        expected["status"] = (
            "semantic_passed"
            if semantic_passed or semantic_status in {"passed", "semantic_passed"}
            else "transport_success_pending_semantics"
            if status in {"ok", "canceled"}
            else "error"
        )
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
            if request_frames is None:
                request_frames = item.get("request_frame_base64")
            if isinstance(request_frames, list):
                expected["request_frames_hex"] = [
                    b64_hex(frame, f"{key}.request_frames_base64[{frame_index}]")
                    for frame_index, frame in enumerate(request_frames)
                ]
            elif isinstance(item.get("request_frames_hex"), list):
                expected["request_frames_hex"] = []
                for frame_index, frame in enumerate(item["request_frames_hex"]):
                    if not isinstance(frame, str):
                        raise ValueError(f"{key}.request_frames_hex[{frame_index}] must be hexadecimal text")
                    try:
                        bytes.fromhex(frame)
                    except ValueError as error:
                        raise ValueError(f"{key}.request_frames_hex[{frame_index}] is not hexadecimal: {error}") from error
                    expected["request_frames_hex"].append(frame.lower())

        response_hexes = frame_hexes(item, key)
        response_digests = item.get("response_sha256")
        if not isinstance(response_digests, list):
            response_digests = [f"sha256:{hashlib.sha256(bytes.fromhex(frame)).hexdigest()}" for frame in response_hexes]
        if len(response_digests) != len(response_hexes):
            raise ValueError(f"{key}: response digest count does not match response frame count")
        for frame_index, (frame, digest) in enumerate(zip(response_hexes, response_digests)):
            if normalized_digest(digest, f"{key}.response_sha256[{frame_index}]") != sha256_hex(frame, f"{key}.response_frame[{frame_index}]"):
                raise ValueError(f"{key}: response frame {frame_index} digest does not match response bytes")
        expected["response_frames_sha256"] = response_digests
        if raw.get("server_streaming"):
            expected["response_frames_hex"] = response_hexes
            decoded_frames = item.get("response_frames")
            if not isinstance(decoded_frames, list) or len(decoded_frames) != len(response_hexes):
                raise ValueError(f"{key}: semantic response_frames are missing or unordered")
            expected["response_frames"] = decoded_frames
            expected["response_frame_types"] = [raw.get("response_type")] * len(response_hexes)
            expected["response_frame_type_ids"] = [raw.get("response_type")] * len(response_hexes)
        else:
            if len(response_hexes) != 1:
                raise ValueError(f"{key}: unary RPC must produce exactly one response frame")
            expected["response_bytes_hex"] = response_hexes[0]
            decoded_response = item.get("response")
            if decoded_response is None:
                decoded_response = item.get("decoded_response")
            if decoded_response is None:
                raise ValueError(f"{key}: semantic response value is missing")
            expected["response"] = decoded_response
            observed_response_type = item.get("response_type_observed", item.get("response_type_id"))
            if isinstance(observed_response_type, str):
                expected["response_type_observed"] = observed_response_type
                expected["response_type_id_observed"] = item.get("response_type_id", observed_response_type)
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
        "executed_package": package_binding,
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
