#!/usr/bin/env python3
"""Fail-closed validation for the installed Ruby/PHP/Dart Rust consumers.

The Rust inventory is the identity oracle. A transport return code is never a
pass: every RPC must appear exactly once, carry the Rust serialized request
(including a legitimate empty protobuf), and expose deserialized response
bytes or ordered streaming frame bytes. Where Rust emitted an expected
response digest, the observed wire bytes must match it.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import subprocess
import tempfile
import re
from pathlib import Path
from typing import Any


def load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def key(value: dict[str, Any]) -> str:
    return f"{value.get('package')}.{value.get('service')}/{value.get('method')}"


def sha256_hex(hex_value: str) -> str:
    return hashlib.sha256(bytes.fromhex(hex_value)).hexdigest()


def normalize_digest(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return None
    return value.removeprefix("sha256:").lower()


def validate_executed_package_binding(
    language: str, receipt: dict[str, Any], authority: dict[str, Any], strict_rust_oracle: bool
) -> dict[str, Any] | None:
    binding = receipt.get("executed_package")
    if binding is None:
        binding = receipt.get("package_binding")
    if binding is None:
        binding = receipt.get("package_provenance")
    if binding is None:
        if strict_rust_oracle:
            raise ValueError(f"{language}: executed package metadata is missing")
        return None
    if not isinstance(binding, dict):
        raise ValueError(f"{language}: executed package metadata must be an object")
    if binding.get("language") not in (None, language):
        raise ValueError(f"{language}: executed package language does not match receipt language")
    if binding.get("source_git_sha") != authority.get("source_git_sha"):
        raise ValueError(f"{language}: executed package source_git_sha does not match Rust authority")
    package_model = binding.get("model_digest", binding.get("rust_model_digest"))
    if package_model != authority.get("model_digest"):
        raise ValueError(f"{language}: executed package model digest does not match Rust authority")
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
            if not isinstance(digest, str) or not re.fullmatch(r"(?:sha256:)?[0-9a-fA-F]{64}", digest):
                raise ValueError(f"{language}: executed package {field}[{path!r}] is not a SHA-256")
    for field in ("artifact_sha256", "package_artifact_sha256", "provenance_sha256"):
        if field in binding and binding[field] is not None:
            digest = binding[field]
            if not isinstance(digest, str) or not re.fullmatch(r"(?:sha256:)?[0-9a-fA-F]{64}", digest):
                raise ValueError(f"{language}: executed package {field} is not a SHA-256")
    return binding


def canonical_terminal(value: Any) -> Any:
    """Normalize Rust's British spelling to the wire receipt spelling."""
    return "canceled" if value == "cancelled" else value


def parse_receipt(arg: str) -> tuple[str, Path]:
    language, sep, path = arg.partition("=")
    if not sep or not language or not path:
        raise SystemExit(f"invalid receipt {arg!r}; expected LANG=PATH")
    return language, Path(path)


def validate_receipt(
    language: str,
    receipt: dict[str, Any],
    expected: dict[str, dict[str, Any]],
    authority: dict[str, Any],
    strict_rust_oracle: bool,
) -> dict[str, Any]:
    receipt_authority = receipt.get("authority")
    if not isinstance(receipt_authority, dict):
        raise ValueError(f"{language}: receipt authority is missing")
    for field in ("source_git_sha", "model_digest"):
        if receipt_authority.get(field) != authority.get(field):
            raise ValueError(f"{language}: receipt authority {field} does not match Rust authority")
    validate_executed_package_binding(language, receipt, authority, strict_rust_oracle)
    methods = receipt.get("methods")
    if not isinstance(methods, list) or len(methods) != 106:
        raise ValueError(f"{language}: receipt must contain exactly 106 methods")
    observed: dict[str, dict[str, Any]] = {}
    for method in methods:
        if not isinstance(method, dict):
            raise ValueError(f"{language}: method receipt is not an object")
        rpc = key(method)
        if rpc in observed:
            raise ValueError(f"{language}: duplicate RPC {rpc}")
        observed[rpc] = method
    if set(observed) != set(expected):
        missing = sorted(set(expected) - set(observed))
        extra = sorted(set(observed) - set(expected))
        raise ValueError(f"{language}: Rust RPC identity mismatch; missing={missing}, extra={extra}")

    failures: list[str] = []
    for rpc, method in observed.items():
        oracle = expected[rpc]
        # `expected_status` is a producer/encoding label.  It is useful for
        # diagnostics, but it is not the terminal-status oracle.  Qualification
        # is anchored only to the Rust-observed terminal spelling and numeric
        # gRPC status code below.
        expected_terminal_raw = oracle.get("terminal_status")
        expected_terminal = canonical_terminal(expected_terminal_raw)
        if strict_rust_oracle:
            if not isinstance(expected_terminal_raw, str):
                failures.append(f"{rpc}: Rust terminal status oracle is absent")
            elif expected_terminal not in {"ok", "canceled"}:
                failures.append(f"{rpc}: Rust terminal status oracle is invalid")
            if not isinstance(oracle.get("terminal_code"), int):
                failures.append(f"{rpc}: Rust terminal code oracle is absent")
        typed = oracle.get("typed_request")
        if not isinstance(typed, dict) or "serialized_hex" not in typed:
            failures.append(f"{rpc}: Rust serialized request is absent")
            continue
        request_hex = typed["serialized_hex"]
        if not isinstance(request_hex, str):
            failures.append(f"{rpc}: serialized_hex is not a string")
            continue
        expected_request_digest = normalize_digest(typed.get("serialized_sha256"))
        if expected_request_digest != sha256_hex(request_hex):
            failures.append(f"{rpc}: Rust request digest is inconsistent")
        if normalize_digest(method.get("request_sha256")) != expected_request_digest:
            failures.append(f"{rpc}: consumer request bytes differ from Rust request bytes")
        expected_request_frames = typed.get("serialized_frames")
        if expected_request_frames is not None:
            if not isinstance(expected_request_frames, list) or not expected_request_frames:
                failures.append(f"{rpc}: Rust request frame oracle is malformed")
            observed_request_frames = method.get("request_frames_hex")
            observed_request_digests = method.get("request_frames_sha256")
            expected_frame_types = [frame.get("type") if isinstance(frame, dict) else None for frame in expected_request_frames] if isinstance(expected_request_frames, list) else []
            observed_frame_types = method.get("request_frame_type_ids")
            if strict_rust_oracle and any(not isinstance(value, str) or not value for value in expected_frame_types):
                failures.append(f"{rpc}: Rust request frame type oracle is absent or malformed")
            if expected_frame_types and (not isinstance(observed_frame_types, list) or observed_frame_types != expected_frame_types):
                failures.append(f"{rpc}: request frame type order differs from Rust")
            if not isinstance(observed_request_frames, list) or len(observed_request_frames) != len(expected_request_frames):
                failures.append(f"{rpc}: request frame count differs from Rust")
            elif not isinstance(observed_request_digests, list) or len(observed_request_digests) != len(expected_request_frames):
                failures.append(f"{rpc}: request frame digests are missing or unordered")
            else:
                for index, expected_frame in enumerate(expected_request_frames):
                    if not isinstance(expected_frame, dict) or not isinstance(expected_frame.get("serialized_hex"), str):
                        failures.append(f"{rpc}: Rust request frame {index} is malformed")
                        continue
                    expected_frame_hex = expected_frame["serialized_hex"]
                    expected_frame_digest = normalize_digest(expected_frame.get("serialized_sha256")) or sha256_hex(expected_frame_hex)
                    if str(observed_request_frames[index]).lower() != expected_frame_hex.lower():
                        failures.append(f"{rpc}: request frame {index} bytes differ from Rust")
                    if normalize_digest(observed_request_digests[index]) != expected_frame_digest:
                        failures.append(f"{rpc}: request frame {index} digest differs from Rust")
                    if normalize_digest(observed_request_digests[index]) != sha256_hex(observed_request_frames[index]):
                        failures.append(f"{rpc}: request frame {index} digest mismatch")

        status = method.get("status")
        if status in {"transport_success_pending_semantics", "pending_missing_typed_request", "error"}:
            failures.append(f"{rpc}: non-qualifying status {status}")
            continue
        if status != "semantic_passed":
            failures.append(f"{rpc}: missing semantic_passed status")
            continue

        observed_terminal_raw = method.get("terminal_status")
        observed_terminal = canonical_terminal(observed_terminal_raw)
        if not isinstance(observed_terminal, str) or observed_terminal not in {"ok", "canceled"}:
            failures.append(f"{rpc}: terminal status is missing or invalid")
        if isinstance(expected_terminal, str) and observed_terminal != expected_terminal:
            failures.append(f"{rpc}: terminal status {observed_terminal_raw!r} differs from Rust {expected_terminal_raw!r}")
        observed_code = method.get("terminal_code")
        expected_code = oracle.get("terminal_code")
        if not isinstance(observed_code, int):
            failures.append(f"{rpc}: numeric gRPC status code is missing")
        elif isinstance(expected_code, int) and observed_code != expected_code:
            failures.append(f"{rpc}: numeric gRPC status code differs from Rust")
        if observed_terminal == "ok" and observed_code != 0:
            failures.append(f"{rpc}: ok terminal status has non-OK numeric gRPC code")
        if observed_terminal == "canceled" and observed_code != 1:
            failures.append(f"{rpc}: canceled terminal status has non-CANCELLED numeric gRPC code")

        streaming = bool(oracle.get("server_streaming"))
        if streaming:
            expected_frame_digests = oracle.get("response_frames_sha256")
            if strict_rust_oracle and not isinstance(expected_frame_digests, list):
                failures.append(f"{rpc}: Rust ordered response frame oracle is absent")
            frames = method.get("response_frames_hex")
            if not isinstance(frames, list):
                failures.append(f"{rpc}: ordered response_frames_hex is missing")
                continue
            if any(not isinstance(frame, str) for frame in frames):
                failures.append(f"{rpc}: response frame bytes are not strings")
                continue
            decoded_frames = method.get("response_frames")
            if not isinstance(decoded_frames, list) or len(decoded_frames) != len(frames):
                failures.append(f"{rpc}: decoded response_frames are missing")
            frame_types = method.get("response_frame_types")
            if not isinstance(frame_types, list) or len(frame_types) != len(frames) or any(not isinstance(value, str) or not value for value in frame_types):
                failures.append(f"{rpc}: response frame types are missing or unordered")
            frame_digests = method.get("response_frames_sha256")
            if not isinstance(frame_digests, list) or len(frame_digests) != len(frames):
                failures.append(f"{rpc}: response frame digests are missing or unordered")
            frame_type_ids = method.get("response_frame_type_ids")
            if not isinstance(frame_type_ids, list) or len(frame_type_ids) != len(frames) or any(not isinstance(value, str) or not value for value in frame_type_ids):
                failures.append(f"{rpc}: canonical response frame type ids are missing or unordered")
            expected_frame_type_ids = oracle.get("response_frame_type_ids")
            if isinstance(expected_frame_type_ids, list):
                if any(not isinstance(value, str) or not value for value in expected_frame_type_ids):
                    failures.append(f"{rpc}: Rust response frame type oracle is malformed")
                elif frame_type_ids != expected_frame_type_ids:
                    failures.append(f"{rpc}: response frame type order differs from Rust")
            elif not isinstance(expected_frame_type_ids, list) and frame_type_ids != [oracle.get("response_type")] * len(frames):
                failures.append(f"{rpc}: response frame type ids differ from the Rust descriptor response type")
            if isinstance(expected_frame_digests, list):
                if len(expected_frame_digests) != len(frames):
                    failures.append(f"{rpc}: response frame count differs from Rust")
                else:
                    for index, expected_frame_digest in enumerate(expected_frame_digests):
                        if normalize_digest(frame_digests[index] if isinstance(frame_digests, list) and index < len(frame_digests) else None) != normalize_digest(expected_frame_digest):
                            failures.append(f"{rpc}: response frame {index} bytes differ from Rust")
            for index, frame in enumerate(frames):
                try:
                    if normalize_digest(frame_digests[index]) != sha256_hex(frame):
                        failures.append(f"{rpc}: response frame {index} digest mismatch")
                except (IndexError, TypeError, ValueError):
                    failures.append(f"{rpc}: response frame {index} digest missing or invalid")
        else:
            if "response_bytes_hex" not in method or "response" not in method:
                failures.append(f"{rpc}: deserialized response bytes/value are missing")
                continue
            if not isinstance(method.get("response_type_observed"), str) or not method["response_type_observed"]:
                failures.append(f"{rpc}: deserialized response type is missing")
            observed_type_id = method.get("response_type_id_observed")
            expected_type_id = oracle.get("response_type")
            if not isinstance(observed_type_id, str) or not observed_type_id:
                failures.append(f"{rpc}: canonical deserialized response type id is missing")
            elif observed_type_id != expected_type_id:
                failures.append(f"{rpc}: canonical response type id differs from Rust")
            response_hex = method["response_bytes_hex"]
            if not isinstance(response_hex, str):
                failures.append(f"{rpc}: response bytes are not a string")
                continue
            if normalize_digest(method.get("response_sha256")) != sha256_hex(response_hex):
                failures.append(f"{rpc}: response digest mismatch")
            expected_response = normalize_digest(oracle.get("response_sha256"))
            if expected_response is None and isinstance(oracle.get("response_base64"), str):
                expected_response = hashlib.sha256(base64.b64decode(oracle["response_base64"])).hexdigest()
            if strict_rust_oracle and expected_response is None:
                failures.append(f"{rpc}: Rust expected unary response bytes are absent")
            if expected_response is not None and normalize_digest(method.get("response_sha256")) != expected_response:
                failures.append(f"{rpc}: response bytes differ from Rust expected response")

    if failures:
        raise ValueError(f"{language}: semantic validation failed: " + "; ".join(failures[:12]))
    return {"language": language, "method_count": len(methods), "status": "semantic_passed"}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--receipt", action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--strict-rust-oracle", action="store_true")
    parser.add_argument("--rust-verifier", type=Path)
    args = parser.parse_args()
    inventory = load(args.manifest)
    methods = inventory.get("methods")
    if not isinstance(methods, list) or len(methods) != 106:
        raise SystemExit("Rust consumer inventory must contain exactly 106 methods")
    expected = {key(method): method for method in methods}
    if len(expected) != 106:
        raise SystemExit("Rust consumer inventory contains duplicate RPC identities")
    authority = inventory.get("authority")
    if not isinstance(authority, dict):
        raise SystemExit("Rust consumer inventory authority is missing")
    if args.strict_rust_oracle and args.rust_verifier is None:
        raise SystemExit("strict qualification requires the Rust RPC observation verifier")
    if args.strict_rust_oracle and not args.rust_verifier.is_file():
        raise SystemExit(f"Rust RPC observation verifier is missing: {args.rust_verifier}")
    verifier_sha256 = hashlib.sha256(args.rust_verifier.read_bytes()).hexdigest() if args.strict_rust_oracle else None
    results = []
    for raw in args.receipt:
        language, path = parse_receipt(raw)
        if args.strict_rust_oracle:
            with tempfile.TemporaryDirectory(prefix="rpd-rust-verifier-") as temp:
                verifier_output = Path(temp) / "result.json"
                command = [
                    str(args.rust_verifier),
                    "--expected", str(args.manifest),
                    "--observed", str(path),
                    "--source-git-sha", str(authority.get("source_git_sha", "")),
                    "--verifier-sha256", verifier_sha256,
                    "--output", str(verifier_output),
                ]
                completed = subprocess.run(command, capture_output=True, text=True)
                if completed.returncode != 0:
                    detail = (completed.stderr or completed.stdout).strip()
                    raise SystemExit(f"Rust RPC observation verifier rejected {language}: {detail}")
                if not verifier_output.is_file():
                    raise SystemExit(f"Rust RPC observation verifier produced no output for {language}")
                verifier_result = load(verifier_output)
                if verifier_result.get("status") != "passed":
                    raise SystemExit(f"Rust RPC observation verifier did not report passed for {language}")
                if verifier_result.get("source_git_sha") != authority.get("source_git_sha"):
                    raise SystemExit(f"Rust RPC observation verifier source_git_sha mismatch for {language}")
                if verifier_result.get("verifier_sha256") != verifier_sha256:
                    raise SystemExit(f"Rust RPC observation verifier executable hash mismatch for {language}")
        results.append(validate_receipt(language, load(path), expected, authority, args.strict_rust_oracle))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"schema": "acyclic.sdk.rpd.semantic-receipt.v1", "languages": results}, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
