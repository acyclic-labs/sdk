"""Focused fail-closed tests for the Rust-bound Ruby/PHP/Dart receipt gate."""
from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path


ROOT = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location("validator", ROOT / "validate-live-receipts.py")
assert SPEC and SPEC.loader
validator = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(validator)


EMPTY_SHA = hashlib.sha256(b"").hexdigest()
FRAME_HEX = "01"
FRAME_SHA = hashlib.sha256(bytes.fromhex(FRAME_HEX)).hexdigest()


def fixtures() -> tuple[dict[str, dict], dict, dict]:
    expected: dict[str, dict] = {}
    methods = []
    for index in range(106):
        rpc = f"pkg.Service/Method{index}"
        streaming = index == 0
        expected[rpc] = {
            "package": "pkg",
            "service": "Service",
            "method": f"Method{index}",
            "response_type": "pkg.Response",
            "server_streaming": streaming,
            "typed_request": {"serialized_hex": "", "serialized_sha256": EMPTY_SHA},
            "terminal_status": "ok",
            "terminal_code": 0,
        }
        method = {
            "package": "pkg",
            "service": "Service",
            "method": f"Method{index}",
            "request_sha256": EMPTY_SHA,
            "status": "semantic_passed",
            "terminal_status": "ok",
            "terminal_code": 0,
        }
        if streaming:
            expected[rpc]["response_frames_sha256"] = [FRAME_SHA]
            method.update(
                response_frames=[{"value": 1}],
                response_frames_hex=[FRAME_HEX],
                response_frame_types=["GeneratedFrame"],
                response_frame_type_ids=["pkg.Response"],
                response_frames_sha256=[FRAME_SHA],
            )
        else:
            expected[rpc]["response_sha256"] = EMPTY_SHA
            method.update(
                response={},
                response_type_observed="GeneratedResponse",
                response_type_id_observed="pkg.Response",
                response_bytes_hex="",
                response_sha256=EMPTY_SHA,
            )
        if index == 1:
            expected[rpc]["typed_request"]["serialized_frames"] = [
                {"serialized_hex": FRAME_HEX, "serialized_sha256": FRAME_SHA, "type": "pkg.Request"}
            ]
            method["request_frames_hex"] = [FRAME_HEX]
            method["request_frames_sha256"] = [FRAME_SHA]
            method["request_frame_type_ids"] = ["pkg.Request"]
        if index == 2:
            # Rust records the canonical British spelling; wire receipts use
            # the canonical gRPC spelling and must compare as equivalent.
            expected[rpc]["terminal_status"] = "cancelled"
            expected[rpc]["terminal_code"] = 1
            method["terminal_status"] = "canceled"
            method["terminal_code"] = 1
        methods.append(method)
    inventory = {"methods": list(expected.values()), "authority": {"source_git_sha": "git", "model_digest": "model"}}
    receipt = {
        "authority": inventory["authority"],
        "executed_package": {
            "language": "test",
            "source_git_sha": "git",
            "model_digest": "model",
        },
        "methods": methods,
    }
    return expected, inventory["authority"], receipt


def expect_failure(receipt: dict, expected: dict, authority: dict, needle: str, strict: bool = True) -> None:
    try:
        validator.validate_receipt("test", receipt, expected, authority, strict)
    except ValueError as error:
        assert needle in str(error), str(error)
    else:
        raise AssertionError(f"receipt unexpectedly passed: {needle}")


expected, authority, receipt = fixtures()
validator.validate_receipt("test", receipt, expected, authority, True)

wrong_unary = {**receipt, "methods": [dict(method) for method in receipt["methods"]]}
wrong_unary["methods"][1]["response_bytes_hex"] = "00"
wrong_unary["methods"][1]["response_sha256"] = hashlib.sha256(b"\x00").hexdigest()
expect_failure(wrong_unary, expected, authority, "differ from Rust")

wrong_frame = {**receipt, "methods": [dict(method) for method in receipt["methods"]]}
wrong_frame["methods"][0]["response_frames_hex"] = ["02"]
wrong_frame["methods"][0]["response_frames_sha256"] = [hashlib.sha256(b"\x02").hexdigest()]
expect_failure(wrong_frame, expected, authority, "bytes differ from Rust")

wrong_request_frame = {**receipt, "methods": [dict(method) for method in receipt["methods"]]}
wrong_request_frame["methods"][1]["request_frames_hex"] = ["02"]
wrong_request_frame["methods"][1]["request_frames_sha256"] = [hashlib.sha256(b"\x02").hexdigest()]
expect_failure(wrong_request_frame, expected, authority, "request frame 0 bytes differ from Rust")

wrong_status = {**receipt, "methods": [dict(method) for method in receipt["methods"]]}
wrong_status["methods"][0]["terminal_status"] = "error"
expect_failure(wrong_status, expected, authority, "terminal status")

metadata_only = {rpc: dict(value) for rpc, value in expected.items()}
metadata_only["pkg.Service/Method1"]["rust_expected_status"] = "observed-ok"
validator.validate_receipt("test", receipt, metadata_only, authority, True)

missing_oracle = {rpc: dict(value) for rpc, value in expected.items()}
del missing_oracle["pkg.Service/Method1"]["terminal_status"]
expect_failure(receipt, missing_oracle, authority, "terminal status oracle is absent")

print("semantic receipt forged-payload/status tests passed")
