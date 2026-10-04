"""Fail-closed validation for installed Python/Go runtime observations.

The validator accepts only executed calls with raw request frames, response
frames (or an explicit terminal status for an empty stream), and a complete
one-to-one match with the Rust authority inventory.  Method names in a
receipt, constructor reachability, or response counts without bytes never
qualify a language.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from pathlib import Path
from typing import Any


def encoded_frame(value: Any, label: str) -> bytes:
    if not isinstance(value, dict):
        raise ValueError(f"{label} is not an object")
    encoded = value.get("bytes_base64")
    if not isinstance(encoded, str):
        raise ValueError(f"{label}.bytes_base64 is missing")
    try:
        raw = base64.b64decode(encoded, validate=True)
    except Exception as exc:  # pragma: no cover - message is part of gate output
        raise ValueError(f"{label}.bytes_base64 is invalid: {exc}") from exc
    expected = value.get("sha256")
    actual = "sha256:" + hashlib.sha256(raw).hexdigest()
    if expected not in (None, actual, actual.removeprefix("sha256:")):
        raise ValueError(f"{label}.sha256 differs from encoded bytes")
    if not isinstance(value.get("sequence"), int):
        raise ValueError(f"{label}.sequence is missing")
    return raw


def authority_inventory(path: Path) -> dict[str, tuple[str, str]]:
    root = json.loads(path.read_text(encoding="utf-8"))
    inventory: dict[str, tuple[str, str]] = {}
    for family in root.get("families", []):
        family_name = str(family.get("source", "")).split("/", 1)[0]
        for method in family.get("rpc_methods", []):
            rpc = method["rpc"]
            if rpc in inventory:
                raise ValueError(f"Rust authority repeats {rpc}")
            inventory[rpc] = (family_name, method["shape"])
    if len(inventory) != 106:
        raise ValueError(f"Rust authority contains {len(inventory)} RPCs; expected 106")
    return inventory


def scenario_values(path: Path) -> list[dict[str, Any]]:
    root = json.loads(path.read_text(encoding="utf-8"))
    values = root.get("scenarios") if isinstance(root, dict) else root
    if not isinstance(values, list):
        raise ValueError(f"{path} must contain a scenarios array")
    output: list[dict[str, Any]] = []
    for index, value in enumerate(values):
        if not isinstance(value, dict):
            raise ValueError(f"{path} scenario {index} is not an object")
        nested = value.get("output_path")
        if isinstance(nested, str) and not value.get("request_frames"):
            candidate = (path.parent / nested).resolve()
            if candidate.is_file():
                value = json.loads(candidate.read_text(encoding="utf-8"))
        output.append(value)
    return output


def validate(authority_path: Path, observed_paths: list[Path], revision: str | None) -> dict[str, Any]:
    expected = authority_inventory(authority_path)
    observed: dict[str, dict[str, Any]] = {}
    failures: list[str] = []
    for path in observed_paths:
        for index, value in enumerate(scenario_values(path)):
            rpc = value.get("rpc")
            label = f"{path}:{index}:{rpc or '<missing>'}"
            if not isinstance(rpc, str):
                failures.append(f"{label} has no RPC identity")
                continue
            if rpc in observed:
                failures.append(f"{label} duplicates an observed RPC")
                continue
            observed[rpc] = value
            if value.get("source_revision") and revision and value["source_revision"] != revision:
                failures.append(f"{label} source revision differs")
            if value.get("invoked") is not True:
                failures.append(f"{label} was not invoked")
            if value.get("execution_mode") != "remote":
                failures.append(f"{label} is not a remote execution")
            if value.get("status") != "passed" or value.get("exit_code") != 0:
                failures.append(f"{label} did not pass: status={value.get('status')} exit={value.get('exit_code')}")
            frames = value.get("request_frames")
            if not isinstance(frames, list) or not frames:
                failures.append(f"{label} has no request frames")
            else:
                for frame_index, item in enumerate(frames):
                    try:
                        encoded_frame(item, f"{label}.request_frames[{frame_index}]")
                    except ValueError as exc:
                        failures.append(str(exc))
            responses = value.get("response_frames")
            terminal = value.get("terminal")
            if not isinstance(responses, list):
                failures.append(f"{label} has no response_frames array")
            elif responses:
                for frame_index, item in enumerate(responses):
                    try:
                        encoded_frame(item, f"{label}.response_frames[{frame_index}]")
                    except ValueError as exc:
                        failures.append(str(exc))
            elif not isinstance(terminal, dict) or not isinstance(terminal.get("code"), str):
                failures.append(f"{label} has neither response frames nor a terminal status")
            if isinstance(frames, list) and [f.get("sequence") for f in frames] != list(range(len(frames))):
                failures.append(f"{label} request frame order is not contiguous")
            if isinstance(responses, list) and [f.get("sequence") for f in responses] != list(range(len(responses))):
                failures.append(f"{label} response frame order is not contiguous")
    for rpc, (family, shape) in expected.items():
        value = observed.get(rpc)
        if value is None:
            failures.append(f"missing observed RPC {rpc}")
            continue
        if value.get("family") != family:
            failures.append(f"{rpc} family differs: expected {family}, observed {value.get('family')}")
        if value.get("shape") != shape:
            failures.append(f"{rpc} shape differs: expected {shape}, observed {value.get('shape')}")
    for rpc in observed:
        if rpc not in expected:
            failures.append(f"unexpected observed RPC {rpc}")
    return {
        "schema": "acyclic.sdk.python-go.runtime-frame-validation.v1",
        "status": "passed" if not failures else "failed",
        "authority": str(authority_path),
        "source_revision": revision,
        "expected_rpc_count": len(expected),
        "observed_rpc_count": len(observed),
        "failures": failures,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority", required=True, type=Path)
    parser.add_argument("--observed", required=True, type=Path, action="append")
    parser.add_argument("--source-revision")
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args()
    result = validate(args.authority, args.observed, args.source_revision)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    if result["status"] != "passed":
        for failure in result["failures"]:
            print(failure)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
