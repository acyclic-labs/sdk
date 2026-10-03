#!/usr/bin/env python3
"""Preflight a Rust-derived OpenAPI document before a Forge comparison run.

This is a research harness, not product generation code. It deliberately uses
the Python standard library so a clean checkout can detect documentation and
wire-identity gaps without installing TypeScript or Forge. Forge remains an
optional downstream resolver/docs comparison in this loop.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any


SEMANTIC_EXTENSIONS = (
    "x-protobuf-json",
    "x-protobuf-presence",
    "x-protobuf-oneof",
    "x-protobuf-oneofs",
    "x-protobuf-enum",
)


def _walk(value: Any, location: str = ""):
    """Yield every JSON object and scalar extension location deterministically."""

    if isinstance(value, dict):
        yield value, location
        for key, child in value.items():
            child_location = f"{location}.{key}" if location else key
            yield from _walk(child, child_location)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _walk(child, f"{location}[{index}]")


def _issue(issues: list[dict[str, str]], code: str, location: str, message: str) -> None:
    issues.append({"code": code, "location": location, "message": message})


def analyze(document: dict[str, Any], source: str = "<memory>") -> dict[str, Any]:
    """Return a deterministic Forge preflight report for one OpenAPI document."""

    issues: list[dict[str, str]] = []
    paths = document.get("paths")
    if not isinstance(paths, dict):
        _issue(issues, "missing-paths", "paths", "OpenAPI document has no paths object")
        paths = {}

    components = document.get("components")
    if not isinstance(components, dict):
        components = {}
    schemas = components.get("schemas", {})
    if not isinstance(schemas, dict):
        _issue(issues, "invalid-schemas", "components.schemas", "schemas must be an object")
        schemas = {}

    operations: list[tuple[str, str, dict[str, Any]]] = []
    operation_ids: dict[str, str] = {}
    for path, path_item in sorted(paths.items()):
        if not isinstance(path_item, dict):
            _issue(issues, "invalid-path-item", f"paths.{path}", "path item must be an object")
            continue
        for method in ("get", "put", "post", "patch", "delete", "options", "head", "trace"):
            operation = path_item.get(method)
            if operation is None:
                continue
            location = f"paths.{path}.{method}"
            if not isinstance(operation, dict):
                _issue(issues, "invalid-operation", location, "operation must be an object")
                continue
            operation_id = operation.get("operationId")
            if not isinstance(operation_id, str) or not operation_id:
                _issue(issues, "missing-operation-id", location, "operationId is required")
                operation_id = f"{method} {path}"
            elif operation_id in operation_ids:
                _issue(
                    issues,
                    "duplicate-operation-id",
                    location,
                    f"operationId already used at {operation_ids[operation_id]}",
                )
            else:
                operation_ids[operation_id] = location
            operations.append((location, operation_id, operation))

            description = operation.get("description")
            if not isinstance(description, str) or not description.strip():
                _issue(
                    issues,
                    "missing-operation-description",
                    location,
                    "Forge docs metadata requires a non-empty operation description",
                )
            rpc = operation.get("x-protobuf-rpc")
            if not isinstance(rpc, str) or "/" not in rpc:
                _issue(
                    issues,
                    "missing-rpc-identity",
                    location,
                    "operation must retain x-protobuf-rpc service/method identity",
                )
            responses = operation.get("responses")
            if not isinstance(responses, dict) or not responses:
                _issue(
                    issues,
                    "missing-responses",
                    location,
                    "operation must retain at least one response for SDK/docs resolution",
                )

    references: list[tuple[str, str]] = []
    extension_counts = {extension: 0 for extension in SEMANTIC_EXTENSIONS}
    for value, location in _walk(document):
        if not isinstance(value, dict):
            continue
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/components/schemas/"):
            references.append((location, ref))
            target = ref.removeprefix("#/components/schemas/")
            if target not in schemas:
                _issue(issues, "unresolved-schema-ref", location, f"schema reference does not exist: {ref}")
        for extension in SEMANTIC_EXTENSIONS:
            if extension in value:
                extension_counts[extension] += 1

    provenance = document.get("x-acyclic-source")
    provenance_digest = (
        provenance.get("descriptor_sha256")
        if isinstance(provenance, dict)
        else None
    ) or (provenance.get("contract_digest") if isinstance(provenance, dict) else None)
    if not provenance_digest:
        _issue(
            issues,
            "missing-provenance",
            "x-acyclic-source",
            "Rust projection must carry a descriptor or contract digest for reproducibility",
        )

    operations_with_descriptions = sum(
        isinstance(operation.get("description"), str) and bool(operation["description"].strip())
        for _, _, operation in operations
    )
    operations_with_rpc = sum(
        isinstance(operation.get("x-protobuf-rpc"), str) and "/" in operation["x-protobuf-rpc"]
        for _, _, operation in operations
    )
    return {
        "tool": "acyclic.forge_probe",
        "source": source,
        "valid": not issues,
        "summary": {
            "operation_count": len(operations),
            "operations_with_descriptions": operations_with_descriptions,
            "description_coverage": (
                operations_with_descriptions / len(operations) if operations else 0.0
            ),
            "operations_with_rpc_identity": operations_with_rpc,
            "schema_count": len(schemas),
            "local_schema_reference_count": len(references),
            "semantic_extension_counts": extension_counts,
        },
        "operations": [
            {"location": location, "operation_id": operation_id}
            for location, operation_id, _ in operations
        ],
        "issues": issues,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("document", type=Path, help="Rust-generated OpenAPI JSON")
    parser.add_argument("--strict", action="store_true", help="return non-zero when preflight issues exist")
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.document.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        print(json.dumps({"tool": "acyclic.forge_probe", "valid": False, "error": str(error)}), file=sys.stderr)
        return 2
    if not isinstance(document, dict):
        print(json.dumps({"tool": "acyclic.forge_probe", "valid": False, "error": "root must be an object"}))
        return 2
    report = analyze(document, str(args.document))
    print(json.dumps(report, indent=2, sort_keys=True))
    return 1 if args.strict and not report["valid"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
