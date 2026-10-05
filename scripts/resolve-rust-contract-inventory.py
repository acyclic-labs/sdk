#!/usr/bin/env python3
"""Resolve the current Rust contract from a generated product directory.

Generated products can carry a small immutable compatibility tail while the
Rust authority manifest describes the current surface.  Qualification must
feed only the manifest's sources to protoc and must report the compatibility
tail separately.  This helper keeps that boundary in one deterministic place
instead of making each language lane infer it from filenames or RPC counts.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

RPC_RE = re.compile(r"^\s*rpc\s+([A-Za-z_][A-Za-z0-9_]*)\s*\([^)]*\)\s+returns\s*\(\s*(?:stream\s+)?[^)]+\)", re.MULTILINE)
PACKAGE_RE = re.compile(r"^\s*package\s+([A-Za-z_][A-Za-z0-9_.]*)\s*;", re.MULTILINE)
SERVICE_RE = re.compile(r"^\s*service\s+([A-Za-z_][A-Za-z0-9_]*)\s*\{", re.MULTILINE)


def rpc_identities(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8")
    package = (PACKAGE_RE.search(text) or [None, ""])[1]
    services = list(SERVICE_RE.finditer(text))
    result: list[str] = []
    for index, service in enumerate(services):
        start = service.end()
        end = services[index + 1].start() if index + 1 < len(services) else len(text)
        # A service closes before the next service.  The RPC grammar is line
        # oriented in the Rust exporter, so the brace depth check also keeps
        # a later service's methods out of this one.
        body = text[start:end]
        depth = 1
        body_lines: list[str] = []
        for line in body.splitlines():
            depth += line.count("{") - line.count("}")
            if depth <= 0:
                break
            body_lines.append(line)
        service_text = "\n".join(body_lines)
        for match in RPC_RE.finditer(service_text):
            result.append(f"{package + '.' if package else ''}{service.group(1)}/{match.group(1)}")
    return result


def authority_methods(document: dict) -> set[str]:
    methods: set[str] = set()
    for family in document.get("families", []):
        for item in family.get("rpc_methods", []):
            if isinstance(item, str):
                match = re.fullmatch(r"@\{rpc=([^;]+); shape=(?:unary|client|server|bidi)\}", item)
                if not match:
                    raise ValueError(f"invalid Rust authority RPC method: {item!r}")
                rpc = match.group(1)
            elif isinstance(item, dict) and isinstance(item.get("rpc"), str):
                rpc = item["rpc"]
            else:
                raise ValueError("invalid Rust authority RPC method entry")
            if rpc in methods:
                raise ValueError(f"duplicate Rust authority RPC method: {rpc}")
            methods.add(rpc)
    return methods


def resolve(product_root: Path) -> dict:
    manifest_path = product_root / "rust-authority.json"
    document = json.loads(manifest_path.read_text(encoding="utf-8"))
    current_sources = [str(f["source"]).replace("\\", "/") for f in document.get("families", [])]
    if len(current_sources) != len(set(current_sources)):
        raise ValueError("Rust authority manifest repeats a family source")
    authority = authority_methods(document)
    # The control-plane schema is emitted beside the family contracts, but it
    # is deliberately outside `families` in the Rust authority manifest.  Its
    # handshake RPC must therefore never be counted as an archived family RPC
    # (or passed to a downstream family generator).  Read the identity from
    # the manifest instead of hard-coding a path so a future versioned control
    # plane remains unambiguous.
    control_source = (
        document.get("control_plane", {}).get("source")
        if isinstance(document.get("control_plane"), dict)
        else None
    )
    control_source = str(control_source).replace("\\", "/") if control_source else None
    proto_paths = sorted(
        path for path in product_root.rglob("*.proto")
        if "/validation/" not in path.as_posix()
        and path.relative_to(product_root).as_posix() != control_source
    )
    by_relative = {path.relative_to(product_root).as_posix(): path for path in proto_paths}
    missing = sorted(set(current_sources) - set(by_relative))
    if missing:
        raise ValueError("Rust authority sources are missing from product output: " + ", ".join(missing))
    discovered: dict[str, str] = {}
    for path in proto_paths:
        relative = path.relative_to(product_root).as_posix()
        for rpc in rpc_identities(path):
            if rpc in discovered:
                raise ValueError(f"duplicate generated RPC identity {rpc}: {discovered[rpc]} and {relative}")
            discovered[rpc] = relative
    missing_methods = sorted(authority - discovered.keys())
    if missing_methods:
        raise ValueError("Rust authority RPCs are missing from product output: " + ", ".join(missing_methods))
    archived_methods = sorted(set(discovered) - authority)
    current_methods = sorted(authority)
    archived_sources = sorted({discovered[rpc] for rpc in archived_methods})
    current_sources_present = [source for source in current_sources if source in by_relative]
    return {
        "schema": "acyclic.sdk.rust-contract-inventory.v1",
        "authority_manifest": manifest_path.name,
        "current_proto_paths": current_sources_present,
        "archived_proto_paths": archived_sources,
        "current_rpc_count": len(current_methods),
        "archived_rpc_count": len(archived_methods),
        "all_rpc_count": len(discovered),
        "current_rpc_identities": current_methods,
        "archived_rpc_identities": archived_methods,
        "control_proto_path": control_source,
    }


def main() -> int:
    if len(sys.argv) not in (3, 4):
        print("usage: resolve-rust-contract-inventory.py PRODUCT_ROOT OUTPUT_JSON [--current-protos|--counts]", file=sys.stderr)
        return 2
    product_root = Path(sys.argv[1]).resolve()
    output = Path(sys.argv[2])
    try:
        inventory = resolve(product_root)
        if inventory["archived_rpc_count"] not in (0, 6):
            raise ValueError(
                "generated product mixes an unexpected archived RPC tail: "
                f"{inventory['archived_rpc_count']} (expected 0 or the immutable six-entry tail)"
            )
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"Rust contract inventory error: {error}", file=sys.stderr)
        return 1
    if len(sys.argv) == 3:
        output.write_text(json.dumps(inventory, indent=2) + "\n", encoding="utf-8")
        return 0
    mode = sys.argv[3]
    if mode == "--current-protos":
        print("\n".join(str(product_root / path) for path in inventory["current_proto_paths"]))
    elif mode == "--counts":
        print(inventory["current_rpc_count"], inventory["archived_rpc_count"], inventory["all_rpc_count"])
    else:
        print(f"unknown mode: {mode}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
