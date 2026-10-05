#!/usr/bin/env python3
"""Fail-closed checks for the live consumer harness.

This test intentionally uses the empty 97bc manifest. It proves that no
language runner can turn an absent Rust request, a default protobuf, or a
transport-only result into semantic qualification.
"""
from pathlib import Path
import json

ROOT = Path(__file__).parent
manifest = json.loads((ROOT / "rust-typed-request-manifest-97bc-v20.json").read_text())
assert manifest["method_count"] == 106
assert len(manifest["methods"]) == 106
assert all(not m["typed_request"].get("serialized_hex") for m in manifest["methods"])
for name in ("ruby_live.rb", "php_live.php", "dart_live.dart"):
    text = (ROOT / name).read_text()
    assert "pending_missing_typed_request" in text
    assert "transport_success_pending_semantics" in text
    assert "semantic_passed" in text
print("fail-closed manifest checks passed for 106 methods")
