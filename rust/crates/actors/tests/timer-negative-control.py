"""Require the production-lock model to catch disabled timer-fire deduplication."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parents[4]
source = root / "rust/crates/actors/src/timers.rs"
original = source.read_text()
check = "idempotency_key: Some(timer.fire_key_validated()?),"
assert original.count(check) == 1, "update the production mutation"
try:
    source.write_text(original.replace(check, "idempotency_key: None,"))
    result = subprocess.run(
        ["node", "scripts/qualify-mcp-loom.mjs", "--timer"], cwd=root,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert result.returncode != 0, "disabled timer deduplication was not detected"
    assert "timer fire retry changed committed receipt" in result.stdout, result.stdout
    print("negative control: production-lock model detected disabled timer deduplication", flush=True)
finally:
    source.write_text(original)
