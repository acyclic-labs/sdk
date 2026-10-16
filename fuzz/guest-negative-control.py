"""Require the constructor fuzz oracle to catch removed version admission."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
source = root.parent / "rust/crates/actors/src/guest/runtime.rs"
original = source.read_text()
check = "if version == 0 {"
assert original.count(check) == 1, "version check changed: update the mutation"
try:
    source.write_text(original.replace(check, "if false {"))
    result = subprocess.run(
        ["cargo", "+nightly", "fuzz", "run", "--dev", "guest_json",
         "corpus/guest_json/caller-zero-version.json", "--", "-runs=1"],
        cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert result.returncode != 0, "bypassed version admission was not detected"
    assert "caller capability admission" in result.stdout, result.stdout
    print("negative control: constructor fuzz oracle detected removed version admission")
finally:
    source.write_text(original)
