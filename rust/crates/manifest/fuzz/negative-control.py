"""Require the independent fuzz oracle to detect a removed production check."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
source = root.parent / "src" / "validation.rs"
original = source.read_text()
check = 'scope(&grant.grant.resource, &inbox.name)\n                    && actions[grant.name.as_str()].contains("streams/append")'
assert original.count(check) == 1, "authority check changed: update the mutation"
try:
    source.write_text(original.replace(check, "true"))
    result = subprocess.run(
        ["cargo", "+nightly", "fuzz", "run", "--dev", "manifest_json",
         "corpus/manifest_json/inbox-authority.json", "--", "-runs=1"],
        cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert result.returncode != 0, "bypassed authority was not detected"
    assert "independent admission invariant" in result.stdout, result.stdout
    assert "inbox-authority" in result.stdout, result.stdout
    print("negative control: the fuzz oracle detected the removed inbox-authority check")
finally:
    source.write_text(original)
