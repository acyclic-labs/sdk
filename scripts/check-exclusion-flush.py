#!/usr/bin/env python3
"""Observe the actual publisher's file flushes, including existing-rule reads."""
import pathlib
import re
import subprocess
import sys
import tempfile

with tempfile.TemporaryDirectory(prefix="acyclic-exclusion-flush-") as temporary:
    trace = pathlib.Path(temporary) / "syscalls"
    result = subprocess.run(
        ["strace", "-f", "-yy", "-e", "trace=fsync,fdatasync", "-o", str(trace),
         sys.argv[1],
         "local::owner::exclusion::tests::held_metadata_excludes_only_owned_tokens_and_preserves_opaque_user_bytes",
         "--exact", "--nocapture"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=9.5,
    )
    print(result.stdout, flush=True)
    assert result.returncode == 0, result.returncode
    assert re.search(r"test result: ok\. 1 passed; 0 failed; 0 ignored;", result.stdout)
    flushes = re.findall(
        r"(?:fsync|fdatasync)\(\d+<[^>]+/\.git/info/exclude>\)\s+= 0",
        trace.read_text(),
    )
    # The same common exclude file is published twice for each of two roots.
    # The second publication appends nothing but must settle the retained rule.
    assert len(flushes) == 4, f"expected four real exclude-file flushes, saw {len(flushes)}"
    print("four exclusion publications performed four actual file flushes", flush=True)
