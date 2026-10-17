"""Check that removing production deadline admission trips the fuzz oracle."""
from pathlib import Path
import subprocess

root = Path(__file__).resolve().parent
source = root.parent / "src" / "timers.rs"
original = source.read_text()
check = "if self.deadline_unix_ms == 0 {"
assert original.count(check) == 1, "deadline check changed: update the mutation"
try:
    source.write_text(original.replace(check, "if false {"))
    result = subprocess.run(
        ["cargo", "+nightly", "fuzz", "run", "--dev", "timer_record",
         "corpus/timer_record/zero.json", "--", "-runs=1"],
        cwd=root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )
    assert result.returncode != 0, "bypassed deadline admission was not detected"
    assert "positive timer deadline" in result.stdout, result.stdout
    print("negative control: fuzz oracle detected removed deadline admission")
finally:
    source.write_text(original)
