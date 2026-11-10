"""Artifact admission controls over the caller's original Cargo output."""
import json
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest

MODULE = runpy.run_path(str(Path(__file__).with_name("qualify-graphcoder-run.py")))

if len(sys.argv) != 2:
    raise SystemExit("step=admit Cargo artifact control: expected original Cargo JSON; "
                     "recovery: python3 scripts/qualify-graphcoder-run.test.py .sessions-shipping.jsonl")
MANIFEST = Path(sys.argv.pop()).resolve()


class QualificationPaths(unittest.TestCase):
    def test_original_artifacts_are_executable_and_exports_preserve_their_paths(self):
        selected = MODULE["binaries"](MANIFEST)
        for path in selected.values():
            subprocess.run([path, "--help"], check=True, stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
        exports = subprocess.check_output(
            [sys.executable, str(Path(__file__).with_name("graphcoder-binaries.py")),
             str(MANIFEST)], text=True)
        observed = subprocess.check_output(
            ["bash", "-c", exports + '\nprintf "%s\\n%s\\n" '
             '"$SESSIONS_ACYCLIC_BINARY" "$SESSIONS_MOCK_BINARY"'], text=True)
        self.assertEqual(observed.splitlines(), list(selected.values()))

    def test_missing_test_and_ambiguous_artifacts_are_rejected(self):
        # Negative controls mutate captured compiler output, not invented Cargo rows.
        original = [json.loads(line) for line in MANIFEST.read_text(encoding="utf-8").splitlines()]
        artifact = next(row for row in original
                        if row.get("reason") == "compiler-artifact"
                        and row.get("target", {}).get("name") == "acyclic"
                        and row.get("executable") and not row.get("profile", {}).get("test"))
        other = MODULE["binaries"](MANIFEST)["harness-mock-model"]
        without = [row for row in original if row is not artifact]
        mutations = [without,
                     [*without, dict(artifact, profile={**artifact["profile"], "test": True})],
                     [*original, dict(artifact, executable=other)]]
        with tempfile.TemporaryDirectory() as temporary:
            manifest = Path(temporary) / "artifacts.jsonl"
            for rows in mutations:
                manifest.write_text("\n".join(map(json.dumps, rows)), encoding="utf-8")
                with self.assertRaises(ValueError):
                    MODULE["binaries"](manifest)


if __name__ == "__main__":
    unittest.main()
