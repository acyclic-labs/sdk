import json
import os
from pathlib import Path
import runpy
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

MODULE = runpy.run_path(str(Path(__file__).with_name("qualify-graphcoder-run.py")))


class QualificationPaths(unittest.TestCase):
    def fixture(self, root):
        rows, paths = [], []
        for name in ("graphcoder", "harness-mock-model"):
            path = root / "target with 'quotes'" / "x86_64-unknown-linux-gnu/debug" / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.touch()
            paths.append(str(path))
            rows.append({"reason": "compiler-artifact", "target": {"name": name, "kind": ["bin"]},
                         "profile": {"test": False}, "executable": str(path)})
        manifest = root / "artifacts.jsonl"
        manifest.write_text("\n".join(map(json.dumps, rows)))
        return manifest, paths, rows

    def test_actual_target_paths_and_failed_suite_exit_are_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest, paths, _ = self.fixture(root)
            with patch.dict(os.environ, {"E2E_OUTPUT": temporary}, clear=True), patch("subprocess.run") as run:
                run.return_value = subprocess.CompletedProcess([], 1)
                self.assertEqual(MODULE["main"](manifest), 1)
                self.assertEqual(run.call_args.args[0], ["node", "scripts/graphcoder-e2e/serial.mjs", *paths])
                self.assertEqual(json.loads((root / "wall.json").read_text())["exit"], 1)
            # Negative control: the previous directory guess cannot name the artifact.
            with self.assertRaises(FileNotFoundError):
                subprocess.run([str(root / "debug/graphcoder")], check=True)
            output = subprocess.check_output([sys.executable, str(Path(__file__).with_name("graphcoder-binaries.py")), str(manifest)], text=True)
            selected = subprocess.check_output(["bash", "-c", output + '\nprintf "%s\\n%s\\n" "$SESSIONS_GRAPHCODER_BINARY" "$SESSIONS_MOCK_BINARY"'], text=True)
            self.assertEqual(selected.splitlines(), paths)

    def test_absent_ambiguous_and_test_artifacts_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            manifest, paths, rows = self.fixture(Path(temporary))
            other = Path(paths[0]).with_name("other")
            other.touch()
            for malformed in ([], [dict(rows[0], profile={"test": True}), rows[1]],
                              [*rows, dict(rows[0], executable=str(other))]):
                manifest.write_text("\n".join(map(json.dumps, malformed)))
                with self.assertRaises(ValueError):
                    MODULE["binaries"](manifest)
            manifest.write_text("\n".join(map(json.dumps, rows)))
            Path(paths[0]).unlink()
            with self.assertRaises(ValueError):
                MODULE["binaries"](manifest)


if __name__ == "__main__":
    unittest.main()
