"""Prove regressions reject removed argument, identity and admission boundaries."""

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path


CONTROLS = [
    ("unchecked runner bootstrap", "bootstrap.py",
     "if hashlib.sha256(payload).hexdigest() != UV_SHA256:", "if False:",
     "test_contracts.BenchmarkContracts.test_bootstrap_rejects_unpinned_bytes_before_archive_parsing"),
    ("unframed shell arguments", "contracts.py", "shlex.join(arguments)", '" ".join(arguments)',
     "test_contracts.BenchmarkContracts.test_shell_metacharacters_roundtrip_as_prompt_data_and_env_is_empty"),
    ("unchecked artifact construction", "contracts.py",
     "if not self.contents or hashlib.sha256(self.contents).hexdigest() != self.sha256:", "if False:",
     "test_contracts.BenchmarkContracts.test_artifact_identity_is_verified_at_every_construction_entry"),
    ("incomplete task inventory", "plan.py", ' or len(dataset["tasks"]) != count', '',
     "test_contracts.BenchmarkContracts.test_incomplete_or_ambiguous_task_universes_cannot_be_planned"),
    ("logging masks failed agent", "contracts.py",
     'str(PurePosixPath(logs) / "graphcoder.stderr")))',
     'str(PurePosixPath(logs) / "graphcoder.stderr")) + " ; true")',
     "test_contracts.BenchmarkContracts.test_log_redirection_preserves_agent_failure_status"),
    ("failed reinstall retains authority", "graphcoder_harbor.py",
     "async def install(self, environment):\n        self._installed_environment = None",
     "async def install(self, environment):",
     "test_harbor.HarborContracts.test_install_run_and_failed_reinstall_fence_execution"),
    ("installation authority crosses environments", "graphcoder_harbor.py",
     "if self._installed_environment is not environment:",
     "if self._installed_environment is None:",
     "test_harbor.HarborContracts.test_install_run_and_failed_reinstall_fence_execution"),
]


def main():
    source = Path(__file__).resolve().parent
    for label, filename, old, new, test in CONTROLS:
        with tempfile.TemporaryDirectory(prefix="predictor-negative-") as temporary:
            package = Path(temporary) / "scripts" / "benchmarks"
            package.mkdir(parents=True)
            for file in source.glob("*.py"):
                shutil.copyfile(file, package / file.name)
            target = package / filename
            text = target.read_text()
            if text.count(old) != 1:
                raise RuntimeError("control no longer targets one production boundary: " + label)
            target.write_text(text.replace(old, new))
            # A syntax/import failure is not a valid negative control. Compile
            # source in memory before running the actual named regression.
            compile(target.read_text(), str(target), "exec")
            result = subprocess.run([sys.executable, "-m", "unittest", "scripts.benchmarks." + test],
                cwd=temporary, env={**os.environ, "PYTHONPATH": temporary, "PYTHONDONTWRITEBYTECODE": "1"},
                capture_output=True, text=True)
            if result.returncode == 0 or "FAIL:" not in result.stderr:
                raise RuntimeError("negative control failed to reject: " + label + "\n" + result.stderr)
            print("negative control REJECTED:", label)


if __name__ == "__main__":
    main()
