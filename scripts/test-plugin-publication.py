#!/usr/bin/env python3
"""Regression tests for the acyclic CLI plugin's qualified-bytes publication."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parent.parent
PREPARE = ROOT / "scripts" / "prepare-plugin-publication.sh"
SOURCE_SHA = "1" * 40


def bash() -> str:
    return os.environ.get("BASH", "bash")


def run_function(call: str, cwd: Path = ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [bash(), "-c", f"source {shlex.quote(str(PREPARE))}; {call}"],
        cwd=cwd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )


def retained(directory: Path, name: str, contents: bytes, source_sha: str = SOURCE_SHA) -> None:
    directory.mkdir(parents=True)
    (directory / name).write_bytes(contents)
    digest = hashlib.sha256(contents).hexdigest()
    (directory / "SHA256SUMS").write_text(f"{digest}  {name}\n", encoding="utf-8")
    (directory / "SOURCE_COMMIT").write_text(source_sha + "\n", encoding="utf-8")


class PluginPublicationTests(unittest.TestCase):
    def test_scripts_parse(self) -> None:
        subprocess.run([bash(), "-n", str(PREPARE)], check=True)
        subprocess.run([bash(), "-n", str(ROOT / "plugin" / "scripts" / "retain-binary.sh")], check=True)

    def test_stage_tag_names_the_plugin_family_only(self) -> None:
        self.assertEqual(run_function("plugin_stage_version stage/npm/plugin/0.0.3").stdout.strip(), "0.0.3")
        self.assertEqual(
            run_function("plugin_stage_version stage/npm/plugin/1.0.0-rc.2").stdout.strip(),
            "1.0.0-rc.2",
        )
        for bad in ("stage/npm/fs/0.0.3", "plugin-v0.0.3", "stage/npm/plugin/0.0.3/extra", "stage/npm/plugin/v0.0.3"):
            self.assertNotEqual(run_function(f"plugin_stage_version {shlex.quote(bad)}").returncode, 0, bad)

    def test_every_target_maps_to_a_retaining_lane(self) -> None:
        targets = run_function("plugin_targets").stdout.split()
        self.assertEqual(targets, ["darwin-arm64", "darwin-x64", "linux-x64", "linux-arm64", "win32-x64"])
        expected = {
            "darwin-arm64": "plugin-macos",
            "darwin-x64": "plugin-macos",
            "linux-x64": "plugin-linux",
            "linux-arm64": "plugin-linux-arm64",
            "win32-x64": "plugin-windows",
        }
        for target, artifact in expected.items():
            self.assertEqual(run_function(f"plugin_artifact_for {target}").stdout.strip(), artifact)
        self.assertNotEqual(run_function("plugin_artifact_for freebsd-x64").returncode, 0)
        self.assertEqual(run_function("plugin_exe_suffix win32-x64").stdout.strip(), ".exe")
        self.assertEqual(run_function("plugin_exe_suffix linux-x64").stdout.strip(), "")

    def test_version_must_agree_across_crates_and_latest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for crate in ("acyclic", "acyclic-engine"):
                (root / "plugin" / "crates" / crate).mkdir(parents=True)
                (root / "plugin" / "crates" / crate / "Cargo.toml").write_text(
                    '[package]\nname = "x"\nversion = "0.0.3"\n', encoding="utf-8"
                )
            (root / "plugin" / "LATEST").write_text("0.0.3\n", encoding="utf-8")
            self.assertEqual(run_function(f"plugin_version_agrees 0.0.3 {shlex.quote(str(root))}").returncode, 0)
            self.assertNotEqual(run_function(f"plugin_version_agrees 0.0.4 {shlex.quote(str(root))}").returncode, 0)
            (root / "plugin" / "LATEST").write_text("0.0.2\n", encoding="utf-8")
            result = run_function(f"plugin_version_agrees 0.0.3 {shlex.quote(str(root))}")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("LATEST", result.stderr)

    def test_repository_versions_agree_with_latest(self) -> None:
        latest = (ROOT / "plugin" / "LATEST").read_text(encoding="utf-8").strip()
        self.assertEqual(run_function(f"plugin_version_agrees {shlex.quote(latest)} .").returncode, 0)

    def test_retained_binary_is_verified_exactly(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            good = Path(temporary) / "good"
            retained(good, "acyclic", b"binary")
            self.assertEqual(run_function(f"verify_retained_binary {shlex.quote(str(good))} {SOURCE_SHA} acyclic").returncode, 0)

            other_commit = run_function(f"verify_retained_binary {shlex.quote(str(good))} {'2' * 40} acyclic")
            self.assertNotEqual(other_commit.returncode, 0)
            self.assertIn("qualified for", other_commit.stderr)

            (good / "acyclic").write_bytes(b"tampered")
            tampered = run_function(f"verify_retained_binary {shlex.quote(str(good))} {SOURCE_SHA} acyclic")
            self.assertNotEqual(tampered.returncode, 0)

            wrong_name = Path(temporary) / "wrong-name"
            retained(wrong_name, "acyclic", b"binary")
            self.assertNotEqual(run_function(f"verify_retained_binary {shlex.quote(str(wrong_name))} {SOURCE_SHA} acyclic.exe").returncode, 0)

            incomplete = Path(temporary) / "incomplete"
            incomplete.mkdir()
            (incomplete / "acyclic").write_bytes(b"binary")
            self.assertNotEqual(run_function(f"verify_retained_binary {shlex.quote(str(incomplete))} {SOURCE_SHA} acyclic").returncode, 0)


if __name__ == "__main__":
    unittest.main()
