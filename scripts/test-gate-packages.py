#!/usr/bin/env python3
"""Exercise gate derivation against a renamed transitive crate and target dependencies."""
import importlib.util
import pathlib
import subprocess
import tempfile
import unittest

spec = importlib.util.spec_from_file_location("gate", pathlib.Path(__file__).with_name("gate-packages.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class GatePackages(unittest.TestCase):
    def test_local_dependency_closure(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifests = {
                "Cargo.toml": '[workspace]\n[workspace.dependencies]\nrenamed = { package="renamed-renderer", path="renderer" }\n',
                "harness/Cargo.toml": '[package]\nname="acyclic-harness-tui"\n[dependencies]\nrenamed.workspace=true\n[target.\'cfg(windows)\'.dev-dependencies]\nplatform={path="../platform"}\n',
                "renderer/Cargo.toml": '[package]\nname="renamed-renderer"\n[build-dependencies]\nhelper={path="../helper", optional=true}\n',
                "helper/Cargo.toml": '[package]\nname="helper"\n[dependencies]\ncycle={path="../renderer"}\n',
                "platform/Cargo.toml": '[package]\nname="platform"\n',
                "unrelated/Cargo.toml": '[package]\nname="unrelated"\n',
                "fuzz/Cargo.toml": '[package]\nname="acyclic-harness-fuzz"\n',
            }
            for path, content in manifests.items():
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            self.assertEqual(gate.packages(root), ["acyclic-harness-tui", "helper", "platform", "renamed-renderer"])


    def test_package_is_workspace_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            (root / "Cargo.toml").write_text(
                '[package]\nname="graphcoder"\n[workspace]\n'
                '[workspace.dependencies]\nrenamed={path="helper"}\n'
                '[dependencies]\nrenamed.workspace=true\n'
            )
            (root / "helper").mkdir()
            (root / "helper/Cargo.toml").write_text('[package]\nname="helper"\n')
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            self.assertEqual(gate.packages(root), ["graphcoder", "helper"])

if __name__ == "__main__":
    unittest.main()
