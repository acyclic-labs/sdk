#!/usr/bin/env python3
"""Exercise gate derivation against a renamed transitive crate and target dependencies."""
import importlib.util
import pathlib
import subprocess
import tempfile
from unittest import mock
import unittest

spec = importlib.util.spec_from_file_location("gate", pathlib.Path(__file__).with_name("gate-packages.py"))
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class GatePackages(unittest.TestCase):
    def assert_packages(self, root, members, expected):
        metadata = {"workspace_members": members,
                    "packages": [{"id": name, "name": name} for name in members]}
        original = subprocess.check_output

        def output(command, **kwargs):
            if command[0] == "cargo":
                self.assertEqual(command, ["cargo", "metadata", "--no-deps", "--format-version", "1", "--offline"])
                self.assertEqual(kwargs["cwd"], root.resolve())
                return gate.json.dumps(metadata).encode()
            return original(command, **kwargs)

        with mock.patch.object(gate.subprocess, "check_output", side_effect=output):
            self.assertEqual(gate.packages(root), expected)

    def test_excluded_path_dependency_is_not_a_test_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = pathlib.Path(directory)
            manifests = {
                "Cargo.toml": '[workspace]\nmembers=["harness", "helper"]\nexclude=["vendor/client"]\nresolver="2"\n',
                "harness/Cargo.toml": '[package]\nname="acyclic-harness-tui"\nversion="0.1.0"\n[dependencies]\nclient={package="acyclic-tonic-web-wasm-client",path="../vendor/client"}\n',
                "helper/Cargo.toml": '[package]\nname="helper"\nversion="0.1.0"\n',
                "vendor/client/Cargo.toml": '[package]\nname="acyclic-tonic-web-wasm-client"\nversion="0.1.0"\n[dev-dependencies]\nhelper={path="../../helper"}\n',
            }
            for path, content in manifests.items():
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
                if "[package]" in content:
                    (target.parent / "src").mkdir()
                    (target.parent / "src/lib.rs").write_text("")
            subprocess.run(["git", "init", "-q", str(root)], check=True)
            subprocess.run(["git", "-C", str(root), "add", "."], check=True)
            self.assertEqual(gate.packages(root), ["acyclic-harness-tui", "helper"])

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
            members = ["acyclic-harness-tui", "helper", "platform", "renamed-renderer", "unrelated", "acyclic-harness-fuzz"]
            self.assert_packages(root, members, ["acyclic-harness-tui", "helper", "platform", "renamed-renderer"])


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
            self.assert_packages(root, ["graphcoder", "helper"], ["graphcoder", "helper"])

if __name__ == "__main__":
    unittest.main()
