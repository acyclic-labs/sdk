"""Exercise target discovery and feature closure without invoking a compiler."""
import importlib.util
from pathlib import Path
import tempfile
import sys
import unittest

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location(
    "test_features", Path(__file__).with_name("check-test-features.py"))
policy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(policy)


class FeatureHygiene(unittest.TestCase):
    def check(self, manifest, source='#![cfg(feature = "local")]\n'):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "tests").mkdir()
            (root / "tests/case.rs").write_text(source, encoding="utf-8")
            path = root / "Cargo.toml"
            path.write_text('[package]\nname = "example"\nversion = "0.0.0"\n'
                            + manifest, encoding="utf-8")
            return policy.violations(path)

    def test_implicit_and_explicit_missing_requirements(self):
        for manifest in ["", '[[test]]\nname = "case"\n']:
            self.assertEqual(len(self.check(manifest)), 1)

    def test_transitive_feature_requirement(self):
        self.assertEqual(self.check('''
[[test]]
name = "case"
required-features = ["backend"]
[features]
backend = ["local"]
local = []
'''), [])

    def test_modules_are_not_cargo_targets(self):
        self.assertEqual(self.check("autotests = false\n"), [])

    def test_mixed_platform_and_feature_gate(self):
        self.assertEqual(len(self.check("", '''
#![cfg(all(
    not(target_arch = "wasm32"), feature = "local"
))]
''')), 1)
        self.assertEqual(self.check("", '#![cfg(target_os = "linux")]\n'), [])

    def test_models_require_opt_in(self):
        self.assertEqual(len(self.check("", '#![cfg(loom)]\n')), 1)
        self.assertEqual(self.check('''
[[test]]
name = "case"
required-features = ["model-checking"]
''', '#![cfg(loom)]\n'), [])


if __name__ == "__main__":
    unittest.main()
