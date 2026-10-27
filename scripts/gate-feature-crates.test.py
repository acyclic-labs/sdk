import importlib.util
import os
import subprocess
import tempfile
import unittest

here = os.path.dirname(os.path.abspath(__file__))


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, os.path.join(here, file))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


features = load('gate_feature_crates', 'gate-feature-crates.py')

# The #1602 break: the module carried an extra stacked cfg its re-export lacked.
BROKEN = '''#[cfg(all(
    feature = "filesystem-local",
    feature = "tui",
    not(target_arch = "wasm32")
))]
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
mod local;
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
pub use local::LocalThing;
'''
FIXED = BROKEN.replace('#[cfg(all(\n    feature = "filesystem-local",\n    feature = "tui",\n    not(target_arch = "wasm32")\n))]\n', '')


class FeatureCrates(unittest.TestCase):
    def git(self, *args):
        subprocess.run(['git', '-c', 'user.name=t', '-c', 'user.email=t@t', '-c', 'commit.gpgsign=false', *args],
                       cwd=self.root, check=True, capture_output=True)

    def write(self, path, text):
        os.makedirs(os.path.join(self.root, os.path.dirname(path)), exist_ok=True)
        with open(os.path.join(self.root, path), 'w', newline='\n') as out:
            out.write(text)

    def test_selects_crates_touching_feature_gating(self):
        with tempfile.TemporaryDirectory() as self.root:
            self.git('init', '-q')
            self.write('Cargo.toml', '[workspace]\nmembers = ["a", "b", "c"]\n')
            for name in 'abc':
                self.write(f'{name}/Cargo.toml', f'[package]\nname = "{name}"\n[features]\nx = []\n')
                self.write(f'{name}/src/lib.rs', BROKEN)
            self.git('add', '-A')
            self.git('commit', '-qm', 'base')
            self.write('a/src/lib.rs', FIXED)  # removed split cfg lines
            self.write('b/src/lib.rs', BROKEN + 'fn unrelated() {}\n')  # no cfg touched
            self.write('c/Cargo.toml', '[package]\nname = "c"\n[features]\nx = ["y"]\ny = []\n')
            self.git('commit', '-qam', 'head')
            previous = os.getcwd()
            os.chdir(self.root)
            try:
                self.assertEqual(features.feature_crates('HEAD~1'), ['a', 'c'])
            finally:
                os.chdir(previous)

    def test_consumer_sets_come_from_metadata(self):
        metadata = {'packages': [
            {'name': 'a', 'metadata': {'acyclic': {'consumer-features': [['f', 'g'], []]}}},
            {'name': 'b', 'metadata': None},
            {'name': 'c', 'metadata': {'acyclic': {'consumer-features': [['h']]}}},
        ]}
        self.assertEqual(list(features.consumer_sets(metadata, {'a', 'b'})), [('a', 'f,g'), ('a', '')])


if __name__ == '__main__':
    unittest.main()
