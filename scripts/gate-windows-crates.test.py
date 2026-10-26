import importlib.util
import json
import os
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    'gate_windows_crates', os.path.join(os.path.dirname(__file__), 'gate-windows-crates.py'))
crates = importlib.util.module_from_spec(spec)
spec.loader.exec_module(crates)


class WindowsCrates(unittest.TestCase):
    def workspace(self, sources):
        root = tempfile.mkdtemp()
        self.addCleanup(__import__('shutil').rmtree, root)
        packages = []
        for name, files in sources.items():
            for path, text in files.items():
                full = os.path.join(root, name, path)
                os.makedirs(os.path.dirname(full), exist_ok=True)
                with open(full, 'w') as f:
                    f.write(text)
            packages.append({'name': name, 'manifest_path': os.path.join(root, name, 'Cargo.toml')})
            open(packages[-1]['manifest_path'], 'w').close()
        return {'packages': packages}

    def test_selects_only_crates_with_windows_cfg(self):
        meta = self.workspace({
            'plain': {'src/lib.rs': '#[cfg(unix)] fn a() {}\n#[cfg(target_os = "linux")] fn b() {}'},
            'win': {'src/sys.rs': '#[cfg(windows)]\nmod imp;'},
            'os': {'src/lib.rs': '#[cfg(all(feature = "x", target_os = "windows"))] fn c() {}'},
            'attr': {'tests/t.rs': '#[cfg_attr(not(windows), ignore)] fn d() {}'},
            'family': {'src/lib.rs': '#[cfg(any(target_family = "windows"))] fn e() {}'},
        })
        self.assertEqual(crates.windows_crates(meta, set()), ['win', 'os', 'attr', 'family'])
        self.assertEqual(crates.windows_crates(meta, {'plain', 'win'}), ['win'])

    def test_nested_crate_sources_are_not_the_parents(self):
        meta = self.workspace({'outer': {'src/lib.rs': 'fn a() {}',
                                         'inner/Cargo.toml': '',
                                         'inner/src/lib.rs': '#[cfg(windows)] fn b() {}'}})
        self.assertEqual(crates.windows_crates(meta, set()), [])

    def test_identifiers_containing_windows_do_not_count(self):
        meta = self.workspace({'x': {'src/lib.rs': '#[cfg(feature = "windows_ntfs")] fn a() {}'}})
        self.assertEqual(crates.windows_crates(meta, set()), [])


if __name__ == '__main__':
    unittest.main()
