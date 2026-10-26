#!/usr/bin/env python3
"""Print the selected workspace crates whose own sources name a Windows cfg.

A Linux check never compiles `cfg(windows)` code, so the lite gate cross-checks
exactly these crates for Windows. Reads `cargo metadata --no-deps` JSON on
stdin; arguments are the selected package names (none = every package).
Sources of nested crates (directories with their own Cargo.toml) belong to
those crates, not to the enclosing one.
"""
import json
import os
import re
import sys

CFG = re.compile(r'\bcfg(?:_attr)?!?\s*\(')
WINDOWS = re.compile(r'\bwindows\b|target_os\s*=\s*"windows"|target_family\s*=\s*"windows"')
SKIP = {'target', 'node_modules', '.git'}


def names_windows(text):
    """True when any cfg(...)/cfg_attr(...)/cfg!(...) predicate names Windows."""
    for match in CFG.finditer(text):
        depth, end = 1, match.end()
        while depth and end < len(text):
            depth += {'(': 1, ')': -1}.get(text[end], 0)
            end += 1
        if WINDOWS.search(text, match.end(), end):
            return True
    return False


def has_windows_code(root):
    for directory, subdirs, files in os.walk(root):
        subdirs[:] = [d for d in subdirs if d not in SKIP
                      and not os.path.exists(os.path.join(directory, d, 'Cargo.toml'))]
        for name in files:
            if not name.endswith('.rs'):
                continue
            try:
                with open(os.path.join(directory, name), encoding='utf-8', errors='replace') as source:
                    if names_windows(source.read()):
                        return True
            except OSError:
                continue
    return False


def windows_crates(metadata, selected):
    return [p['name'] for p in metadata['packages']
            if (not selected or p['name'] in selected)
            and has_windows_code(os.path.dirname(p['manifest_path']))]


if __name__ == '__main__':
    for name in windows_crates(json.load(sys.stdin), set(sys.argv[1:])):
        print(name)
