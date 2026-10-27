#!/usr/bin/env python3
"""Print the crates whose diff against BASE touches feature gating.

`cargo check --all-features` never compiles the narrower selections that real
consumers build, so a stray `#[cfg(feature = ...)]` (or a `[features]` edit)
can break them unseen. The lite gate re-checks exactly these crates under
their declared consumer feature sets. A crate is printed when the diff adds or
removes a line naming `feature` inside a cfg predicate in one of its Rust
sources, or changes its manifest's `[features]` table.

usage: gate-feature-crates.py BASE [HEAD]   (run at the repository root)
       gate-feature-crates.py --sets CRATES_FILE < cargo-metadata.json
"""
import json
import os
import re
import subprocess
import sys
import tomllib

# rustfmt splits long predicates, so a `feature = "..."` line alone counts.
CFG_FEATURE = re.compile(r'\bcfg(?:_attr)?!?\s*\(.*\bfeature\b|\bfeature\s*=\s*"')


def git(*args):
    return subprocess.run(['git', *args], check=True, capture_output=True).stdout.decode('utf-8', 'replace')


def owner(path, cache={}):
    """Package name of the nearest existing Cargo.toml; None for a workspace root."""
    directory = os.path.dirname(path)
    while directory not in cache:
        if os.path.isfile(os.path.join(directory or '.', 'Cargo.toml')):
            with open(os.path.join(directory or '.', 'Cargo.toml'), 'rb') as source:
                cache[directory] = tomllib.load(source).get('package', {}).get('name')
        elif not directory:
            cache[directory] = None
        else:
            directory = os.path.dirname(directory)
    return cache[directory]


def features(rev, path):
    try:
        return tomllib.loads(git('show', f'{rev}:{path}')).get('features')
    except (subprocess.CalledProcessError, tomllib.TOMLDecodeError):
        return None


def touched(base, head):
    """Map each changed .rs path to its added/removed lines, in one diff."""
    lines, path = {}, None
    for line in git('diff', '--no-renames', '-U0', base, head, '--', '*.rs').splitlines():
        # A deletion's `+++ /dev/null` keeps the `--- a/` path: removed cfg
        # lines still change the crate's feature gating.
        if line.startswith(('--- a/', '+++ b/')):
            path = line[6:]
        elif line.startswith(('--- ', '+++ ')):
            continue
        elif path and line[:1] in '+-':
            lines.setdefault(path, []).append(line[1:])
    return lines


def feature_crates(base, head='HEAD'):
    paths = {path for path, lines in touched(base, head).items()
             if any(CFG_FEATURE.search(line) for line in lines)}
    manifests = git('diff', '--no-renames', '--name-only', '-z', base, head, '--', 'Cargo.toml', '*/Cargo.toml')
    paths |= {path for path in manifests.split('\0') if path and features(base, path) != features(head, path)}
    return sorted({name for path in paths if (name := owner(path))})


def consumer_sets(metadata, crates):
    """(crate, "f1,f2") per declared consumer set; crates declaring none are skipped."""
    for package in sorted(metadata['packages'], key=lambda p: p['name']):
        if package['name'] in crates:
            sets = ((package.get('metadata') or {}).get('acyclic') or {}).get('consumer-features', [])
            if not sets:
                print(f'GATE_LITE_FEATURES_UNDECLARED crate={package["name"]}', file=sys.stderr)
            for features in sets:
                yield package['name'], ','.join(features)


if __name__ == '__main__':
    if sys.argv[1:2] == ['--sets'] and len(sys.argv) == 3:
        with open(sys.argv[2], encoding='utf-8') as names:
            crates = set(names.read().split())
        for name, features in consumer_sets(json.load(sys.stdin), crates):
            print(f'{name}\t{features}')
    elif len(sys.argv) in (2, 3):
        print('\n'.join(feature_crates(*sys.argv[1:])))
    else:
        sys.exit(__doc__.strip().splitlines()[-1])
