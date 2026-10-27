"""Locate immutable proof captures after their physical fixture relocation.

Captured receipts retain the logical paths recorded by their real producers.
Historical paths resolve to canonical immutable inputs. Receipts and their
hashes remain unchanged; there is no fallback to current production source.
"""
from pathlib import Path

CAPTURE_PREFIX = 'docs/proofs/evidence/'
FIXTURE_PREFIX = 'scripts/tests/fixtures/proof-policy/'
FS_SOURCE = 'rust/crates/filesystem/src/'
CAPTURE_ALIASES = {
    FIXTURE_PREFIX + 'fs-append-eof/342/' + FS_SOURCE + name:
    FIXTURE_PREFIX + 'fs-append-eof/341/' + FS_SOURCE + name
    for name in ('workspace/content.rs', 'tests/workspace/append_segments.rs', 'tests/workspace/mod.rs')
}
CAPTURE_ALIASES[FIXTURE_PREFIX + 'owner-updates-v1/fs339/' + FS_SOURCE + 'tests/workspace/mod.rs'] = (
    FIXTURE_PREFIX + 'fs-append-eof/341/' + FS_SOURCE + 'tests/workspace/mod.rs'
)
CAPTURE_ALIASES.update({
    FIXTURE_PREFIX + 'entrypoint-policy-20261026T030954Z/' + name + '.py':
    FIXTURE_PREFIX + 'entrypoint-policy-20261026T030635Z/' + name + '.py'
    for name in ('omit-candidate-multiset', 'borrow-dependency-proof', 'borrow-unrelated-invariant', 'omit-wildcard-discovery')
})


def repository_relative(relative):
    relative = FIXTURE_PREFIX + relative[len(CAPTURE_PREFIX):] if relative.startswith(CAPTURE_PREFIX) else relative
    return CAPTURE_ALIASES.get(relative, relative)


def repository_path(root, relative):
    root = Path(root).resolve()
    path = (root / repository_relative(relative)).resolve()
    if not path.is_relative_to(root):
        raise ValueError(f'path escapes repository: {relative}')
    return path
