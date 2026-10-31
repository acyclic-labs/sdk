"""Locate immutable proof captures after their physical fixture relocation.

Historical paths resolve to canonical captured inputs. Redacted metadata is
verified independently, and historical assertions read the original Git blob.
There is no fallback to current production source.
"""
from pathlib import Path
from functools import lru_cache
import json
import subprocess

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


@lru_cache(maxsize=1)
def _capture_index(content):
    return json.loads(content)


@lru_cache(maxsize=128)
def _original_blob(root, blob):
    # Git object identities are immutable. Distributed bytes are never cached.
    return subprocess.run(["git", "cat-file", "blob", blob],
                          cwd=root, capture_output=True)


def captured_bytes(path):
    """Validate distributed metadata, then read its original hash-bound Git blob.

    Historical assertions apply to the original capture, never to a rewritten
    execution. Distributed copies are checked independently before replay.
    Declared Git-only source inventories have no current replacement.
    """
    import hashlib

    root = Path(__file__).resolve().parents[1]
    path = Path(path).resolve()
    if not path.is_relative_to(root):
        return path.read_bytes()
    index = _capture_index((root / (FIXTURE_PREFIX + "redactions.json")).read_bytes())
    entry = index["files"].get(path.relative_to(root).as_posix())
    if entry is None:
        return path.read_bytes()
    if entry.get("distribution") != "git-only-source-inventory":
        content = path.read_bytes()
        if (hashlib.sha256(content).hexdigest() != entry["redacted_sha256"]
                or len(content) != entry["redacted_bytes"]):
            raise ValueError(f"STEP_FAILED capture-integrity: {path.name}; recovery: git restore -- {path.relative_to(root).as_posix()}")
    result = _original_blob(root, entry["original_blob"])
    if (result.returncode or hashlib.sha256(result.stdout).hexdigest() != entry["original_sha256"]
            or len(result.stdout) != entry["original_bytes"]):
        raise ValueError(f"STEP_FAILED original-capture: {path.name}; recovery: git fetch origin {index['source']}")
    return result.stdout
