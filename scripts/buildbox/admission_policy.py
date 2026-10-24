"""Shared admission policy for direct and generated bb2 commands."""
import hashlib
import json
import os
from pathlib import Path
import shutil

QUERIES = frozenset({'metadata', 'tree', 'locate-project', 'pkgid', 'verify-project', 'read-manifest'})


def cargo_query(argv):
    # Recognize Cargo's own subcommands only. Unknown options, wrappers and
    # aliases cannot turn a compile into an unaccounted query.
    if not argv or Path(argv[0]).name != 'cargo': return False
    rest = argv[1:]
    if rest and rest[0].startswith('+'): rest = rest[1:]
    while rest and rest[0] in ('--locked', '--offline', '--frozen', '--verbose', '-v', '--quiet', '-q'):
        rest = rest[1:]
    return bool(rest and rest[0] in QUERIES)


def canonical_argv(argv, env):
    # Generated wrappers are dispatched with their already-armed metadata.
    # No shell parsing or tenant-specific bypass is maintained here.
    if env.get('BB_ADMIT_WRAPPED_COMMAND') == '1':
        meta = env.get('BUILD_SLOT_RECEIPT_META')
        if meta:
            data = json.loads(Path(meta).read_text())
            command = data.get('argv')
            if isinstance(command, list) and all(isinstance(x, str) for x in command): return command
    return argv


def resolve_wrapper(env, compiler=False):
    wrapper = env.get('RUSTC_WRAPPER', '/home/var/bin/sccache-rustc' if compiler else '')
    if not wrapper or env.get('BB_CACHE_DISABLED') == '1': return None
    executable = shutil.which(wrapper, path=env.get('PATH'))
    if not executable: raise RuntimeError('RUSTC_WRAPPER_MISSING admission-INFRA: ' + wrapper)
    executable = str(Path(executable).resolve(strict=True))
    data = Path(executable).read_bytes()
    if data.startswith(b'#!'):
        interpreter = data.splitlines()[0][2:].strip().split()[0].decode()
        if not os.path.isabs(interpreter) or not os.access(interpreter, os.X_OK):
            raise RuntimeError('RUSTC_WRAPPER_INTERPRETER_MISSING admission-INFRA: ' + interpreter)
    env['RUSTC_WRAPPER'] = executable
    return {'path': executable, 'sha256': hashlib.sha256(data).hexdigest()}
