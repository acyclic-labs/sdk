"""Admit real Streams output as partial observations, never full qualification."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path('[private-metadata-removed]')
OWNER = Path('[checkout-path-removed]')
ORIGIN = Path('[private-path-removed]')
DEST = ROOT / 'docs/proofs/evidence/streams-checkpoint-v3'
RECOVERY = 'python [private-path-removed]'
step = 'resolve_real_git_identities'

def git(*args):
    return subprocess.check_output(['git', '-C', str(OWNER), *args])

try:
    ids = {}
    for name, revision in [('native_snapshot', '3c6e213240'), ('native_signed', '391d21d2f2'),
                           ('typescript_snapshot', 'e8b7315427'), ('current_signed', '3a348ebf16')]:
        ids[name] = dict(commit=git('rev-parse', revision + '^{commit}').decode().strip(),
                         tree=git('rev-parse', revision + '^{tree}').decode().strip())
    if ids['native_snapshot']['tree'] != ids['native_signed']['tree'] or ids['typescript_snapshot']['tree'] != ids['current_signed']['tree']:
        raise ValueError('reported snapshot/signed tree equivalence is false')
    DEST.mkdir(parents=True, exist_ok=True)
    captures = []

    def capture(relative, data, origin):
        path = DEST / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)
        captures.append(dict(repo_path=path.relative_to(ROOT).as_posix(), sha256=hashlib.sha256(data).hexdigest(), bytes=len(data), origin=origin))

    capture('identities.json', (json.dumps(ids, indent=2) + '\n').encode(), 'real git rev-parse commit/tree in ' + str(OWNER))
    for name in ('native_signed', 'current_signed'):
        capture(name + '-signature.txt', git('log', '-1', '--format=%H %G? %GS', ids[name]['commit']), 'real git log signature status')
    step = 'capture_real_source_and_logs'
    for name in ('owner-qualified-20261026T0245Z.log', 'owner-typescript-20261026T0258Z.log',
                 'owner-final-surface-20261026T0256Z.log', 'owner-native-surface-20261026T0256Z.log',
                 'owner-static-surface-20261026T0258Z.log', 'adoption.md'):
        capture('logs/' + name, (ORIGIN / name).read_bytes(), str(ORIGIN / name))
    for name in ('rust/crates/stream/tests/wave1_loom.rs', 'rust/crates/stream/src/memory/recovery_model.rs',
                 'rust/crates/stream/src/memory/commit.rs', 'rust/crates/stream/src/memory/provider.rs',
                 'rust/crates/stream/src/query_sync.rs', 'rust/crates/stream/src/query_sync/locks.rs',
                 'rust/crates/stream/src/conformance.rs'):
        capture('source/' + name, git('show', ids['native_signed']['commit'] + ':' + name), 'git show ' + ids['native_signed']['commit'] + ':' + name)
    capture('producer.py', Path(__file__).read_bytes(), str(Path(__file__)))
    step = 'record_partial_scope'
    receipt = dict(schema_version=1, kind='streams_checkpoint_partial_observation', captures=captures,
                   identities=ids, qualified_modes=[], current_head_qualified=False,
                   exact_compiled_artifacts_verified=False, positive_proofs=0,
                   native=dict(library_tests=381, library_seconds=8.98, wave1_tests=10, wave1_seconds=0.04,
                               recovery_tests=1, recovery_seconds=0.65, loom_tests=8, loom_seconds=0.01),
                   typescript=dict(tests=105, files=14, seconds=6.37, runtime='Bun; not Node', generated_wasm_hash=None),
                   controls=dict(root_only='production_fault', exclusions='production_fault', page_bytes='production_fault',
                                 independent_reads='model_postprocessing'),
                   provenance_gaps=['compiled input manifests', 'executed binary hashes', 'generated WASM artifact identity', 'full current-head native qualification'],
                   remaining=['shared HST histories and actual hosted endpoint', 'browser and staged native addon', 'persistent closure/GC/restore and lifetime attempt fencing', 'production negative for independent page reads', 'all cross-mode obligations'])
    (DEST / 'receipt.json').write_bytes((json.dumps(receipt, indent=2) + '\n').encode())
    print(json.dumps(dict(status='partial_source_log_join_only', captures=len(captures), identities=ids, positive_proofs=0)))
except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
    print(f'VERIFY_STREAM_CHECKPOINT_FAILURE step={step} error={error}; recovery: {RECOVERY}', file=sys.stderr)
    sys.exit(1)
