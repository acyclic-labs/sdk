"""Run real proof-entry checks and actual scanner/guard mutations, without Rust builds."""
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone

ROOT = Path('[private-metadata-removed]')
RECOVERY = 'python [private-path-removed]'
step = 'export_inputs'


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


try:
    step = 'preflight_staged_input_bytes'
    for name in ['scripts/check-proof-coverage.py', 'scripts/proof_inventory_scan.py', 'scripts/test-proof-coverage.py', 'scripts/check-wave1-inventory.py', 'docs/proofs/entrypoint-inventory.json', 'docs/proofs/wave1-inventory.json']:
        staged = subprocess.check_output(['git', 'show', ':' + name], cwd=ROOT)
        if staged != (ROOT / name).read_bytes():
            raise ValueError('working/index byte mismatch; normalize and stage exact input before qualification: ' + name)
    records = Path('[private-path-removed]') / ('entrypoint-policy-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    records.mkdir()
    shutil.copyfile(__file__, records / 'producer.py')
    selected = [path.relative_to(ROOT).as_posix() for path in (ROOT / 'docs/proofs').rglob('*') if path.is_file()]
    selected += ['scripts/check-proof-coverage.py', 'scripts/proof_inventory_scan.py', 'scripts/test-proof-coverage.py', 'scripts/check-wave1-inventory.py', 'scripts/qualify-wave1-coverage.py', 'rust/crates/stream/src/conformance/wave1/coverage.rs', 'rust/crates/stream/src/conformance/wave1.rs', 'rust/crates/stream/tests/wave1_coverage.rs', 'rust/crates/stream/examples/wave1-conformance.rs', 'rust/crates/native-runtime/src/held_sync.rs']
    source = {name: sha(ROOT / name) for name in sorted(set(selected))}
    (records / 'source.json').write_text(json.dumps(source, indent=2) + '\n')
    mutations = {
        'omit-candidate-multiset': ('scripts/check-proof-coverage.py', "require(Counter((row['kind'], row['line'], row['name']) for row in rows) == Counter(candidates(root, name)), 'public/unsafe/wildcard proof entry multiset differs: ' + name)", "require(True, 'candidate comparison omitted')", 3, ['omit', 'duplicate', 'retype']),
        'borrow-dependency-proof': ('scripts/check-proof-coverage.py', 'result[name] = dict(source={HST_POLICY_SOURCE: compiled[HST_POLICY_SOURCE]}, invariants={HST_POLICY_INVARIANT})', 'result[name] = dict(source=compiled, invariants={HST_POLICY_INVARIANT})', 1, ['test_dependency_input_cannot_borrow_reporting_proof']),
        'borrow-unrelated-invariant': ('scripts/check-proof-coverage.py', "require(set(row['invariants']) <= proofs[row['proof']]['invariants'], 'receipt does not prove the registered invariant: ' + name)", "require(True, 'invariant comparison omitted')", 1, ['test_unrelated_invariant_cannot_borrow_reporting_proof']),
        'omit-unsafe-discovery': ('scripts/proof_inventory_scan.py', "r'\\bunsafe\\s*(?:\\{|fn\\b|impl\\b|extern\\b|trait\\b)'", "r'(?!)'", 1, ['test_actual_captured_unsafe_sites_and_wildcard_are_discovered']),
        'omit-wildcard-discovery': ('scripts/check-proof-coverage.py', "if row['kind'] == 'catch_all_arm':", 'if False:  # omitted wildcard classification', 1, ['test_actual_captured_unsafe_sites_and_wildcard_are_discovered']),
    }
    rows = []
    with tempfile.TemporaryDirectory(prefix='verify-entrypoint-') as directory:
        export = Path(directory)
        for name in source:
            target = export / name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(ROOT / name, target)
        for label in ['positive', *mutations]:
            step = label
            # Reset actual production implementations before each independent control.
            for name in ('scripts/check-proof-coverage.py', 'scripts/proof_inventory_scan.py'):
                shutil.copyfile(ROOT / name, export / name)
            if label != 'positive':
                name, before, after, failures, witnesses = mutations[label]
                path = export / name
                original = path.read_bytes()
                if original.count(before.encode()) != 1:
                    raise ValueError('mutation must select exactly one production site')
                path.write_bytes(original.replace(before.encode(), after.encode()))
                (records / (label + '.py')).write_bytes(path.read_bytes())
                (records / (label + '.json')).write_text(json.dumps(dict(path=name, before=before, after=after, positive_sha256=source[name], mutant_sha256=sha(path)), indent=2) + '\n')
            started = time.monotonic()
            run = subprocess.run([sys.executable, 'scripts/test-proof-coverage.py'], cwd=export, capture_output=True, timeout=10)
            elapsed = time.monotonic() - started
            output = run.stdout + run.stderr
            text = output.decode('utf-8')
            log = records / (label + '.log')
            log.write_bytes(output)
            if elapsed >= 10 or 'Ran 13 tests' not in text:
                raise ValueError('unexpected selection or per-suite budget exceeded')
            if label == 'positive':
                if run.returncode or not text.rstrip().endswith('OK'):
                    raise ValueError('positive source failed named assertions')
            else:
                if run.returncode != 1 or f'FAILED (failures={failures})' not in text or 'ERROR:' in text or any(witness not in text for witness in witnesses):
                    raise ValueError('control failed outside the unchanged semantic assertions')
            rows.append(dict(label=label, selected=13, exit=run.returncode, seconds=elapsed, log_sha256=sha(log), implementations={name: sha(export / name) for name in ('scripts/check-proof-coverage.py', 'scripts/proof_inventory_scan.py')}))
    for name, digest in source.items():
        if sha(ROOT / name) != digest:
            raise ValueError('source changed during qualification: ' + name)
    receipt = dict(kind='proof_entry_policy_only_not_full_api_or_runtime_proof', producer_sha256=sha(records / 'producer.py'), source_manifest_sha256=sha(records / 'source.json'), tests_sha256=source['scripts/test-proof-coverage.py'], runs=rows)
    (records / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(dict(status='qualified_entry_policy_controls', receipt=str(records / 'receipt.json'), selected=13, mutations=len(mutations))))
except (OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as error:
    print(f'VERIFY_ENTRYPOINT_POLICY_FAILURE step={step} error={error}; recovery: {RECOVERY}', file=sys.stderr)
    sys.exit(1)
