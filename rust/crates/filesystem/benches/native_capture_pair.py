"""Prepare locally with --prepare COMMIT FILE; run with --baseline FILE --output DIR."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--prepare', nargs=2, metavar=('COMMIT', 'FILE'))
parser.add_argument('--baseline', type=Path)
parser.add_argument('--output', type=Path)
parser.add_argument('--windows-slot', action='store_true')
args = parser.parse_args()
if args.prepare:
    base, name = args.prepare
    head = subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip()
    names = subprocess.check_output(['git', 'diff', '--name-only', base, head], text=True).splitlines()
    sources, hashes = {}, {}
    for name in names:
        if not name.startswith('rust/crates/filesystem/src/'):
            continue
        old = subprocess.run(['git', 'show', base + ':' + name], capture_output=True)
        sources[name] = old.stdout.decode('utf-8') if old.returncode == 0 else None
        hashes[name] = hashlib.sha256(Path(name).read_bytes()).hexdigest()
    Path(args.prepare[1]).write_text(json.dumps(dict(base=base, candidate=head,
        sources=sources, candidate_hashes=hashes)), encoding='utf-8')
    sys.exit(0)
assert args.baseline and args.output
baseline = json.loads(args.baseline.read_text(encoding='utf-8'))
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
candidate = {p: Path(p).read_bytes() for p in baseline['sources']}
for name, contents in candidate.items():
    assert hashlib.sha256(contents).hexdigest() == baseline['candidate_hashes'][name], name
env = {k: v for k, v in os.environ.items()
       if k != 'RUSTFLAGS' and not k.startswith('ACYCLIC_BENCH')}
features = 'distributed,local,memory,native-watch,native-mount,model-checking,fuzzing'
bins = {}
build_seconds = {}
try:
    for stage in ['before', 'after']:
        for name, old in baseline['sources'].items():
            path = Path(name)
            contents = (old.encode('utf-8') if old is not None else None) if stage == 'before' else candidate[name]
            if contents is None:
                path.unlink(missing_ok=True)
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(contents)
        cmd = ['cargo', 'bench', '--locked', '-p', 'acyclic-fs', '--no-default-features',
               '--features', features, '--bench', 'native_capture', '--no-run', '--message-format=json']
        if args.windows_slot:
            cmd = ['powershell', '-NoProfile', '-File', 'Q:/lanes/lb.ps1'] + cmd
        started = time.monotonic()
        r = subprocess.run(cmd, env=env, capture_output=True, text=True, encoding='utf-8')
        build_seconds[stage] = time.monotonic() - started
        (out / (stage + '-build.stdout')).write_text(r.stdout, encoding='utf-8')
        (out / (stage + '-build.stderr')).write_text(r.stderr, encoding='utf-8')
        print('CAPTURE_PAIR_BUILD', stage, 'seconds=%.3f' % build_seconds[stage], flush=True)
        if r.returncode:
            raise RuntimeError(r.stderr[-6000:] + r.stdout[-6000:])
        for line in r.stdout.splitlines():
            if not line.startswith('{'):
                continue
            e = json.loads(line)
            if e.get('reason') == 'compiler-artifact' and e.get('executable') and e['target']['name'] == 'native_capture':
                dst = out / (stage + '-native_capture' + Path(e['executable']).suffix)
                shutil.copyfile(e['executable'], dst)
                dst.chmod(0o755)
                bins[stage] = dst
        assert stage in bins, stage
finally:
    for name, contents in candidate.items():
        Path(name).parent.mkdir(parents=True, exist_ok=True)
        Path(name).write_bytes(contents)

scale = {'ns': 1, 'µs': 1000, 'μs': 1000, 'ms': 1e6, 's': 1e9}
labels = ['128-empty', '128-80bytes', '1024-empty']
if sys.platform == 'win32':
    labels += ['sparse-65536', 'sparse-262144']
def timings(stdout):
    values = []
    for line in stdout.splitlines():
        matches = [re.search(r'([\d.,]+)\s*(ns|µs|μs|ms|s)\s*$', c) for c in line.split('│')]
        times = [float(m[1].replace(',', '')) * scale[m[2]] for m in matches if m]
        if len(times) == 4:
            values.append(times[2])
    assert len(values) == len(labels), stdout
    return values

rows = []
for trace in [False, True]:
    pairs = []
    for repeat in range(7):
        pair = {}
        for stage in (['before', 'after'] if repeat % 2 == 0 else ['after', 'before']):
            run_env = dict(env)
            if trace:
                run_env['ACYCLIC_BENCH_TRACE'] = '1'
            cmd = [str(bins[stage]), 'rescan', '--bench', '--threads', '1', '--timer', 'os',
                   '--sample-count', '31', '--sample-size', '1', '--max-time', '0.1', '--color', 'never']
            r = subprocess.run(cmd, env=run_env, capture_output=True, text=True, encoding='utf-8')
            prefix = f'{trace}-{repeat}-{stage}'
            (out / (prefix + '.stdout')).write_text(r.stdout, encoding='utf-8')
            (out / (prefix + '.stderr')).write_text(r.stderr, encoding='utf-8')
            if r.returncode:
                raise RuntimeError(r.stdout[-6000:] + r.stderr[-6000:])
            pair[stage] = timings(r.stdout)
        pairs.append(pair)
    for i, label in enumerate(labels):
        changes = [100 * (1 - p['after'][i] / p['before'][i]) for p in pairs]
        row = dict(platform=sys.platform, fixture=label, trace=trace,
                   before_ns=statistics.median(p['before'][i] for p in pairs),
                   after_ns=statistics.median(p['after'][i] for p in pairs),
                   faster_percent=statistics.median(changes), range=[min(changes), max(changes)])
        rows.append(row)
        print('CAPTURE_PAIR', json.dumps(row), flush=True)
    (out / ('pairs-trace-' + str(trace) + '.json')).write_text(json.dumps(pairs), encoding='utf-8')
(out / 'summary.json').write_text(json.dumps(dict(base=baseline['base'], candidate=baseline['candidate'],
    build_seconds=build_seconds, rows=rows), indent=2), encoding='utf-8')
