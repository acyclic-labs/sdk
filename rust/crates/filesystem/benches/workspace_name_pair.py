"""Run from the repository root via bs.ps1 with a frozen baseline manifest."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import statistics
import subprocess
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline', required=True, type=pathlib.Path)
parser.add_argument('--test-binary', type=pathlib.Path)
parser.add_argument('--reuse-release-dir', type=pathlib.Path)
parser.add_argument('--output', type=pathlib.Path, default=pathlib.Path('/work/target/workspace-name-pair'))
args = parser.parse_args()
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
env = os.environ.copy()
for key in ['RUSTFLAGS', 'ACYCLIC_BENCH_TRACE', 'ACYCLIC_BENCH_WORK',
            'ACYCLIC_BENCH_MARKERS', 'ACYCLIC_BENCH_LEGACY_NAME']:
    env.pop(key, None)
features = 'distributed,local,memory,native-watch,native-mount,model-checking,fuzzing'
base = ['--locked', '-p', 'acyclic-fs', '--no-default-features', '--features', features]
baseline = json.loads(args.baseline.read_text())
expected = ['93db8b63d0803d0973ea235c7b0ea11899cb76c3c9551ff341feaeae7643ce2c',
            '08010d21d654c7034d706e50345449f90c206cdbe93759987e8bbfd5f734c416']
assert [hashlib.sha256(s.encode()).hexdigest() for s in baseline.values()] == expected
candidate = {p: pathlib.Path(p).read_bytes() for p in baseline}

def run(label, command, environment=env, timeout=None, permit_failure=False):
    start = time.monotonic()
    p = subprocess.run(list(map(str, command)), env=environment, capture_output=True,
                       text=True, timeout=timeout)
    (out / (label + '.stdout')).write_text(p.stdout)
    (out / (label + '.stderr')).write_text(p.stderr)
    print('RUN', label, p.returncode, round(time.monotonic() - start, 3), flush=True)
    if p.returncode and not permit_failure:
        raise RuntimeError(f'{label}: {p.returncode}\n{p.stdout[-5000:]}\n{p.stderr[-5000:]}')
    return p

def build(label, command):
    p = run(label, command + ['--message-format=json'])
    binaries = {}
    for line in p.stdout.splitlines():
        e = json.loads(line)
        if e.get('reason') == 'compiler-artifact' and e.get('executable'):
            binary = out / (label + '-' + e['target']['name'])
            shutil.copyfile(e['executable'], binary)
            binary.chmod(0o755)
            binaries[e['target']['name']] = binary
    return binaries

# Independently initialized parsers compare full canonical bytes, exact errors,
# every observed operation/failure prefix, all counters and six heap statistics.
test = args.test_binary or build('ordinary', ['cargo', 'test'] + base + ['--lib', '--no-run'])['acyclic_fs']
run('differential-and-physical-controls', [test, 'workspace::identity::name_tests::'], timeout=9)
if args.reuse_release_dir:
    bins = {stage: {n: args.reuse_release_dir / (stage + '-' + n)
                   for n in ['filesystem', 'filesystem_alloc']} for stage in ['before', 'after']}
else:
    bins = {}
    try:
        for stage in ['before', 'after']:
            for p in baseline:
                pathlib.Path(p).write_bytes(baseline[p].encode() if stage == 'before' else candidate[p])
            bins[stage] = build(stage, ['cargo', 'bench'] + base + [
                '--bench', 'filesystem', '--bench', 'filesystem_alloc', '--no-run'])
    finally:
        for p, contents in candidate.items():
            pathlib.Path(p).write_bytes(contents)

cores = {}
for cpu in sorted(os.sched_getaffinity(0)):
    topology = pathlib.Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
    cores.setdefault(((topology / 'physical_package_id').read_text(),
                      (topology / 'core_id').read_text()), cpu)
cpus = sorted(cores.values())[-4:]
scale = {'ns': 1, 'µs': 1000, 'μs': 1000, 'ms': 1e6, 's': 1e9}
def timings(stdout, count):
    values = []
    for line in stdout.splitlines():
        matches = [re.search(r'([\d.,]+)\s*(ns|µs|μs|ms|s)\s*$', cell) for cell in line.split('│')]
        times = [float(m[1].replace(',', '')) * scale[m[2]] for m in matches if m]
        if len(times) == 4:
            values.append(times[2])
    assert len(values) == count, stdout
    return values

summary = []
for trace in [False, True]:
    timed = env.copy()
    if trace:
        timed['ACYCLIC_BENCH_TRACE'] = '1'
    for threads, kind, pattern, count, samples, size in [
        (1, 'solo', r'workspace_names::workspace_name::', 12, 128, 8),
        (4, 'solo', r'workspace_names::workspace_name::', 12, 128, 8),
        (1, 'shared64', r'workspace_names::workspace_name_shared::', 6, 32, 1),
    ]:
        pairs = []
        for repeat in range(7):
            pair = {}
            for stage in (['before', 'after'] if repeat % 2 == 0 else ['after', 'before']):
                e = timed.copy()
                if stage == 'before':
                    e['ACYCLIC_BENCH_LEGACY_NAME'] = '1'
                command = ['taskset', '-c', ','.join(map(str, cpus[:threads] if kind == 'solo' else cpus)),
                           bins[stage]['filesystem'], pattern, '--bench', '--threads', threads,
                           '--timer', 'os', '--sample-count', samples, '--sample-size', size,
                           '--max-time', '0.1', '--color', 'never']
                p = run(f'{stage}-{kind}-t{threads}-trace{int(trace)}-r{repeat}', command, e, timeout=9)
                pair[stage] = timings(p.stdout, count)
            pairs.append(pair)
        for index in range(count):
            changes = [100 * (1 - p['after'][index] / p['before'][index]) for p in pairs]
            row = dict(trace=trace, threads=threads, kind=kind, case=index,
                       before_ns=statistics.median(p['before'][index] for p in pairs),
                       after_ns=statistics.median(p['after'][index] for p in pairs),
                       paired_percent=statistics.median(changes),
                       range_percent=[min(changes), max(changes)])
            summary.append(row)
            print(json.dumps(row), flush=True)
    for stage in ['before', 'after']:
        e = timed.copy()
        e['ACYCLIC_BENCH_WORK'] = '1'
        if stage == 'before':
            e['ACYCLIC_BENCH_LEGACY_NAME'] = '1'
        params = [r'workspace_names::workspace_name::', '--bench', '--threads', '1',
                  '--sample-count', '1', '--sample-size', '1', '--color', 'never']
        run(f'{stage}-physical-trace{int(trace)}', [bins[stage]['filesystem_alloc']] + params, e, timeout=9)
        e['ACYCLIC_BENCH_MARKERS'] = '1'
        run(f'{stage}-syscalls-trace{int(trace)}', [
            'strace', '-qq', '-f', '-ttt', '-T', '-s', '1024', '-o',
            out / f'{stage}-trace{int(trace)}.strace', 'taskset', '-c', ','.join(map(str, cpus)),
            bins[stage]['filesystem']] + params, e, timeout=9)
        run(f'{stage}-shared-syscalls-trace{int(trace)}', [
            'strace', '-qq', '-f', '-ttt', '-T', '-s', '1024', '-o',
            out / f'{stage}-shared-trace{int(trace)}.strace', 'taskset', '-c', ','.join(map(str, cpus)),
            bins[stage]['filesystem'], r'workspace_names::workspace_name_shared::',
            '--bench', '--threads', '1', '--sample-count', '1', '--sample-size', '1', '--color', 'never'],
            e, timeout=9)
        run(f'{stage}-rss-trace{int(trace)}', [
            '/usr/bin/time', '-v', '-o', out / f'{stage}-trace{int(trace)}.time',
            bins[stage]['filesystem']] + params, e, timeout=9)
(out / 'summary.json').write_text(json.dumps(summary, indent=2))
print('Independent differential/physical assertions and seven paired production-path measurements passed.')
