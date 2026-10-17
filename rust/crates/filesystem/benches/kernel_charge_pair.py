"""Run from the repository root via bs.ps1; see --help for retained binaries."""
import argparse
import hashlib
import json
import os
import pathlib
import re
import shutil
import statistics
import subprocess

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--baseline-accounting', required=True, type=pathlib.Path)
parser.add_argument('--reuse-release-dir', type=pathlib.Path)
parser.add_argument('--test-binary', type=pathlib.Path)
parser.add_argument('--output', type=pathlib.Path, default=pathlib.Path('/work/target/kernel-charge-pair'))
args = parser.parse_args()
out = args.output.resolve()
out.mkdir(parents=True, exist_ok=True)
env = os.environ.copy()
for key in ['RUSTFLAGS', 'ACYCLIC_BENCH_TRACE', 'ACYCLIC_BENCH_WORK', 'ACYCLIC_BENCH_MARKERS']:
    env.pop(key, None)
base = ['--locked', '-p', 'acyclic-fs', '--no-default-features', '--features',
        'distributed,local,memory,native-watch,native-mount,model-checking,fuzzing']
baseline = args.baseline_accounting.read_bytes()
assert hashlib.sha256(baseline).hexdigest() == '26a0ee9ca3eb5de152f17db162e2b731a868c506ea22e120981a4abac44f274e', 'expected frozen main 96790de61 accounting.rs'


def run(label, command, environment=env):
    result = subprocess.run(list(map(str, command)), env=environment, capture_output=True, text=True)
    (out / (label + '.stdout')).write_text(result.stdout)
    (out / (label + '.stderr')).write_text(result.stderr)
    if result.returncode:
        raise RuntimeError(f'{label}: {result.returncode}\n{result.stdout[-6000:]}\n{result.stderr[-6000:]}')
    return result


def build(label, command):
    result = run(label, command + ['--message-format=json'])
    bins = {}
    for line in result.stdout.splitlines():
        event = json.loads(line)
        if event.get('reason') == 'compiler-artifact' and event.get('executable'):
            binary = out / (label + '-' + event['target']['name'])
            shutil.copyfile(event['executable'], binary)
            binary.chmod(0o755)
            bins[event['target']['name']] = binary
    return bins


# This runs the separately initialized old-path/candidate differential property:
# every result/error and the entire receipt/live-storage/heap trace, per step.
test = args.test_binary or build('ordinary', ['cargo', 'test'] + base + ['--lib', '--no-run'])['acyclic_fs']
checked = run('differential-and-physical-controls', [test, 'kernel::allocation::accounting::charge_tests::'])
assert '3 passed' in checked.stdout and '0 failed' in checked.stdout, checked.stdout

if args.reuse_release_dir:
    bins = {stage: {name: args.reuse_release_dir / (stage + '-' + name)
                    for name in ['filesystem', 'filesystem_alloc']} for stage in ['before', 'after']}
else:
    source = pathlib.Path('rust/crates/filesystem/src/kernel/allocation/accounting.rs')
    candidate = source.read_bytes()
    bins = {}
    try:
        # Install the identical final benchmark harness around each implementation.
        # All other production changes in this PR are cfg(test) lock probes.
        for stage, contents in [('before', baseline), ('after', candidate)]:
            source.write_bytes(contents)
            bins[stage] = build(stage, ['cargo', 'bench'] + base + [
                '--bench', 'filesystem', '--bench', 'filesystem_alloc', '--no-run'])
    finally:
        source.write_bytes(candidate)

cores = {}
for cpu in sorted(os.sched_getaffinity(0)):
    topology = pathlib.Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
    cores.setdefault(((topology / 'physical_package_id').read_text(), (topology / 'core_id').read_text()), cpu)
cpus = sorted(cores.values())[-4:]
scale = {'ns': 1, 'µs': 1000, 'μs': 1000, 'ms': 1e6, 's': 1e9}
pattern = r'admission::(kernel_charge::|admission_charge_local)'


def timings(stdout, count):
    values = []
    for line in stdout.splitlines():
        cells = [re.search(r'([\d.,]+)\s*(ns|µs|μs|ms|s)\s*$', cell) for cell in line.split('│')]
        times = [float(match[1].replace(',', '')) * scale[match[2]] for match in cells if match]
        if len(times) == 4:
            values.append(times[2])
    assert len(values) == count, stdout
    return values


summary = []
for trace in [False, True]:
    observed = env.copy()
    observed['ACYCLIC_BENCH_WORK'] = '1'
    if trace:
        observed['ACYCLIC_BENCH_TRACE'] = '1'
    checks = {}
    for stage in ['before', 'after']:
        result = run(f'{stage}-heap-trace{int(trace)}', [bins[stage]['filesystem_alloc'],
                     r'admission::(kernel_charge|admission_charge_local)', '--bench', '--threads', '1',
                     '--sample-count', '1', '--sample-size', '1', '--color', 'never'], observed)
        receipts = [line for line in result.stderr.splitlines() if line.startswith('kernel_charge')]
        assert len(receipts) == 14, receipts
        # The profiler omits allocation columns entirely for zero allocation work.
        header = result.stdout.splitlines()[0]
        assert 'alloc' not in header.replace('filesystem_alloc', '') and 'bytes' not in header, header
        profiles = [line for line in result.stdout.splitlines() if '│' in line
                    and not re.search(r'\b(?:ns|µs|μs|ms|s|Mitem|Kitem)\b', line)]
        checks[stage] = (receipts, profiles)
    assert checks['before'] == checks['after'], ('full receipts/heap profiles', trace, checks)
    timing_env = env.copy()
    if trace:
        timing_env['ACYCLIC_BENCH_TRACE'] = '1'
    for threads in [1, 4]:
        for kind, selected, size, count in [('bulk', pattern, 8, 7), ('shared', r'admission::kernel_charge_shared', 128, 2)]:
            pairs = []
            for repeat in range(7):
                pair = {}
                for stage in (['before', 'after'] if repeat % 2 == 0 else ['after', 'before']):
                    label = f'{stage}-{kind}-threads{threads}-trace{int(trace)}-repeat{repeat}'
                    result = run(label, ['taskset', '-c', ','.join(map(str, cpus[:threads])),
                                 bins[stage]['filesystem'], selected, '--bench', '--threads', threads,
                                 '--timer', 'os', '--sample-count', '128', '--sample-size', size,
                                 '--max-time', '0.5', '--color', 'never'], timing_env)
                    pair[stage] = timings(result.stdout, count)
                pairs.append(pair)
            for index in range(count):
                changes = [100 * (1 - pair['after'][index] / pair['before'][index]) for pair in pairs]
                row = dict(trace=trace, threads=threads, kind=kind, case=index,
                           before_ns=statistics.median(pair['before'][index] for pair in pairs),
                           after_ns=statistics.median(pair['after'][index] for pair in pairs),
                           paired_percent=statistics.median(changes), range_percent=[min(changes), max(changes)])
                summary.append(row)
                print(json.dumps(row), flush=True)
(out / 'summary.json').write_text(json.dumps(summary, indent=2))
print('Exact differential/physical assertions, full receipt/heap comparisons, and seven paired runs passed.')
