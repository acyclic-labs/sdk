"""Portable paired name timing (macOS/Windows): python3 workspace_name_pair_portable.py baseline.json outdir"""
import json, os, pathlib, re, shutil, statistics, subprocess, sys, time
baseline = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding='utf-8'))
out = pathlib.Path(sys.argv[2]).resolve(); out.mkdir(parents=True, exist_ok=True)
features = 'distributed,local,memory,native-watch,native-mount,model-checking,fuzzing'
candidate = {p: pathlib.Path(p).read_bytes() for p in baseline}
env = {k: v for k, v in os.environ.items() if not k.startswith('ACYCLIC_BENCH')}
bins = {}
try:
    for stage in ['before', 'after']:
        for p in baseline:
            pathlib.Path(p).write_bytes(baseline[p].encode() if stage == 'before' else candidate[p])
        started = time.monotonic()
        r = subprocess.run(['cargo', 'bench', '--locked', '-p', 'acyclic-fs', '--no-default-features', '--features', features,
                            '--bench', 'filesystem', '--no-run', '--message-format=json'], capture_output=True, text=True, encoding='utf-8')
        (out / (stage + '-build.err')).write_text(r.stderr)
        print(f'NAME_BUILD {stage} seconds={time.monotonic()-started:.3f}',flush=True)
        if r.returncode: sys.exit(r.stderr[-4000:])
        for line in r.stdout.splitlines():
            e = json.loads(line)
            if e.get('reason') == 'compiler-artifact' and e.get('executable') and e['target']['name'] == 'filesystem':
                dst = out / (stage + '-filesystem' + pathlib.Path(e['executable']).suffix)
                shutil.copyfile(e['executable'], dst); os.chmod(dst, 0o755); bins[stage] = dst
finally:
    for p, c in candidate.items(): pathlib.Path(p).write_bytes(c)
scale = {'ns': 1, 'µs': 1000, 'μs': 1000, 'ms': 1e6, 's': 1e9}
def timings(stdout):
    values = []
    for line in stdout.splitlines():
        m = [re.search(r'([\d.,]+)\s*(ns|µs|μs|ms|s)\s*$', c) for c in line.split('│')]
        t = [float(x[1].replace(',', '')) * scale[x[2]] for x in m if x]
        if len(t) == 4: values.append(t[2])
    return values
pairs = []
for repeat in range(7):
    pair = {}
    for stage in (['before', 'after'] if repeat % 2 == 0 else ['after', 'before']):
        e = dict(env)
        if stage == 'before': e['ACYCLIC_BENCH_LEGACY_NAME'] = '1'
        r = subprocess.run([str(bins[stage]), 'workspace_names::workspace_name::', '--bench', '--threads', '1',
                            '--timer', 'os', '--sample-count', '128', '--sample-size', '8', '--max-time', '0.1', '--color', 'never'],
                           env=e, capture_output=True, text=True, encoding='utf-8')
        if r.returncode: sys.exit(r.stdout[-3000:] + r.stderr[-3000:])
        pair[stage] = timings(r.stdout)
        assert len(pair[stage]) == 12, r.stdout
    pairs.append(pair)
labels = [f'{n}-{"native" if native else "standalone"}' for n in ['private', 'pin47', 'cafe', 'combining', 'ascii255', 'emoji16'] for native in [False, True]]
for i, label in enumerate(labels):
    ch = [100 * (1 - p['after'][i] / p['before'][i]) for p in pairs]
    print(f"PAIR {sys.platform} {label} before={statistics.median(p['before'][i] for p in pairs):.1f} after={statistics.median(p['after'][i] for p in pairs):.1f} faster={statistics.median(ch):.1f}% range=[{min(ch):.1f},{max(ch):.1f}]", flush=True)
