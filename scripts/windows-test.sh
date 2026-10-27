#!/usr/bin/env bash
# Created by Codex. Run from your own SDK worktree. Cargo runs only on bb2.
# Example: bash Q:/lanes/win-test.sh acyclic-plugin --all-features --bin acyclic pi_native
set -euo pipefail
export MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1
# This value names a Linux directory consumed on bb2. MSYS otherwise rewrites
# its environment value before the Windows Python launcher can serialize it.
export MSYS2_ENV_CONV_EXCL="${MSYS2_ENV_CONV_EXCL:+$MSYS2_ENV_CONV_EXCL;}WIN_TEST_TARGET_DIR"
exec python.exe - "$@" <<'WIN_TEST_PY'
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import time
import uuid


def fail(message):
    raise RuntimeError(message)


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def relative(name):
    path = PurePosixPath(name)
    if not path.parts or path.is_absolute() or '..' in path.parts or '\\' in name or ':' in name:
        fail('Unsafe archive/source path: ' + name)
    return path


def arguments(args):
    cargo, runtime, filters = [], [], []
    values = {'--features', '-F', '--test', '--bin', '--example', '--profile',
              '--config', '-j', '--jobs', '--color'}
    forbidden = {'--target', '--target-dir', '--message-format', '--manifest-path',
                 '-p', '--package', '--workspace', '--all', '--exclude', '--doc'}
    i = 0
    while i < len(args):
        arg = args[i]
        key = arg.split('=', 1)[0]
        if arg == '--':
            runtime = args[i + 1:]
            break
        if key in forbidden:
            fail('The helper owns package, target, artifact format and workspace selection: ' + arg)
        if arg == '--no-run':
            i += 1
            continue
        if key in values:
            cargo.append(arg)
            if '=' not in arg:
                i += 1
                if i == len(args):
                    fail('Missing value for ' + arg)
                cargo.append(args[i])
        elif arg.startswith('-'):
            cargo.append(arg)
        else:
            filters.append(arg)
        i += 1
    if len(filters) > 1:
        fail('Use one Cargo test filter; put libtest options after --')
    if any(x.startswith('--test-threads') for x in runtime):
        fail('The helper fixes --test-threads=4, including child test processes')
    return cargo, filters + runtime


REMOTE = r'''
from pathlib import Path, PurePosixPath
import hashlib,json,os,subprocess,tarfile,time
c=CONFIG
os.environ.update(CARGO_INCREMENTAL='0', XWIN_ACCEPT_LICENSE='1')

assert os.environ.get('GATE_XWIN_CACHE_ROOT'), 'shared SDK workspace was not admitted'
print('WIN_TEST_XWIN_PRIVATE_CACHE',os.environ['XWIN_CACHE_DIR'],flush=True)
root=Path.cwd().resolve()
target=Path(os.environ['CARGO_TARGET_DIR']).resolve()
# CARGO_TARGET_DIR is the job-private mount supplied by bs.ps1.
print('WIN_TEST_TARGET_DIR',str(target),flush=True)
metadata=json.loads(subprocess.check_output(['cargo','metadata','--locked','--no-deps','--format-version=1'],text=True))
packages=[p for p in metadata['packages'] if p['name'] in (c['crate'],'acyclic-'+c['crate'])]
assert len(packages)==1, 'Unknown or ambiguous crate: '+c['crate']
p=packages[0]
start=time.monotonic()
base=['cargo','xwin','test','--no-run','--locked','--target','x86_64-pc-windows-msvc','-p',p['name'],'--message-format=json-render-diagnostics']
all_artifacts=[]
logs=[]
def build(cmd,label,package_id=p['id']):
    log=root/(c['stem']+'-'+label+'.jsonl')
    logs.append(log)
    print('WIN_TEST_BUILD',json.dumps(cmd),flush=True)
    with log.open('w') as output:
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=None,text=True)
        for line in proc.stdout:
            output.write(line)
            try: v=json.loads(line)
            except ValueError: continue
            if v.get('reason')=='compiler-artifact' and v.get('package_id')==package_id:
                all_artifacts.append((v,log))
        rc=proc.wait()
    assert rc==0, 'Cross-build failed with exit '+str(rc)+'; no Windows tests were run'
# A test build unifies dev-dependency features into normal bin artifacts.
# Own the production graph's immutable export before a test build can replace
# the same Cargo output pathname. Both graphs use the one artifact stager.
admission={'artifacts':{}}
rows=[]
host_rows=[]

def stage(label, artifacts, is_test):
    specifications=[];identities=set()
    for v,log in artifacts:
        if not v.get('executable') or bool(v['profile']['test']) != is_test:continue
        identity=(v['target']['name'],is_test)
        assert identity not in identities, 'Ambiguous package executable identity: '+str(identity)
        identities.add(identity)
        name=identity[0]+('-test' if is_test else '-cli')+'.exe'
        specifications.append(dict(name=name,target=identity[0],test=is_test,cargo_json=str(log)))
    assert specifications, 'Cargo emitted ZERO '+label+' executables for '+p['name']
    specification=root/(c['stem']+'-'+label+'-specification.json')
    specification.write_text(json.dumps(specifications))
    bundle=root/(c['stem']+'-'+label+'-artifacts')
    subprocess.run(['node','scripts/graphcoder-e2e/artifacts.mjs',str(specification),str(bundle)],check=True,stdout=subprocess.DEVNULL)
    selected=json.loads((bundle/'artifacts.json').read_text())['artifacts']
    assert not admission['artifacts'].keys() & selected.keys(), 'Duplicate staged artifact identity'
    admission['artifacts'].update(selected)
    for spec in specifications:
        value=selected[spec['name']]
        assert Path(value['source']).suffix.lower()=='.exe', 'Cargo artifact is not a Windows executable'
        row=dict(remote=value['path'],cargo_path=value['source'],name=spec['name'],target=spec['target'],test=is_test,sha256=value['sha256'])
        rows.append(row)
        print('WIN_TEST_REMOTE_EXE',json.dumps(row),flush=True)

# The sole composable host producer stages before test feature unification.
from types import ModuleType
host_factory=ModuleType('acyclic_native_host_producer')
host_factory.__file__=str(root/'scripts/windows-native-host.py')
exec(compile(Path(host_factory.__file__).read_bytes(),host_factory.__file__,'exec'),host_factory.__dict__)
host=host_factory.produce(root,root/(c['stem']+'-process-host'),
    expected_source=host_factory.source_catalog(root),cargo_args=c['cargo'])
host_rows.append(host['row'])
admission['artifacts'].update(host['admission'])
logs.append(host['log'])

keys={arg.split('=',1)[0] for arg in c['cargo']}
unit_selection=bool(keys & {'--lib','--bin','--bins','--example','--examples','--bench','--benches'})
integration_selection=bool(keys & {'--test','--tests','--all-targets'}) or not unit_selection
bins=[t for t in p['targets'] if 'bin' in t['kind'] and t['name']!='acyclic-process-host']
if integration_selection and bins:
    assert len(bins)==1, 'Ambiguous normal executable fixture: '+repr([t['name'] for t in bins])
    shared=[];i=0
    value_flags={'--features','-F','--profile','--config','-j','--jobs','--color'}
    switches={'--all-features','--no-default-features','--release','--offline','--frozen'}
    while i<len(c['cargo']):
        arg=c['cargo'][i];key=arg.split('=',1)[0]
        if key in value_flags:
            shared.append(arg)
            if '=' not in arg:i+=1;shared.append(c['cargo'][i])
        elif arg in switches:shared.append(arg)
        i+=1
    build(['cargo','xwin','build','--locked','--target','x86_64-pc-windows-msvc','-p',p['name'],'--bin',bins[0]['name'],'--message-format=json-render-diagnostics']+shared,'cli')
    stage('cli',all_artifacts,False)
    all_artifacts.clear()
build(base+c['cargo'],'tests')
stage('tests',all_artifacts,True)
assert any(r['test'] for r in rows), 'No test executables survived artifact selection'
files={}
# All tracked runtime fixtures, including corpora, source-policy inputs and scripts.
tracked=subprocess.check_output(['git','ls-files','-z'],text=True).split('\0')
for name in tracked+c['fixtures']:
    if not name:continue
    path=Path(name)
    assert not path.is_absolute() and '..' not in path.parts, name
    if path.is_dir():
        candidates=[x for x in path.rglob('*') if x.is_file()]
    else:candidates=[path]
    for f in candidates:
        assert not f.is_symlink(), 'Fixture symlinks require an explicit portable fixture: '+str(f)
        assert f.is_file(), 'Missing runtime fixture: '+str(f)
        files[f.as_posix()]=hashlib.file_digest(f.open('rb'),'sha256').hexdigest()
dlls={}
for row in rows+host_rows:
    folder=Path(row['cargo_path']).parent
    for directory in (folder,folder.parent,folder/'deps'):
        for f in directory.glob('*.dll'):
            h=hashlib.file_digest(f.open('rb'),'sha256').hexdigest()
            assert f.name not in dlls or dlls[f.name]['sha256']==h, 'Ambiguous runtime DLL '+f.name
            dlls[f.name]=dict(remote=str(f),sha256=h)
host_receipt=host['receipt']
host_receipt_path=host['receipt_path']
process_host=dict(host_rows[0],receipt=host_receipt,receipt_sha256=hashlib.file_digest(host_receipt_path.open('rb'),'sha256').hexdigest())
manifest=dict(crate=p['name'],crate_dir=Path(p['manifest_path']).parent.relative_to(root).as_posix(),target_dir=str(target),head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),artifacts=rows,artifact_admission=admission,process_host=process_host,source=files,dlls=dlls,build_seconds=time.monotonic()-start)
manifest_path=root/(c['stem']+'-manifest.json');manifest_path.write_text(json.dumps(manifest))
archive=root/(c['stem']+'.tar.gz')
with tarfile.open(archive,'w:gz',compresslevel=1) as tar:
    tar.add(manifest_path,arcname='manifest.json')
    for row in rows+host_rows:tar.add(row['remote'],arcname='bin/'+row['name'])
    tar.add(host_receipt_path,arcname='host-receipt.json')
    for name,row in dlls.items():tar.add(row['remote'],arcname='bin/'+name)
    for name in files:tar.add(name,arcname='source/'+name)
    for log in logs:tar.add(log,arcname='logs/'+log.name)
print('WIN_TEST_ARCHIVE',hashlib.file_digest(archive.open('rb'),'sha256').hexdigest(),archive.name,flush=True)
'''


INPUT_ROOT = '/mnt/bb/jobs'
# The consumer verifies and takes ownership before executing the program. The
# producer never removes an input which a dispatched job may still need.
CONSUME_INPUT = """import hashlib,pathlib,sys
p=pathlib.Path(sys.argv[1]); program=p.read_bytes()
if hashlib.sha256(program).hexdigest()!=sys.argv[2]: raise RuntimeError('Windows input digest mismatch')
p.unlink()
exec(compile(program,str(p),'exec'),{'__name__':'__main__'})
"""
STAGE_INPUT = """import hashlib,os,pathlib,sys
p=pathlib.Path(sys.argv[1]); program=sys.stdin.buffer.read()
if hashlib.sha256(program).hexdigest()!=sys.argv[2]: raise RuntimeError('Windows input transfer digest mismatch')
with os.fdopen(os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o444),'wb') as f:
 f.write(program); f.flush(); os.fsync(f.fileno())
print('WIN_TEST_INPUT',sys.argv[2],len(program),str(p),flush=True)
"""


def remote_invocation(config):
    # Only a digest and an immutable owned input path enter executor argv. Nested
    # executor shell quoting cannot expand the program beyond OS argument bounds.
    program = ('import json\nCONFIG=json.loads(' + repr(json.dumps(config)) + ')\n' + REMOTE).encode('utf-8')
    checksum = hashlib.sha256(program).hexdigest()
    filename = INPUT_ROOT + '/win-input-' + checksum + '.py'
    return ['python3', 'scripts/with-xwin-cache.py', '--workers', '1', '--', 'python3', '-c', CONSUME_INPUT, filename, checksum], program


def stage_invocation(command):
    return ['python3', '-c', STAGE_INPUT, *command[-2:]]


def runner_module(path):
    # Execute the admitted source directly, without importlib's bytecode cache.
    from types import ModuleType
    module = ModuleType('acyclic_windows_runner')
    module.__file__ = str(path)
    exec(compile(path.read_bytes(), str(path), 'exec'), module.__dict__)
    return module


def main():
    if os.name != 'nt':
        fail('Launch with Git Bash on Windows: bash Q:/lanes/win-test.sh <crate> [cargo args]')
    if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
        print('Usage from your worktree: bash Q:/lanes/win-test.sh <crate> [cargo test args] [FILTER] [-- libtest args]')
        print('Examples: acyclic-harness-core --all-features --lib hooks::release_tests')
        print('          acyclic-plugin --all-features --bin acyclic pi_native')
        print('Resume a completed producer: windows-test.sh --consume RECORDS [FILTER] [-- libtest args]')
        print('Optional WIN_TEST_FIXTURES: JSON array of additional worktree-relative fixture files/directories.')
        print('Optional WIN_TEST_SCRATCH_ROOT: Windows records directory (default D:/lane-scratch/<worktree>).')
        return 0 if len(sys.argv) > 1 else 2
    root = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
    os.chdir(root)
    if sys.argv[1] == '--consume':
        if len(sys.argv) < 3:
            fail('Usage: windows-test.sh --consume RECORDS [FILTER] [-- libtest args]')
        cargo, runtime = arguments(sys.argv[3:])
        if cargo:
            fail('--consume only accepts runtime filters/options; it never runs Cargo')
        return consume(root, Path(sys.argv[2]), runtime)
    cargo, runtime = arguments(sys.argv[2:])
    fixtures = json.loads(os.environ.get('WIN_TEST_FIXTURES', '[]'))
    if not isinstance(fixtures, list) or not all(isinstance(x, str) for x in fixtures):
        fail('WIN_TEST_FIXTURES must be a JSON array of relative paths')
    for name in fixtures:
        relative(name)
    for leftover in root.glob('.bb-*.tar*'):
        if leftover.is_file() and leftover.stat().st_size > 16 * 1024 * 1024:
            fail('A previous build archive would exceed build-slot sync limits; move it outside the worktree before retrying: ' + str(leftover))
    stem = '.bb-win-test-' + uuid.uuid4().hex[:12]
    scratch = Path(os.environ.get('WIN_TEST_SCRATCH_ROOT', 'D:/lane-scratch/' + root.name)).resolve()
    if not scratch.is_relative_to(Path('D:/lane-scratch').resolve()):
        fail('Windows lane scratch must stay under D:/lane-scratch: ' + str(scratch))
    records = scratch / 'win-test' / stem[-12:]
    records.mkdir(parents=True)
    if os.environ.get('WIN_TEST_TARGET_DIR') or os.environ.get('WIN_TEST_PERSISTENT_SOURCE_CLOCK'):
        fail('Windows builds require build-slot job-owned targets; custom targets/source clocks are prohibited')
    for shared in ('scripts/with-xwin-cache.py', 'scripts/windows-runner.py', 'scripts/windows-native-host.py', 'scripts/windows-artifacts.py', 'scripts/graphcoder-e2e/materialize.py'):
        path = root / shared
        if not path.is_file() or path.is_symlink():
            fail('The source snapshot must contain the shared SDK factory: ' + shared)
    config = dict(crate=sys.argv[1], cargo=cargo, stem=stem, fixtures=fixtures)
    payload, program = remote_invocation(config)
    import shlex
    # This transfers bytes only. Compilation remains inside bs.ps1 below.
    stage = ['wsl.exe', '-d', 'Ubuntu', '-e', 'ssh', '-F', '/home/var/.ssh/buildbox/config', '-o', 'BatchMode=yes', 'bb2', shlex.join(stage_invocation(payload))]
    subprocess.run(stage, input=program, check=True)
    env = os.environ.copy()
    # Copy-back follows successful producer receipt and disk-capacity admission.
    env.pop('BUILD_SLOT_REMOTE_FETCH', None)
    print('WIN_TEST_RECORDS', records, flush=True)
    with (records / 'build.log').open('w', encoding='utf-8') as log:
        process = subprocess.Popen(['pwsh.exe', '-NoProfile', '-File', 'Q:/lanes/bs.ps1', *payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
        for line in process.stdout:
            log.write(line); log.flush()
            print(line, end='', flush=True)
        rc = process.wait()
    if rc:
        fail('bb2 build/export failed (exit ' + str(rc) + '); see ' + str(records / 'build.log'))
    return consume(root, records, runtime)


def consume(root, records, runtime):
    records = records.absolute()
    if not records.resolve().is_relative_to(Path('D:/lane-scratch').resolve()):
        fail('Windows export records must stay under D:/lane-scratch')
    if not records.is_dir() or records.is_symlink() or records.is_junction():
        fail('Windows export records must be an existing owned directory')
    admission = runner_module(root / 'scripts/windows-artifacts.py')
    archive, checksum = admission.fetch_export(records, (records / 'build.log').read_text(encoding='utf-8'))
    admitted = admission.admit_archive(records, archive, checksum)
    return run_tests(admitted.directory, admitted.manifest, runtime)


def run_tests(records, manifest, runtime):
    # Execute the exact shared runner admitted with this producer's source.
    path = records / 'source/scripts/windows-runner.py'
    expected = manifest['source'].get('scripts/windows-runner.py')
    if not expected or digest(path) != expected:
        fail('Shared Windows runner is absent or differs from admitted source')
    module = runner_module(path)
    return module.run_tests(records, manifest, runtime)


try:
    sys.exit(main())
except (RuntimeError, AssertionError, OSError, ValueError, subprocess.SubprocessError) as error:
    print('WIN_TEST_ERROR:', error, file=sys.stderr, flush=True)
    sys.exit(1)
WIN_TEST_PY
