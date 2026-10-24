#!/usr/bin/env bash
# Created by Codex. Run from your own SDK worktree. Cargo runs only on bb2.
# Example: bash Q:/lanes/win-test.sh acyclic-plugin --all-features --bin acyclic pi_native
set -euo pipefail
export MSYS2_ARG_CONV_EXCL='*' PYTHONUTF8=1
# This value names a Linux directory consumed on bb2. MSYS otherwise rewrites
# its environment value before the Windows Python launcher can serialize it.
export MSYS2_ENV_CONV_EXCL="${MSYS2_ENV_CONV_EXCL:+$MSYS2_ENV_CONV_EXCL;}WIN_TEST_TARGET_DIR"
exec python.exe - "$@" <<'WIN_TEST_PY'
import ctypes
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys
import tarfile
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
import fcntl,hashlib,json,os,subprocess,tarfile,tempfile,time
c=CONFIG
os.environ.update(CARGO_INCREMENTAL='0', XWIN_ACCEPT_LICENSE='1')

def xwin_cache_snapshot(seed):
    # Only preparation and snapshotting share a mutable namespace. Compiles
    # own separate CoW files; cargo-xwin's destructive setup retry can never
    # delete another live compile's SDK, or hold the seed lease for its build.
    seed.mkdir(parents=True,exist_ok=True)
    owner=tempfile.TemporaryDirectory(prefix='win-test-xwin-')
    snapshot=Path(owner.name)/'xwin'
    snapshot.mkdir()
    try:
        with (seed/'.win-test.lock').open('a') as lease:
            fcntl.flock(lease,fcntl.LOCK_EX)
            env=dict(os.environ,XWIN_CACHE_DIR=str(seed))
            subprocess.run(['cargo','xwin','cache','xwin'],env=env,check=True)
            subprocess.run(['cp','-a','--reflink=auto','--',
                            str(seed/'xwin'/'crt'),str(seed/'xwin'/'sdk'),
                            str(seed/'xwin'/'DONE'),str(snapshot)],check=True)
            for entry in snapshot.rglob('*'):
                if entry.is_symlink() and not entry.resolve(strict=True).is_relative_to(snapshot):
                    raise RuntimeError('SDK alias escapes its private snapshot: '+str(entry))
        return owner
    except BaseException:
        owner.cleanup()
        raise

cache_snapshot=xwin_cache_snapshot(Path('/mnt/bb/toolchains/win-test-xwin-v1'))
os.environ['XWIN_CACHE_DIR']=cache_snapshot.name
print('WIN_TEST_XWIN_PRIVATE_CACHE',cache_snapshot.name,flush=True)
root=Path.cwd().resolve()
if c.get('target_dir'):
    selected=Path(c['target_dir']).resolve()
    assert selected.is_relative_to(Path('/mnt/bb/targets')) and selected != Path('/mnt/bb/targets'), 'Windows targets must stay under /mnt/bb/targets'
    selected.mkdir(parents=True,exist_ok=True)
    (selected/'.build-slot-lease').touch()
    os.environ['CARGO_TARGET_DIR']=str(selected)
target=Path(os.environ['CARGO_TARGET_DIR']).resolve()
if c.get('target_clock'):
    clock_scope={'__name__':'cargo_target_clock'}
    exec(compile(c['target_clock'],'cargo-target-clock.py','exec'),clock_scope)
    target_owner=clock_scope['prepare'](root,target)
    print('WIN_TEST_SOURCE_CLOCK_SHA256',hashlib.sha256(c['target_clock'].encode()).hexdigest(),flush=True)
print('WIN_TEST_TARGET_DIR',str(target),flush=True)
metadata=json.loads(subprocess.check_output(['cargo','metadata','--locked','--no-deps','--format-version=1'],text=True))
packages=[p for p in metadata['packages'] if p['name'] in (c['crate'],'acyclic-'+c['crate'])]
assert len(packages)==1, 'Unknown or ambiguous crate: '+c['crate']
p=packages[0]
start=time.monotonic()
base=['cargo','xwin','test','--no-run','--locked','--target','x86_64-pc-windows-msvc','-p',p['name'],'--message-format=json-render-diagnostics']
all_artifacts=[]
logs=[]
def build(cmd,label):
    if c.get('target_clock'):
        refreshed_owner=clock_scope['prepare'](root,target,inherited=target_owner.fileno())
        refreshed_owner.close()
    log=root/(c['stem']+'-'+label+'.jsonl')
    logs.append(log)
    print('WIN_TEST_BUILD',json.dumps(cmd),flush=True)
    with log.open('w') as output:
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=None,text=True)
        for line in proc.stdout:
            output.write(line)
            try: v=json.loads(line)
            except ValueError: continue
            if v.get('reason')=='compiler-artifact' and v.get('package_id')==p['id']:
                all_artifacts.append(v)
        rc=proc.wait()
    assert rc==0, 'Cross-build failed with exit '+str(rc)+'; no Windows tests were run'
build(base+c['cargo'],'tests')
tests=[v for v in all_artifacts if v.get('profile',{}).get('test') and v.get('executable')]
if not tests:
    print('WIN_TEST_ARTIFACT_DIAGNOSTIC',json.dumps([dict(target=v['target']['name'],profile=v.get('profile'),executable=v.get('executable'),filenames=v.get('filenames')) for v in all_artifacts]),flush=True)
    raise AssertionError('Cargo emitted ZERO test executables for '+p['name']+'; check --lib/--bin/--test, required-features and cfg gates. No local run attempted.')
# Only integration selections need Cargo's normal CLI fixture. Unit-only
# selections must not trigger a second feature graph and unnecessary rebuild.
if any('test' in v['target']['kind'] for v in tests) and any(t['name']=='acyclic' and 'bin' in t['kind'] for t in p['targets']):
    if not any(v.get('executable') and not v.get('profile',{}).get('test') and v['target']['name']=='acyclic' for v in all_artifacts):
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
        build(['cargo','xwin','build','--locked','--target','x86_64-pc-windows-msvc','-p',p['name'],'--bin','acyclic','--message-format=json-render-diagnostics']+shared,'cli')
rows=[];seen=set()
for v in all_artifacts:
    if not v.get('executable'):continue
    f=Path(v['executable'])
    if str(f) in seen:continue
    seen.add(str(f))
    assert f.is_file() and f.suffix.lower()=='.exe', 'Cargo JSON executable does not exist ON BB2: '+str(f)
    row=dict(remote=str(f),name=f.name,target=v['target']['name'],test=bool(v.get('profile',{}).get('test')),sha256=hashlib.file_digest(f.open('rb'),'sha256').hexdigest())
    rows.append(row)
    print('WIN_TEST_REMOTE_EXE',json.dumps(row),flush=True)
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
for row in rows:
    folder=Path(row['remote']).parent
    for directory in (folder,folder.parent,folder/'deps'):
        for f in directory.glob('*.dll'):
            h=hashlib.file_digest(f.open('rb'),'sha256').hexdigest()
            assert f.name not in dlls or dlls[f.name]['sha256']==h, 'Ambiguous runtime DLL '+f.name
            dlls[f.name]=dict(remote=str(f),sha256=h)
manifest=dict(crate=p['name'],crate_dir=Path(p['manifest_path']).parent.relative_to(root).as_posix(),target_dir=str(target),head=subprocess.check_output(['git','rev-parse','HEAD'],text=True).strip(),artifacts=rows,source=files,dlls=dlls,build_seconds=time.monotonic()-start)
manifest_path=root/(c['stem']+'-manifest.json');manifest_path.write_text(json.dumps(manifest))
archive=root/(c['stem']+'.tar.gz')
with tarfile.open(archive,'w:gz',compresslevel=1) as tar:
    tar.add(manifest_path,arcname='manifest.json')
    for row in rows:tar.add(row['remote'],arcname='bin/'+row['name'])
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
    return ['python3', '-c', CONSUME_INPUT, filename, checksum], program


def stage_invocation(command):
    return ['python3', '-c', STAGE_INPUT, *command[-2:]]


def main():
    if os.name != 'nt':
        fail('Launch with Git Bash on Windows: bash Q:/lanes/win-test.sh <crate> [cargo args]')
    if len(sys.argv) < 2 or sys.argv[1] in ('-h', '--help'):
        print('Usage from your worktree: bash Q:/lanes/win-test.sh <crate> [cargo test args] [FILTER] [-- libtest args]')
        print('Examples: acyclic-harness-core --all-features --lib hooks::release_tests')
        print('          acyclic-plugin --all-features --bin acyclic pi_native')
        print('Optional WIN_TEST_FIXTURES: JSON array of additional worktree-relative fixture files/directories.')
        print('Optional WIN_TEST_SCRATCH_ROOT: Windows records directory (default D:/lane-scratch/<worktree>).')
        print('Optional WIN_TEST_TARGET_DIR: reuse the lane target below /mnt/bb/targets on bb2.')
        return 0 if len(sys.argv) > 1 else 2
    root = Path(subprocess.check_output(['git', 'rev-parse', '--show-toplevel'], text=True).strip()).resolve()
    os.chdir(root)
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
    archive = records / (stem + '.tar.gz')
    config = dict(crate=sys.argv[1], cargo=cargo, stem=stem, fixtures=fixtures,
                  target_dir=os.environ.get('WIN_TEST_TARGET_DIR'))
    if os.environ.get('WIN_TEST_PERSISTENT_SOURCE_CLOCK') == '1':
        if not config['target_dir']:
            fail('A persistent source clock requires an explicit lane target')
        config['target_clock'] = Path('Q:/lanes/cargo-target-clock.py').read_text(encoding='utf-8')
    payload, program = remote_invocation(config)
    import shlex
    # This transfers bytes only. Compilation remains inside bs.ps1 below.
    stage = ['wsl.exe', '-d', 'Ubuntu', '-e', 'ssh', '-F', '/home/var/.ssh/buildbox/config', '-o', 'BatchMode=yes', 'bb2', shlex.join(stage_invocation(payload))]
    subprocess.run(stage, input=program, check=True)
    env = os.environ.copy()
    env['BUILD_SLOT_REMOTE_FETCH'] = archive.name
    fetch_dir = '/mnt/' + records.drive[0].lower() + records.as_posix()[2:]
    print('WIN_TEST_RECORDS', records, flush=True)
    with (records / 'build.log').open('w', encoding='utf-8') as log:
        process = subprocess.Popen(['pwsh.exe', '-NoProfile', '-File', 'Q:/lanes/bs.ps1', '--env', 'BUILD_SLOT_REMOTE_FETCH_DIR=' + fetch_dir, *payload], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
        for line in process.stdout:
            log.write(line); log.flush()
            print(line, end='', flush=True)
        rc = process.wait()
    if rc:
        fail('bb2 build/export failed (exit ' + str(rc) + '); see ' + str(records / 'build.log'))
    matches = re.findall(r'WIN_TEST_ARCHIVE ([0-9a-f]{64}) ' + re.escape(archive.name), (records / 'build.log').read_text(encoding='utf-8'))
    if not archive.is_file() or len(matches) != 1:
        fail('Build succeeded but BUILD_SLOT_REMOTE_FETCH did not return exactly one archive: ' + str(archive))
    if digest(archive) != matches[0]:
        fail('Fetched archive SHA-256 mismatch')
    saved = records / 'artifacts.tar.gz'
    shutil.move(str(archive), saved)
    with tarfile.open(saved, 'r|gz') as tar:
        manifest = None; allowed = {}; seen = set()
        for member in tar:
            relative(member.name)
            if not member.isfile():
                fail('Unexpected non-file archive member ' + member.name)
            if member.name == 'manifest.json' and manifest is None:
                manifest = json.load(tar.extractfile(member))
                allowed.update({'bin/' + r['name']: r['sha256'] for r in manifest['artifacts']})
                allowed.update({'bin/' + n: r['sha256'] for n, r in manifest['dlls'].items()})
                allowed.update({'source/' + n: h for n, h in manifest['source'].items()})
                (records / 'manifest.json').write_text(json.dumps(manifest, indent=2))
                continue
            if manifest is None or member.name in seen:
                fail('Missing manifest or duplicate member: ' + member.name)
            if member.name not in allowed and not (member.name.startswith('logs/' + stem) and member.name.endswith('.jsonl')):
                fail('Unexpected archive member ' + member.name)
            dest = records.joinpath(*relative(member.name).parts)
            dest.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(member) as src, dest.open('wb') as out:
                shutil.copyfileobj(src, out)
            if member.name in allowed and digest(dest) != allowed[member.name]:
                fail('Fetched file SHA-256 mismatch: ' + member.name)
            seen.add(member.name)
        if manifest is None or not set(allowed).issubset(seen):
            fail('Incomplete artifact/fixture fetch')
    print('WIN_TEST_FETCH_VERIFIED', len(manifest['artifacts']), 'executables,', len(manifest['source']), 'source/fixture files', flush=True)
    return run_tests(records, manifest, runtime)

def run_tests(records, manifest, runtime):
    # The runner is also used by fixture drivers launched outside this shell.
    # Own the output encoding before forwarding arbitrary UTF-8 TUI frames.
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    kernel = ctypes.WinDLL('kernel32', use_last_error=True)
    kernel.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
    kernel.CreateMutexW.restype = ctypes.c_void_p
    kernel.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
    kernel.ReleaseMutex.argtypes = [ctypes.c_void_p]
    kernel.CloseHandle.argtypes = [ctypes.c_void_p]
    mutex = kernel.CreateMutexW(None, False, 'Global\\AcyclicWindowsTestRun')
    if not mutex:
        raise ctypes.WinError(ctypes.get_last_error())
    acquired = False
    results = []
    proc = None
    try:
        print('WIN_TEST_WAITING_FOR_GLOBAL_MUTEX', flush=True)
        waited = 0
        while True:
            status = kernel.WaitForSingleObject(mutex, 1000)
            if status in (0, 0x80):
                acquired = True
                break
            if status != 0x102:
                raise ctypes.WinError(ctypes.get_last_error())
            waited += 1
            if waited % 30 == 0:
                print('WIN_TEST_MUTEX_WAIT_SECONDS', waited, flush=True)
        for row in manifest['artifacts']:
            path = (records / 'bin' / row['name']).resolve()
            print('WIN_TEST_LOCAL_EXE', path, 'test=' + str(row['test']), flush=True)
            if not row['test']:
                continue
            home = records / 'profiles' / row['sha256'][:8] / 'home'
            codex = home / '.codex-fixture'
            temp = records / 'tmp' / row['sha256'][:8]
            for folder in (codex, temp, home / 'AppData/Local', home / 'AppData/Roaming'):
                folder.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            for key in list(env):
                if key.endswith('API_KEY') or 'AUTH_TOKEN' in key or key in ('OPENAI_ACCESS_TOKEN', 'CODEX_ACCESS_TOKEN'):
                    env.pop(key)
            env.pop('RUST_MIN_STACK', None)
            env.update(HOME=str(home), USERPROFILE=str(home), CODEX_HOME=str(codex), TEMP=str(temp), TMP=str(temp), LOCALAPPDATA=str(home / 'AppData/Local'), APPDATA=str(home / 'AppData/Roaming'), HOMEDRIVE=home.drive, HOMEPATH=str(home)[len(home.drive):], RUST_TEST_THREADS='4', ACYCLIC_TEST_SOURCE_ROOT=str((records / 'source').resolve()), GIT_CONFIG_NOSYSTEM='1', GIT_CONFIG_GLOBAL=str(home / '.gitconfig'))
            cli = next((r for r in manifest['artifacts'] if not r['test'] and r['target'] == 'acyclic'), None)
            if cli:
                env['ACYCLIC_TEST_BINARY'] = str((records / 'bin' / cli['name']).resolve())
            else:
                env.pop('ACYCLIC_TEST_BINARY', None)
            args = [str(path)] + runtime + ['--test-threads=4']
            start = time.monotonic()
            with (records / (path.stem + '.log')).open('w', encoding='utf-8') as log:
                proc = subprocess.Popen(args, cwd=records / 'source' / manifest['crate_dir'], env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8', errors='replace')
                summaries = []
                for line in proc.stdout:
                    log.write(line); log.flush()
                    print(line, end='', flush=True)
                    if 'test result:' in line:
                        summaries.append(line.strip())
                rc = proc.wait()
            result = dict(target=row['target'], executable=str(path), exit=rc, seconds=round(time.monotonic()-start, 4), under_10s=time.monotonic()-start < 10, summaries=summaries, sha256=row['sha256'])
            results.append(result)
            (records / 'results.json').write_text(json.dumps(results, indent=2))
            print('WIN_TEST_RESULT', json.dumps(result), flush=True)
            if rc:
                return rc if 0 < rc < 256 else 1
        # Cargo accepts a misspelled filter and happily reports zero tests. Reject it.
        summaries = [s for r in results for s in r['summaries']]
        if runtime and summaries and not any(re.search(r'\b[1-9][0-9]* passed\b', s) for s in summaries) and '--list' not in runtime:
            fail('Filter selected ZERO passing tests. This is not verification: ' + repr(runtime))
        print('WIN_TEST_COMPLETE', len(results), 'suites;', records / 'results.json', flush=True)
        return 0
    finally:
        # Reap our active test tree before releasing the fleet mutex on interruption.
        if proc is not None and proc.poll() is None:
            subprocess.run(['taskkill.exe', '/PID', str(proc.pid), '/T', '/F'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            proc.wait()
        if acquired:
            kernel.ReleaseMutex(mutex)
        kernel.CloseHandle(mutex)


try:
    sys.exit(main())
except (RuntimeError, AssertionError, OSError, ValueError, subprocess.SubprocessError) as error:
    print('WIN_TEST_ERROR:', error, file=sys.stderr, flush=True)
    sys.exit(1)
WIN_TEST_PY
