#!/usr/bin/env python3
"""The privileged, test-gated publication boundary for bb2 tools.

Normal users cannot modify the root-owned bin directory or published files.
Run the reviewed candidate's bb-install.py --source <its scripts/buildbox>.
The candidate owns its complete tool set and qualification policy; an installed
predecessor cannot qualify additions it does not know about.
The fixed test policy runs as var against a frozen candidate before any rename.
"""
import argparse
import contextlib
import fcntl
import importlib.util
import hashlib
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile
import shlex

BIN = Path('/home/var/bin')
NODE = '/home/var/tools/node24/node-v24.3.0-linux-x64/bin/node'
# The complete tool set of one generation; it is only ever published whole.
TOOLS = ['admission_policy.py', 'input_closure.py', 'job_caps.py', 'cpu_leases.py', 'bbcgroup-systemd-run', 'bb-cache.py', 'sccache-rustc', 'job_exports.py', 'bb_target_store.py', 'bb_cargo_cache.py',
         'prune-wt-sdk.py', 'buildbox-receipt.mjs', 'bb-job.py', 'bb-admit.py', 'bb-admit', 'build-slot', 'bb-install.py']
# Generations are complete, immutable tool directories. Store entries are
# absolute links through CURRENT, so one rename switches every tool at once and
# a name absent from the live generation is absent (dangling), never stale.
ROOT = Path('/opt/buildbox')
CANARY_TIMEOUT = 300


def cache_authority(generation):
    spec=importlib.util.spec_from_file_location('generation_cache_'+Path(generation).name.replace('-','_'),Path(generation)/'bb-cache.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def secure_bin(alias=BIN, store=Path('/opt/buildbox/bin'), persistent=True):
    """Pin the legacy entry point to a store below protected root parents.

    A root-owned directory alone can be renamed by its writable HOME parent.
    A bind mount cannot: rename/unlink of the mountpoint returns EBUSY. Keep
    the canonical store outside HOME, with every ancestor root-protected.
    """
    alias=Path(alias); store=Path(store)
    store.mkdir(parents=True, exist_ok=True, mode=0o755)
    for parent in [store, *store.parents]:
        info=parent.stat()
        if parent.is_symlink() or info.st_uid != 0 or info.st_mode & 0o022:
            raise PermissionError('unprotected canonical tool parent: '+str(parent))
    # stat-based ismount misses same-filesystem bind mounts.
    if subprocess.run(['mountpoint', '-q', str(alias)]).returncode != 0:
        for directory, dirs, files in os.walk(alias, followlinks=False):
            destination=store/Path(directory).relative_to(alias)
            destination.mkdir(exist_ok=True, mode=0o755)
            for name in [*dirs, *files]:
                if (Path(directory)/name).is_symlink():raise ValueError('tool alias symlink refused')
            for name in files:publish(destination/name,(Path(directory)/name).read_bytes())
        if persistent:
            unit=b'[Unit]\nDescription=Qualified buildbox tool entry point\n[Mount]\nWhat=/opt/buildbox/bin\nWhere=/home/var/bin\nType=none\nOptions=bind,ro\n[Install]\nWantedBy=local-fs.target\n'
            publish('/etc/systemd/system/home-var-bin.mount',unit,0o644)
            subprocess.run(['systemctl','daemon-reload'],check=True)
            subprocess.run(['systemctl','enable','--now','home-var-bin.mount'],check=True)
        else:subprocess.run(['mount','--bind',str(store),str(alias)],check=True)
    if (alias.stat().st_dev,alias.stat().st_ino)!=(store.stat().st_dev,store.stat().st_ino):
        raise PermissionError('tool entry point is not pinned to its canonical store')
    subprocess.run(['mount','-o','remount,bind,ro',str(alias)],check=True)
    # Refresh persistence even when upgrading an already pinned entry.
    if persistent:
        unit=Path('/etc/systemd/system/home-var-bin.mount')
        publish(unit,unit.read_bytes().replace(b'Options=bind\n',b'Options=bind,ro\n'),0o644)
        subprocess.run(['systemctl','daemon-reload'],check=True)
    return store


def publish(destination, data, mode=0o555):
    """Same-directory write + fsync + rename + directory fsync, never truncate."""
    destination = Path(destination)
    fd, tmp = tempfile.mkstemp(prefix='.' + destination.name + '.', dir=destination.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(data); stream.flush(); os.fchmod(stream.fileno(), mode); os.fsync(stream.fileno())
        os.replace(tmp, destination)
        parent = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(parent)
        finally: os.close(parent)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def qualify(source, user):
    commands = [[NODE, '--test', 'generated.test.mjs', 'receipt-namespace.test.mjs'],
                [NODE, '--test', 'receipt-outcome.test.mjs'],
                [NODE, '--test', 'prune-admission.test.mjs'],
                ['python3', '-B', 'cpu_test_fixture.py', 'runtime.test.py'],
                ['python3', '-B', 'cpu_test_fixture.py', 'admission.test.py'],
                ['python3', '-B', 'cpu_test_fixture.py', 'command_custody.test.py'],
                ['python3', 'install.test.py'], ['python3', 'limits.test.py'],
                ['python3', '-B', 'input_closure.test.py'],
                ['python3', 'prune.test.py'],
                ['python3', 'reaping.test.py'],
                ['python3', '-B', 'cache_policy.test.py'],
                ['python3', '-B', 'cpu_leases.test.py'],
                ['python3', '-B', 'job_caps.test.py'],
                ['python3', 'cargo_cache.test.py']]
    for command in commands:
        # The tested code cannot modify its frozen source or installed tools.
        subprocess.run(['runuser', '-u', user, '--', *command], cwd=source, check=True, stdout=sys.stderr)


def prepare(source):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo; direct tool edits are refused')
    if file_hash(Path(source)/'bb-install.py')!=file_hash(Path(__file__)):
        raise ValueError('run the reviewed source installer; its tool set and qualification policy must match the candidate')
    account = pwd.getpwnam('var')
    stage = Path(tempfile.mkdtemp(prefix='bb-install-', dir='/mnt/bb/tmp')); stage.chmod(0o755)
    try:
        # Copy only regular source files. No symlinks or mutable shared test
        # dependencies enter the qualification generation.
        for item in Path(source).rglob('*'):
            if '__pycache__' in item.parts: continue
            if item.parent==Path(source) and item.name in ('.qualified.json','.predecessors.json'):continue
            if item.is_symlink(): raise ValueError('tool source symlink refused: ' + str(item))
            if item.is_file() and not item.name.endswith('.bak-gatespeed'):
                destination=stage/item.relative_to(source)
                destination.parent.mkdir(parents=True, exist_ok=True)
                publish(destination, item.read_bytes(), 0o444)
        qualify_generation(stage, account.pw_name)
        return stage
    except BaseException:
        # A failed candidate remains evidence but has no qualification token.
        raise


def file_hash(file):
    return hashlib.sha256(file.read_bytes()).hexdigest() if file.exists() else None


def candidate_directory(candidate):
    stage=Path(candidate)
    if stage.parent != Path('/mnt/bb/tmp') or not stage.name.startswith('bb-install-') or stage.is_symlink():
        raise ValueError('not an installer candidate')
    return stage


def qualify_generation(stage, user):
    # Qualification tokens are never inherited from the preceding generation.
    (stage/'.qualified.json').unlink(missing_ok=True)
    hashes={name:file_hash(stage/name) for name in TOOLS}
    if any(value is None for value in hashes.values()):
        raise ValueError('qualification requires every candidate tool')
    previous={name:file_hash(BIN/name) for name in TOOLS}
    publish(stage/'.predecessors.json',json.dumps(previous).encode(),0o400)
    qualify(stage,user)
    publish(stage/'.qualified.json',json.dumps(hashes).encode(),0o400)


def carried(source):
    """Files the installer does not manage (other owners' shims and binaries)
    carry forward byte-exactly. Tools managed by this or the predecessor's
    installer never do, so a tool dropped from the set is really gone."""
    source=Path(source) if source else None
    if source is None or not source.is_dir():return {}
    managed=set(TOOLS)|{'bbcg','installed-tools.json'}
    try:managed|=set(json.loads((source/'installed-tools.json').read_text())['sha256'])
    except (FileNotFoundError,KeyError,ValueError):pass
    files={}
    for file in source.rglob('*'):
        relative=file.relative_to(source)
        if relative.parts[0] in managed or relative.parts[0].startswith('.'):continue
        if file.is_symlink():raise ValueError('unmanaged tool link refused: '+str(file))
        if file.is_file():files[str(relative)]=file.read_bytes()
    return files


def generation_payload(payload, hashes):
    """The complete on-disk generation: every tool plus its sudo-path shim."""
    files={name:data for name,data in payload.items()}
    if 'bbcgroup-systemd-run' in files:files['bbcg/systemd-run']=files['bbcgroup-systemd-run']
    files['installed-tools.json']=json.dumps({'schema_version':1,'sha256':hashes}).encode()
    return files


def fsync_directory(directory):
    descriptor=os.open(directory,os.O_RDONLY|os.O_DIRECTORY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def materialize(files, name):
    """Write a generation beside its siblings, then expose it by one rename."""
    generations=ROOT/'generations';generations.mkdir(parents=True,exist_ok=True,mode=0o755)
    generation=generations/name
    if generation.exists():
        if {str(p.relative_to(generation)):p.read_bytes() for p in generation.rglob('*') if p.is_file()}!=files:
            raise ValueError('generation identity collision: '+str(generation))
        return generation
    temporary=Path(tempfile.mkdtemp(prefix='.tmp-'+name+'-',dir=generations))
    for relative,data in sorted(files.items()):
        destination=temporary/relative;destination.parent.mkdir(parents=True,exist_ok=True,mode=0o755)
        publish(destination,data,0o444 if relative=='installed-tools.json' else 0o555)
    for directory in [temporary,*[p for p in temporary.rglob('*') if p.is_dir()]]:
        if os.geteuid()==0:os.chown(directory,0,0)
        directory.chmod(0o755);fsync_directory(directory)
    os.rename(temporary,generation);fsync_directory(generations)
    return generation


def link(path, target):
    path=Path(path);temporary=path.with_name('.'+path.name+'.link-'+str(os.getpid()))
    temporary.unlink(missing_ok=True);os.symlink(str(target),temporary)
    os.replace(temporary,path);fsync_directory(path.parent)


def current_generation():
    current=ROOT/'current'
    return Path(os.readlink(current)) if current.is_symlink() else None


def adopt_legacy(store):
    """A pre-generation store of regular files becomes a retained generation."""
    files={}
    for file in Path(store).rglob('*'):
        if file.is_symlink():raise ValueError('legacy tool store link refused: '+str(file))
        if file.is_file():files[str(file.relative_to(store))]=file.read_bytes()
    return materialize(files,'legacy-'+hashlib.sha256(json.dumps(sorted((k,hashlib.sha256(v).hexdigest()) for k,v in files.items())).encode()).hexdigest()[:16])


def switch(generation, store):
    """CURRENT is the one live pointer. Store entries are fixed absolute links
    through it and only accumulate, so after the first adoption a switch is
    exactly one rename; every entry resolves within one complete generation."""
    previous=current_generation()
    if previous is None:
        previous=adopt_legacy(store);link(ROOT/'current',previous)
        retired=ROOT/'retired';retired.mkdir(exist_ok=True,mode=0o755)
        for entry in list(Path(store).iterdir()):
            if not entry.is_symlink():os.rename(entry,retired/(entry.name+'.'+previous.name))
    for root in [previous,generation]:
        for name in sorted(p.name for p in Path(root).iterdir() if not p.name.startswith('.')):
            entry=Path(store)/name;target=ROOT/'current'/name
            if not (entry.is_symlink() and os.readlink(entry)==str(target)):link(entry,target)
    if previous!=generation:link(ROOT/'previous',previous)
    link(ROOT/'current',generation)
    return previous


@contextlib.contextmanager
def admission_drained(generation):
    """No ticket handshake proceeds and no admitted job or daemon is left."""
    Path('/mnt/bb/admit').mkdir(parents=True,exist_ok=True)
    with open('/mnt/bb/admit/upgrade.lock','a') as admission:
        fcntl.flock(admission,fcntl.LOCK_EX)
        cache_authority(generation).drain_admission()
        yield


@contextlib.contextmanager
def generation_services(generation):
    """Host services follow the generation that owns them."""
    if (Path(generation)/'bb-cache.py').exists():
        with cache_authority(generation).migration(drained=True):yield
        return
    unit=Path('/etc/systemd/system/buildbox-cache.service')
    if unit.exists():subprocess.run(['systemctl','disable','--now','buildbox-cache.service'],check=True)
    yield


CANARY_CRATE = {'Cargo.toml':b'[package]\nname = "bb-install-canary"\nversion = "0.0.0"\nedition = "2021"\n[workspace]\n',
                'src/lib.rs':b'pub fn canary() -> u8 { 1 }\n'}


def canary(generation=None):
    """Real fleet-shaped jobs through the native client path: an undeclared
    `bash -c` script and a cargo build. With `generation`, the staged
    generation is bound over the client entry in a private mount namespace;
    otherwise the live entry is used. Both get a private admission root, so
    the drained host admission lock stays held, and its daemon is retired."""
    work=Path(tempfile.mkdtemp(prefix='bb-install-canary-',dir='/mnt/bb/tmp'))
    try:
        admit=work/'admit';crate=work/'crate';admit.mkdir()
        for relative,data in CANARY_CRATE.items():
            (crate/relative).parent.mkdir(parents=True,exist_ok=True);(crate/relative).write_bytes(data)
        account=pwd.getpwnam('var')
        for path in [work,admit,*crate.rglob('*'),crate]:os.chown(path,account.pw_uid,account.pw_gid)
        run=['runuser','-u','var','--','env','-i','HOME=/home/var','USER=var','LOGNAME=var',
             'PATH=/home/var/.cargo/bin:/home/var/bin:/usr/local/bin:/usr/bin:/bin',
             'BUILD_SLOT_LABEL=bb-install-canary','/home/var/bin/build-slot']
        steps=['set -e','mount --bind '+shlex.quote(str(admit))+' /mnt/bb/admit']
        if generation is not None:
            steps.append('mount -o bind,ro '+shlex.quote(str(generation))+' /home/var/bin')
        # The staged generation's cache authority is not yet the host service.
        if generation is not None:run.append('--cold')
        steps+=['cd '+shlex.quote(str(crate)),
                shlex.join([*run,'bash','-c','echo BB_CANARY_SCRIPT_OK']),
                shlex.join([*run,'cargo','build','--offline','--quiet'])+' && echo BB_CANARY_CARGO_OK']
        result=subprocess.run(['unshare','--mount','--propagation','private','sh','-c','\n'.join(steps)],
                              capture_output=True,text=True,timeout=CANARY_TIMEOUT)
        try:
            if (admit/'admit.sock').exists():cache_authority(generation or ROOT/'current').drain_admission(admit)
        finally:
            output=result.stdout+result.stderr
            if result.returncode or 'BB_CANARY_SCRIPT_OK' not in result.stdout or 'BB_CANARY_CARGO_OK' not in result.stdout:
                raise RuntimeError('kind=GENERATION_CANARY stage='+('staged' if generation else 'live')+
                                   ' rc='+str(result.returncode)+' output='+shlex.quote(output[-2000:]))
    finally:
        subprocess.run(['rm','-rf','--one-file-system',str(work)],check=False)


def deploy(generation, store):
    """Canary the staged generation, switch it in by one rename, canary the
    live entry, and switch back to the retained predecessor on failure.
    Admission stays drained throughout, so no lane job sees a failing
    or mixed generation."""
    with admission_drained(generation):
        canary(generation)
        before=current_generation();previous=None
        try:
            with generation_services(generation):previous=switch(generation,store)
            canary()
        except BaseException:
            restore=previous or before or current_generation()
            if restore is not None and current_generation()!=restore:
                with generation_services(restore):link(ROOT/'current',restore)
                link(ROOT/'previous',generation)
            raise
    return previous


def publish_candidate(candidate):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo')
    stage=candidate_directory(candidate)
    token=stage/'.qualified.json'; fence=stage/'.predecessors.json'
    for file in [stage, token, fence, *[stage/name for name in TOOLS]]:
        if file.is_symlink() or file.stat().st_uid != 0 or file.stat().st_mode & 0o022:
            raise PermissionError('candidate custody is unproven: '+str(file))
    hashes=json.loads(token.read_text())
    payload={name:(stage/name).read_bytes() for name in TOOLS}
    if hashes != {name:hashlib.sha256(data).hexdigest() for name,data in payload.items()}:
        raise ValueError('qualified candidate changed')
    if hashes['bb-install.py'] != hashlib.sha256(Path(__file__).read_bytes()).hexdigest():
        raise ValueError('use the qualified candidate installer to publish this generation')
    # Kernel file lock serializes installers, not jobs. Qualification happens
    # before the coordinator's idle gate lock; only publication takes that lock.
    with open('/mnt/bb/locks/bb-install.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous=json.loads(fence.read_text())
        if (not isinstance(previous,dict) or set(previous)!=set(TOOLS)
                or any(value is not None and (not isinstance(value,str) or len(value)!=64
                       or any(c not in '0123456789abcdef' for c in value)) for value in previous.values())):
            raise ValueError('invalid installed predecessor record')
        if any(file_hash(BIN/name) != previous.get(name) for name in TOOLS):
            raise ValueError('installed predecessor changed; re-qualify against current tools')
        BIN.mkdir(exist_ok=True)
        store=secure_bin()
        files={**carried(current_generation() or store),**generation_payload(payload,hashes)}
        name=hashlib.sha256(json.dumps(sorted((k,hashlib.sha256(v).hexdigest()) for k,v in files.items())).encode()).hexdigest()[:16]
        generation=materialize(files,name)
        deploy(generation,store)
    return {'generation':str(generation),'sha256':hashes}


def activate(generation):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo')
    generation=Path(generation)
    if generation.parent!=ROOT/'generations' or generation.is_symlink() or not (generation/'installed-tools.json').is_file():
        raise ValueError('not a retained generation: '+str(generation))
    with open('/mnt/bb/locks/bb-install.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        deploy(generation,secure_bin())
    return generation


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source');group.add_argument('--publish')
    # Rollback: re-activate a retained generation through the same canaries.
    group.add_argument('--activate')
    args = parser.parse_args()
    if args.source:
        try:print(prepare(args.source))
        except (OSError,ValueError,subprocess.CalledProcessError) as error:
            recovery=shlex.join(['sudo','-n','/usr/bin/python3','-B',str(Path(args.source).resolve()/'bb-install.py'),
                                 '--source',args.source])
            print('kind=INSTALL_INFRA step=qualify-source detail='+shlex.quote(str(error))+
                  ' recovery='+shlex.quote(recovery),file=sys.stderr)
            raise SystemExit(224)
    elif args.activate:print(json.dumps({'activated':str(activate(args.activate))}),file=sys.stderr)
    else:print(json.dumps(publish_candidate(args.publish)),file=__import__('sys').stderr)
