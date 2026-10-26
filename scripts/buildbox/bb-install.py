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
# Dependencies precede consumers. Every replacement remains a complete file;
# in-flight interpreters retain their old inode and loaded module generation.
TOOLS = ['admission_policy.py', 'input_closure.py', 'job_caps.py', 'cpu_leases.py', 'bbcgroup-systemd-run', 'bb-cache.py', 'sccache-rustc', 'job_exports.py', 'bb_target_store.py', 'bb_cargo_cache.py',
         'prune-wt-sdk.py', 'buildbox-receipt.mjs', 'bb-job.py', 'bb-admit.py', 'bb-admit', 'build-slot', 'bb-install.py']
CACHE_TOOLS = frozenset({'admission_policy.py','input_closure.py','job_caps.py','cpu_leases.py','bbcgroup-systemd-run','bb-cache.py','sccache-rustc','bb-job.py','bb-admit.py','build-slot'})


def migration_scope(stage, selected):
    if not CACHE_TOOLS.intersection(selected):return contextlib.nullcontext()
    if not CACHE_TOOLS.issubset(selected):
        recovery=shlex.join(['sudo','-n','/usr/bin/python3','-B',str(Path(stage)/'bb-install.py'),
                             '--publish',str(stage),'--only',*sorted(CACHE_TOOLS)])
        raise ValueError('kind=CACHE_POLICY step=cache-generation-publication owner=gate-speed '
                         'recovery='+shlex.quote(recovery))
    spec=importlib.util.spec_from_file_location('qualified_cache_migration',Path(stage)/'bb-cache.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module.migration()


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


def select_tools(only):
    selected = list(TOOLS) if only is None else list(only)
    if not selected or len(set(selected)) != len(selected) or any(name not in TOOLS for name in selected):
        raise ValueError('publication selection must contain distinct qualified tools')
    return selected


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


def publish_candidate(candidate, only=None):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo')
    stage=candidate_directory(candidate)
    token=stage/'.qualified.json'; fence=stage/'.predecessors.json'
    selected=select_tools(only)
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
    import fcntl
    with open('/mnt/bb/locks/bb-install.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        previous=json.loads(fence.read_text())
        if (not isinstance(previous,dict) or set(previous)!=set(TOOLS)
                or any(value is not None and (not isinstance(value,str) or len(value)!=64
                       or any(c not in '0123456789abcdef' for c in value)) for value in previous.values())):
            raise ValueError('invalid installed predecessor record')
        if any(file_hash(BIN/name) != previous.get(name) for name in selected):
            raise ValueError('installed predecessor changed; re-qualify against current tools')
        BIN.mkdir(exist_ok=True)
        store=secure_bin()
        with migration_scope(stage,selected):
            for name in selected:
                data=payload[name]
                destination=store/name; backup=store/(name+'.bak-gatespeed')
                if destination.exists() and not backup.exists():publish(backup,destination.read_bytes())
                publish(destination,data)
            if 'bbcgroup-systemd-run' in selected:
                directory=store/'bbcg';directory.mkdir(exist_ok=True)
                entry=directory/'systemd-run';backup=directory/'systemd-run.bak-gatespeed'
                if entry.exists() and not backup.exists():publish(backup,entry.read_bytes())
                publish(entry,payload['bbcgroup-systemd-run'])
        # Existing launchers also cross this privilege boundary; var can
        # execute, but cannot truncate, unlink or replace a tool directly.
        for directory, dirs, files in os.walk(store, followlinks=False):
            os.chown(directory, 0, 0); Path(directory).chmod(0o755)
            for name in files:
                file=Path(directory)/name
                if not file.is_symlink():os.chown(file,0,0);file.chmod(0o555)
        installed={name:file_hash(store/name) for name in TOOLS}
        publish(store/'installed-tools.json',json.dumps({'schema_version':1,'sha256':installed}).encode(),0o444)
    return {name:hashes[name] for name in selected}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source');group.add_argument('--publish')
    parser.add_argument('--only',nargs='+',choices=TOOLS)
    args = parser.parse_args()
    if args.source:
        if args.only:parser.error('--only requires --publish')
        try:print(prepare(args.source))
        except (OSError,ValueError,subprocess.CalledProcessError) as error:
            recovery=shlex.join(['sudo','-n','/usr/bin/python3','-B',str(Path(args.source).resolve()/'bb-install.py'),
                                 '--source',args.source])
            print('kind=INSTALL_INFRA step=qualify-source detail='+shlex.quote(str(error))+
                  ' recovery='+shlex.quote(recovery),file=sys.stderr)
            raise SystemExit(224)
    else:print(json.dumps(publish_candidate(args.publish,args.only)),file=__import__('sys').stderr)
