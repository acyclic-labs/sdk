#!/usr/bin/env python3
"""The privileged, test-gated publication boundary for bb2 tools.

Normal users cannot modify the root-owned bin directory or published files.
Install with sudo bb-install --source <reviewed scripts/buildbox directory>.
The fixed test policy runs as var against a frozen candidate before any rename.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import pwd
import subprocess
import sys
import tempfile

BIN = Path('/home/var/bin')
NODE = '/home/var/tools/node24/node-v24.3.0-linux-x64/bin/node'
# Dependencies precede consumers. Every replacement remains a complete file;
# in-flight interpreters retain their old inode and loaded module generation.
TOOLS = ['admission_policy.py', 'job_exports.py', 'bb_target_store.py', 'bb_cargo_cache.py',
         'prune-wt-sdk.py', 'buildbox-receipt.mjs', 'bb-job.py', 'bb-admit.py', 'bb-admit', 'build-slot', 'bb-install.py']


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
                ['python3', 'runtime.test.py'], ['python3', 'admission.test.py'],
                ['python3', 'install.test.py']]
    for command in commands:
        # The tested code cannot modify its frozen source or installed tools.
        subprocess.run(['runuser', '-u', user, '--', *command], cwd=source, check=True, stdout=sys.stderr)


def prepare(source):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo; direct tool edits are refused')
    account = pwd.getpwnam('var')
    stage = Path(tempfile.mkdtemp(prefix='bb-install-', dir='/mnt/bb/tmp')); stage.chmod(0o755)
    try:
        # Copy only regular source files. No symlinks or mutable shared test
        # dependencies enter the qualification generation.
        for item in Path(source).rglob('*'):
            if '__pycache__' in item.parts: continue
            if item.is_symlink(): raise ValueError('tool source symlink refused: ' + str(item))
            if item.is_file() and not item.name.endswith('.bak-gatespeed'):
                destination=stage/item.relative_to(source)
                destination.parent.mkdir(parents=True, exist_ok=True)
                publish(destination, item.read_bytes(), 0o444)
        hashes = {name: hashlib.sha256((stage/name).read_bytes()).hexdigest() for name in TOOLS}
        qualify(stage, account.pw_name)
        publish(stage/'.qualified.json', json.dumps(hashes).encode(), 0o400)
        return stage
    except BaseException:
        # A failed candidate remains evidence but has no qualification token.
        raise


def publish_candidate(candidate):
    if os.geteuid() != 0: raise PermissionError('bb-install requires sudo')
    stage=Path(candidate)
    if stage.parent != Path('/mnt/bb/tmp') or not stage.name.startswith('bb-install-') or stage.is_symlink():
        raise ValueError('not an installer candidate')
    token=stage/'.qualified.json'
    for file in [stage, token, *[stage/name for name in TOOLS]]:
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
        BIN.mkdir(exist_ok=True)
        store=secure_bin()
        for name, data in payload.items():
            destination=store/name; backup=store/(name+'.bak-gatespeed')
            if destination.exists() and not backup.exists():publish(backup,destination.read_bytes())
            publish(destination,data)
        # Existing launchers also cross this privilege boundary; var can
        # execute, but cannot truncate, unlink or replace a tool directly.
        for directory, dirs, files in os.walk(store, followlinks=False):
            os.chown(directory, 0, 0); Path(directory).chmod(0o755)
            for name in files:
                file=Path(directory)/name
                if not file.is_symlink():os.chown(file,0,0);file.chmod(0o555)
        publish(store/'installed-tools.json',json.dumps({'schema_version':1,'sha256':hashes}).encode(),0o444)
    return hashes


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source');group.add_argument('--publish')
    args = parser.parse_args()
    if args.source:print(prepare(args.source))
    else:print(json.dumps(publish_candidate(args.publish)),file=__import__('sys').stderr)
