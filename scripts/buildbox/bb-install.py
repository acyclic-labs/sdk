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
    # Kernel file lock serializes installers, not jobs. Qualification happens
    # before the coordinator's idle gate lock; only publication takes that lock.
    import fcntl
    with open('/mnt/bb/locks/bb-install.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        BIN.mkdir(exist_ok=True)
        os.chown(BIN, 0, 0); BIN.chmod(0o755)
        for name, data in payload.items():
            destination=BIN/name; backup=BIN/(name+'.bak-gatespeed')
            if destination.exists() and not backup.exists():publish(backup,destination.read_bytes())
            publish(destination,data)
        # Existing launchers also cross this privilege boundary; var can
        # execute, but cannot truncate, unlink or replace a tool directly.
        for directory, dirs, files in os.walk(BIN, followlinks=False):
            os.chown(directory, 0, 0); Path(directory).chmod(0o755)
            for name in files:
                file=Path(directory)/name
                if not file.is_symlink():os.chown(file,0,0);file.chmod(0o555)
        publish(BIN/'installed-tools.json',json.dumps({'schema_version':1,'sha256':hashes}).encode(),0o444)
    return hashes


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--source');group.add_argument('--publish')
    args = parser.parse_args()
    if args.source:print(prepare(args.source))
    else:print(json.dumps(publish_candidate(args.publish)),file=__import__('sys').stderr)
