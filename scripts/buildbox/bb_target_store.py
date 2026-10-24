#!/usr/bin/env python3
"""Codex: completed Cargo target snapshots; never cache test verdicts.

The v1 request key is deliberately stricter than Cargo's build-unit key. Cargo
still validates every fingerprint, and every requested test command executes.
Only /work/target in a proven private buildbox mount is eligible.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import time

# Earlier snapshots did not exclude writers in nested target directories.
# A new key generation prevents any such snapshot from being reused.
VERSION = 'sdk-target-reflink-v3-source-clock'
ROOT = Path('/mnt/bb/sdk-target-store-v1')
BUDGET = 64 * 1024**3  # Logical upper bound, including shared extents.
SCAFFOLD = {'.host-targets', '.host-src', '.gate.lock', '.cargo-lock',
            '.gate-cache-boot', '.gate-cache-inflight'}


def require_data_path(path):
    """Check the effective filesystem, including bind mounts and symlinks."""
    path = Path(path).resolve()
    ancestor = path
    while not ancestor.exists():
        ancestor = ancestor.parent
    device = ancestor.stat().st_dev
    if device != Path('/mnt/bb').stat().st_dev or device == Path('/').stat().st_dev:
        raise RuntimeError('target/tmp must reside on /mnt/bb filesystem: ' + str(path))
    return {'path': str(path), 'device': device}


def require_job_target(path, env):
    """A data-disk path is not permission to write another job's targets."""
    reading = require_data_path(path)
    path = Path(reading['path'])
    home = Path(env.get('BB_JOB_HOME', '')).absolute()
    job = home.parent
    if (not env.get('BB_JOB_HOME') or home.name != 'home'
            or job.parent != Path('/mnt/bb/jobs') or job.is_symlink()):
        raise RuntimeError('Cargo target requires current job custody')
    if path.is_relative_to(job.resolve()):
        return reading
    file = env.get('BUILD_SLOT_RECEIPT_META')
    if (path.is_relative_to(Path('/work/target')) and file
            and private_target(json.loads(Path(file).read_text()))):
        return reading
    raise RuntimeError('Cargo target is outside the current job: ' + str(path))


def children(root):
    return [p for p in Path(root).iterdir() if p.name not in SCAFFOLD]


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def private_target(meta, target=Path('/work/target')):
    """A directory's existence is not proof of private mount custody."""
    prefix = meta.get('job_prefix', '')
    if not prefix or Path(prefix).parent != Path('/mnt/bb/jobs'):
        return False
    host = Path(prefix + '.targets')
    try:
        if host.is_symlink() or target.is_symlink() or not os.path.samefile(host, target):
            return False
        mounts = Path('/proc/self/mountinfo').read_text().splitlines()
        return any(line.split()[4] == str(target) for line in mounts)
    except OSError:
        return False


def tool_identity():
    """Metadata only: no config, login, auth or credential file bodies."""
    files = list(Path('/home/var/.rustup/toolchains').glob('*/bin/rustc'))
    files += list(Path('/home/var/.rustup/toolchains').glob('*/bin/cargo'))
    files += list(Path('/home/var/.rustup/toolchains').glob('*/bin/rustdoc'))
    files += [Path(p) for p in ['/usr/bin/cc', '/usr/bin/ld', '/usr/bin/ld.lld',
              '/usr/bin/clang', '/usr/bin/mold', '/home/var/bin/sccache-rustc',
              '/home/var/bin/bigstack/rustc', '/home/var/.cargo/config.toml', '/mnt/bb/toolchains/cargo/config.toml',
              '/home/var/bin/build-slot']]
    result = []
    for file in sorted(files):
        try:
            real = file.resolve(); s = real.stat()
            result.append([str(file), str(real), s.st_dev, s.st_ino, s.st_size, s.st_mtime_ns])
        except OSError:
            result.append([str(file), 'absent'])
    # Toolchain channel selection may change without changing installed binaries.
    for name in ['settings.toml']:
        file = Path('/home/var/.rustup') / name
        if file.exists():
            s = file.stat(); result.append([str(file), s.st_size, s.st_mtime_ns])
    return result


def source_unchanged(meta, env, root=Path('/work/src')):
    """A completed compile must still describe the initially uploaded inputs.

    Restored past timestamps are safe only for that exact immutable source.
    Build scripts that alter inputs cannot publish under the original key.
    ctime catches a write that restores its old mtime; external links bypass.
    """
    try:
        clock = json.loads(Path(env['BB_SOURCE_TIMES_FILE']).read_text())
        if (not clock['cacheable'] or clock['root'] != str(root) or not clock['files']
                or clock['content_sha256'] != meta['source']['content_sha256']):
            return False
        for name, *record in clock['files']:
            expected, content = record[:-1], record[-1]
            file = Path(clock['root']) / name
            if not file.resolve().is_relative_to(root):
                return False
            s = file.stat()
            if expected != list(map(str, [s.st_mode, s.st_size, s.st_mtime_ns, s.st_ctime_ns])):
                return False
            if hashlib.sha256(file.read_bytes()).hexdigest() != content:
                return False
        return True
    except (OSError, ValueError, KeyError, TypeError):
        return False


def request_key(meta, env, job, components=None):
    source = meta.get('source') or {}
    tree = source.get('content_sha256')
    payload = env.get('BB_TARGET_REQUEST_SHA256')
    if (meta.get('repo') != 'sdk' or not tree or not meta.get('argv') or not payload
            or env.get('BB_SOURCE_TIMES_POLICY') != 'blob-past-private-v1'):
        return None
    # Operational identities vary for every dispatch. Build flags, PATH, custom
    # variables and compiler selection remain in the key; only digests persist.
    ignored = {'PWD', 'OLDPWD', 'SHLVL', '_', 'CARGO_BUILD_JOBS', 'SYSTEMD_EXEC_PID',
               'INVOCATION_ID', 'JOURNAL_STREAM', 'SSH_CLIENT', 'SSH_CONNECTION',
               'SSH_TTY', 'SUDO_COMMAND', 'SUDO_UID', 'SUDO_GID', 'SUDO_USER'}
    selected = {}
    for name, value in env.items():
        if name in ignored or name.startswith(('BUILD_SLOT_', 'BB_', 'SCCACHE_')):
            continue
        selected[name] = value.replace(str(job), '$JOB')
    inputs = {'version': VERSION, 'source': source, 'argv': meta['argv'],
              'cwd': str(Path.cwd()), 'environment': digest(selected),
              'tools': tool_identity(), 'target': env.get('CARGO_TARGET_DIR')}
    if components is not None:
        # Diagnose missed reuse without persisting argument/environment bodies.
        # This is observational: the exact original key payload is unchanged.
        components.update({name: digest(value) for name, value in inputs.items()})
    return digest(inputs)


def inspect_tree(root):
    """Reject special files and links that can escape a private clone."""
    total = count = 0
    root = root.resolve()
    for where, dirs, files in os.walk(root, followlinks=False):
        if Path(where) == root:
            dirs[:] = [d for d in dirs if d not in SCAFFOLD]
            files = [f for f in files if f not in SCAFFOLD]
        for name in dirs + files:
            file = Path(where) / name
            s = file.lstat()
            if stat.S_ISLNK(s.st_mode):
                link = os.readlink(file)
                if os.path.isabs(link) or not file.resolve().is_relative_to(root):
                    raise ValueError('target contains an escaping symlink')
            elif stat.S_ISREG(s.st_mode):
                total += s.st_size; count += 1
            elif not stat.S_ISDIR(s.st_mode):
                raise ValueError('target contains a special file')
    return total, count


def permissions(root, writable):
    if root.is_file() and not root.is_symlink():
        mode = stat.S_IMODE(root.stat().st_mode)
        root.chmod(mode | stat.S_IWUSR if writable else mode & ~0o222)
        return
    for where, dirs, files in os.walk(root, followlinks=False):
        for file in [Path(where)] + [Path(where) / x for x in dirs + files]:
            if file.is_symlink():
                continue
            mode = stat.S_IMODE(file.stat().st_mode)
            file.chmod(mode | stat.S_IWUSR if writable else mode & ~0o222)


def remove_tree(root):
    if root.exists():
        permissions(root, True)
        shutil.rmtree(root)


def clone(source, destination):
    items = children(source)
    if items:
        subprocess.run(['cp', '-a', '--reflink=always', '--'] + [str(p) for p in items] + [str(destination)],
                       check=True, capture_output=True)
        for item in items:
            permissions(destination / item.name, True)


class Session:
    def __init__(self, root, key, target):
        self.root, self.key, self.target = Path(root), key, Path(target)
        self.entry = self.root / 'entries' / key
        self.lock = None
        self.receipt = {'version': VERSION, 'key': key, 'status': 'preparing'}

    def open(self):
        for name in ['entries', 'locks', 'staging']:
            (self.root / name).mkdir(parents=True, exist_ok=True)
        self.lock = (self.root / 'locks' / self.key).open('a')
        started = time.monotonic()
        fcntl.flock(self.lock, fcntl.LOCK_EX)
        self.receipt['wait_seconds'] = time.monotonic() - started
        # Every stage is created and removed under the catalog lease. Holding
        # it proves all existing stages abandoned, regardless of their key;
        # cleanup cannot depend on the crashed source/argv being used again.
        with (self.root / 'catalog.lock').open('a') as catalog:
            fcntl.flock(catalog, fcntl.LOCK_EX)
            for abandoned in (self.root / 'staging').iterdir():
                remove_tree(abandoned)
        started = time.monotonic()
        marker = self.entry / 'complete.json'
        if marker.is_file():
            manifest = json.loads(marker.read_text())
            if manifest['version'] != VERSION or manifest['key'] != self.key:
                raise ValueError('target manifest identity mismatch')
            clone(self.entry / 'target', self.target)
            self.receipt.update(status='hit', logical_bytes=manifest['logical_bytes'],
                                files=manifest['files'])
            os.utime(self.entry, None)
        else:
            self.receipt['status'] = 'miss'
        self.receipt['clone_seconds'] = time.monotonic() - started
        # Keep the request lease through execution. Even after a warm seed,
        # Cargo may invalidate a unit: an identical requester must not build it
        # concurrently. Tests execute after acquiring their own private clone.
        return self

    def publish(self, budget=BUDGET):
        if self.entry.exists():
            return
        started = time.monotonic()
        size, files = inspect_tree(self.target)
        self.receipt.update(logical_bytes=size, files=files)
        if not files or size > budget:
            self.receipt['publish'] = 'empty' if not files else 'over-budget'
            return
        # One short catalog lease for eviction and publication, never execution.
        with (self.root / 'catalog.lock').open('a') as catalog:
            fcntl.flock(catalog, fcntl.LOCK_EX)
            entries = []
            for entry in (self.root / 'entries').iterdir():
                manifest = json.loads((entry / 'complete.json').read_text())
                entries.append((entry.stat().st_mtime_ns, entry, manifest['logical_bytes']))
            occupied = sum(x[2] for x in entries)
            for _, entry, amount in sorted(entries):
                if occupied + size <= budget:
                    break
                with (self.root / 'locks' / entry.name).open('a') as lock:
                    try:
                        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    except BlockingIOError:
                        continue
                    remove_tree(entry); occupied -= amount
            if occupied + size > budget:
                self.receipt['publish'] = 'busy-budget'
                return
            stage = Path(tempfile.mkdtemp(prefix=self.key + '-', dir=self.root / 'staging'))
            try:
                (stage / 'target').mkdir()
                clone(self.target, stage / 'target')
                manifest = {'version': VERSION, 'key': self.key, 'logical_bytes': size,
                            'files': files, 'completed_at': time.time()}
                (stage / 'complete.json').write_text(json.dumps(manifest) + '\n')
                permissions(stage, False)
                # Moving a directory between parents needs permission to
                # rewrite its '..' entry. Freeze its root after the rename,
                # while both publication and request leases still exclude it.
                stage.chmod(0o700)
                os.replace(stage, self.entry)
                self.entry.chmod(0o500)
            finally:
                remove_tree(stage)
            self.receipt['publish'] = 'completed'
        self.receipt['publish_seconds'] = time.monotonic() - started

    def close(self):
        if self.lock:
            self.lock.close(); self.lock = None


def disabled(env):
    return (env.get('BB_CACHE_DISABLED') == '1'
            or env.get('BB_TARGET_STORE_DISABLED') == '1'
            or env.get('RUSTC_WRAPPER') == '')


def prepare(meta, env, job):
    if disabled(env) or not private_target(meta):
        return None
    key = request_key(meta, env, job)
    if key is None:
        return None
    session = Session(ROOT, key, Path('/work/target'))
    try:
        return session.open()
    except BaseException:
        session.close()
        raise
