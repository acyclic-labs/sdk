#!/usr/bin/env python3
"""Run a cross-check with job-owned SDK snapshots, one per matrix worker."""
import argparse
from contextlib import contextmanager
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess
import sys
import tempfile


def compiler_path():
    path = Path(os.environ.get('SDK_MSVC_COMPILER', '/home/var/bin/msvc-compiler.py'))
    if not path.is_file():
        recovery = shlex.join([sys.executable, *sys.argv])
        raise RuntimeError('STEP_FAILED msvc-compiler: installed shim missing at ' + str(path)
                           + '; recovery: SDK_MSVC_COMPILER=/absolute/path/to/msvc-compiler.py ' + recovery)
    return path


def sdk_identity(root):
    # Include ctime/inode: restoring an old mtime cannot hide an SDK write.
    rows=[]
    for file in [root, *sorted(root.rglob('*'))]:
        stat=file.lstat()
        rows.append([str(file.relative_to(root)), stat.st_dev, stat.st_ino,
                     stat.st_mode, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns,
                     os.readlink(file) if file.is_symlink() else None])
    return hashlib.sha256(json.dumps(rows,separators=(',',':')).encode()).hexdigest()


def snapshot(seed, destination):
    seed.mkdir(parents=True, exist_ok=True)
    destination.mkdir()
    sdk = destination / "xwin"
    sdk.mkdir()
    # Same preparation/snapshot custody as the qualified Windows test helper.
    with (seed / ".win-test.lock").open("a") as lease:
        fcntl.flock(lease, fcntl.LOCK_EX)
        subprocess.run(["cargo", "xwin", "cache", "xwin"],
                       env=dict(os.environ, XWIN_CACHE_DIR=str(seed), XWIN_ACCEPT_LICENSE="1"), check=True, stdout=sys.stderr)
        identity=sdk_identity(seed/'xwin')
        subprocess.run(["cp", "-a", "--reflink=auto", "--", str(seed / "xwin" / "crt"),
                        str(seed / "xwin" / "sdk"), str(seed / "xwin" / "DONE"), str(sdk)], check=True)
        for entry in sdk.rglob("*"):
            if entry.is_symlink() and not entry.resolve(strict=True).is_relative_to(sdk):
                raise RuntimeError("SDK alias escapes private snapshot: " + str(entry))
        if sdk_identity(seed/'xwin')!=identity:
            raise RuntimeError('GATE_RUNNER_INFRA Windows SDK changed during snapshot')
        (destination/'sdk-identity.sha256').write_text(identity)


def populate(root, workers):
    for index in range(workers):
        snapshot(Path("/mnt/bb/toolchains/win-test-xwin-v1"), root / f"worker-{index}")
    compilers = root / "bin"
    compilers.mkdir()
    driver = compilers / "clang-cl"
    shutil.copyfile(compiler_path(), driver)
    driver.chmod(0o755)


def workspace_identity(target, workers):
    return {'target': str(Path(target).resolve()), 'workers': workers,
            'compiler_sha256': hashlib.sha256(compiler_path().read_bytes()).hexdigest()}


def workspace_payload_complete(root, workers, identity):
    try:
        driver = root / 'bin' / 'clang-cl'
        if (driver.is_symlink() or (root / 'bin').is_symlink()
                or hashlib.sha256(driver.read_bytes()).hexdigest() != identity['compiler_sha256']):
            return False
        for index in range(workers):
            worker = root / f'worker-{index}'
            sdk = worker / 'xwin'
            if any(p.is_symlink() or not p.is_dir() for p in [worker, sdk, sdk/'crt', sdk/'sdk']):
                return False
            if (sdk/'DONE').is_symlink() or not (sdk/'DONE').is_file():
                return False
            identity_file=worker/'sdk-identity.sha256'
            value=identity_file.read_text()
            if identity_file.is_symlink() or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):
                return False
        return True
    except OSError:
        return False


def prepare_workspace(root, workers, identity):
    marker = root / 'complete.json'
    if root.exists() or root.is_symlink():
        try:
            complete = (not root.is_symlink() and not marker.is_symlink()
                        and json.loads(marker.read_text()) == identity
                        and workspace_payload_complete(root, workers, identity))
        except (OSError, ValueError):
            complete = False
        if not complete:
            raise RuntimeError('GATE_RUNNER_INFRA incomplete private Windows SDK workspace')
        return
    root.mkdir(mode=0o700)
    populate(root, workers)
    if not workspace_payload_complete(root, workers, identity):
        raise RuntimeError('GATE_RUNNER_INFRA incomplete private Windows SDK payload')
    # A killed initializer cannot leave a completion marker. Never repair or
    # delete a possibly live SDK; the job runtime owns final retirement.
    staged = root / 'complete.tmp'
    with staged.open('w') as output:
        json.dump(identity, output); output.flush(); os.fsync(output.fileno())
    staged.replace(marker)


@contextmanager
def workspace(workers, env=None):
    env = os.environ if env is None else env
    helper = Path(__file__).with_name('gate-target-cache.py')
    target = env.get('GATE_FEATURE_TARGET_DIR')
    gate = None
    if helper.is_file() and target:
        spec = importlib.util.spec_from_file_location('gate_xwin_runtime', helper)
        gate = importlib.util.module_from_spec(spec)
        # Import the reviewed helper without generating files in its uploaded
        # source directory after the immutable input clock was recorded.
        bytecode = sys.dont_write_bytecode
        try:
            sys.dont_write_bytecode = True
            spec.loader.exec_module(gate)
        finally:
            sys.dont_write_bytecode = bytecode
        if gate.private_inputs(env, [target]) is None:
            gate = None
    if gate is None:
        with tempfile.TemporaryDirectory(prefix='gate-xwin-') as temporary:
            root = Path(temporary); populate(root, workers); yield root
        return
    # Stable virtual SDK and compiler paths are build inputs, too. Random
    # temporary names invalidate the full request key and Cargo's link flags.
    # This root is private to the job; its runtime lease owns final retirement.
    identity = workspace_identity(target, workers)
    root = Path('/work/target') / ('.gate-xwin-' + gate.cache.store.digest(identity))
    job = Path(env['BB_JOB_HOME']).parent
    with gate.cache.local_target_leases(job, root):
        prepare_workspace(root, workers, identity)
    yield root


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command[1:] if args.command[:1] == ["--"] else args.command
    if args.workers < 1 or not command:
        parser.error("positive worker count and command required")
    with workspace(args.workers) as root:
        compilers = root / 'bin'
        env = dict(os.environ, GATE_XWIN_CACHE_ROOT=str(root),
                   XWIN_CACHE_DIR=str(root / "worker-0"), XWIN_ACCEPT_LICENSE="1",
                   GATE_XWIN_SDK_SHA256=hashlib.sha256(json.dumps([
                       (root/f'worker-{index}'/'sdk-identity.sha256').read_text()
                       for index in range(args.workers)]).encode()).hexdigest(),
                   PATH=str(compilers) + os.pathsep + os.environ["PATH"])
        return subprocess.run(command, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
