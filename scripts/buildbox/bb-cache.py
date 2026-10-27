#!/usr/bin/python3 -B
"""Host cache authority: kernel peer/birth validation, never client lazy startup.

Only `recover` starts the root-owned authority in the host systemd namespace.
It drains admission and birth-pinned legacy servers before switching clients.
Jobs and compiler wrappers can only request validation from its protected socket.
"""
import fcntl
import ctypes
import array
import errno
import contextlib
import json
import os
from pathlib import Path
import pwd
import select
import selectors
import signal
import socket
import struct
import subprocess
import sys
from admission_policy import (CACHE_ROOT, CACHE_ENDPOINT, SCCACHE_DIRECTORY,
                              SCCACHE_CACHE_BYTES, CachePolicyError, cache_environment)
from admission_policy import process_identity as birth
from admission_policy import cpu_host_set, wait_process

SCCACHE = '/home/var/.cargo/bin/sccache'
CONTROL = CACHE_ROOT / 'control.sock'


def require_host_namespace():
    # The installed policy is immutable. Isolated controls relocate its root;
    # production must be supervised by the host init, never a job's PID 1.
    if CACHE_ROOT == Path('/run/buildbox-cache') and Path('/proc/1/comm').read_text().strip() != 'systemd':
        raise CachePolicyError(detail='host cache authority cannot run in a job PID namespace')


def peer(connection):
    return struct.unpack('3i', connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))


def process_env(pid):
    return dict(item.decode().split('=', 1) for item in Path(f'/proc/{pid}/environ').read_bytes().split(b'\0') if b'=' in item)


def validate_server(endpoint=CACHE_ENDPOINT, expected=None):
    """Executed in the host namespace, not against namespace-local numeric PIDs."""
    with socket.socket(socket.AF_UNIX) as connection:
        connection.connect(str(endpoint))
        pid, uid, _ = peer(connection)
        identity = birth(pid)
        if not identity or uid != pwd.getpwnam('var').pw_uid:
            raise CachePolicyError(endpoint=endpoint, detail='unobservable or wrong server peer')
        descriptor = os.pidfd_open(pid)
        try:
            if birth(pid) != identity: raise CachePolicyError(endpoint=endpoint, detail='peer birth changed')
            observed = process_env(pid)
            if Path(f'/proc/{pid}/exe').resolve() != Path(SCCACHE).resolve():
                raise CachePolicyError(endpoint=endpoint, detail='peer executable mismatch')
            canonical = cache_environment({}, endpoint, server=True)
            if any(observed.get(key) != value for key, value in canonical.items()):
                raise CachePolicyError(observed.get('SCCACHE_CACHE_SIZE', 'unknown'), endpoint, 'server launch policy')
            if os.sched_getaffinity(pid)!=set(cpu_host_set()):
                raise CachePolicyError(endpoint=endpoint,detail='host cache CPU affinity differs from shared policy')
            record = {'pid': pid, 'identity': identity, 'endpoint': str(endpoint),
                      'pid_namespace': os.readlink(f'/proc/{pid}/ns/pid'),
                      'scope': 'host-shared', 'cache_bytes': SCCACHE_CACHE_BYTES,
                      'directory': SCCACHE_DIRECTORY,'host_cpus':list(cpu_host_set())}
            if expected is not None and record != expected:
                raise CachePolicyError(endpoint=endpoint, detail='server peer/birth differs from READY owner')
            if birth(pid) != identity or select.select([descriptor], [], [], 0)[0]:
                raise CachePolicyError(endpoint=endpoint, detail='server ended during validation')
            return record
        finally: os.close(descriptor)


def protected(path):
    for parent in [Path(path), *Path(path).parents]:
        info = parent.lstat()
        if parent.is_symlink() or info.st_uid != 0 or info.st_mode & 0o022:
            raise CachePolicyError(detail='unprotected cache authority path: '+str(parent))


def admitted(env):
    canonical = cache_environment(env)
    try:
        protected(CACHE_ROOT)
        # One atomic request carries a private reply socket. The authority
        # never accepts or retains an idle connection or client descriptor.
        with socket.socket(socket.AF_UNIX,socket.SOCK_DGRAM) as request:
            receiver,reply_socket=socket.socketpair(socket.AF_UNIX,socket.SOCK_SEQPACKET)
            with receiver,reply_socket:
                receiver.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
                request.connect(str(CONTROL))
                request.sendmsg([b'verify'],[(socket.SOL_SOCKET,socket.SCM_RIGHTS,array.array('i',[reply_socket.fileno()]))])
                # The authority now owns the only sending endpoint. If it
                # exits before replying, recv returns EOF rather than waiting
                # forever on a client-owned datagram socket.
                reply_socket.close()
                data,ancillary,flags,_=receiver.recvmsg(16384,socket.CMSG_SPACE(12))
                if not data:raise CachePolicyError(detail='authority unavailable: ended before reply')
                credentials=[struct.unpack('3i',value) for level,kind,value in ancillary
                             if level==socket.SOL_SOCKET and kind==socket.SCM_CREDENTIALS]
                if flags & (socket.MSG_TRUNC|socket.MSG_CTRUNC) or len(credentials)!=1 or credentials[0][1]!=0:
                    raise CachePolicyError(detail='authority reply has no root kernel credentials')
                reply=json.loads(data)
        if 'error' in reply: raise CachePolicyError(detail=reply['error'])
        owner = reply['owner']
        if (owner['scope'] != 'host-shared' or owner['endpoint'] != str(CACHE_ENDPOINT)
                or owner['cache_bytes'] != SCCACHE_CACHE_BYTES or not owner['identity']):
            raise CachePolicyError(detail='authority response schema/policy mismatch')
        env.update(canonical)
        return owner
    except CachePolicyError: raise
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise CachePolicyError(detail='authority unavailable: '+str(error)) from error


def compiler(argv):
    # Validate before even the incremental bypass. No code path lazily starts
    # sccache. Its socket parent is root-owned/nonwritable after READY, so a
    # server death between validation and exec cannot create a replacement.
    env = dict(os.environ)
    admitted(env)
    for key in ['CARGO_TARGET_DIR', 'CARGO_BUILD_TARGET_DIR', 'CARGO_INCREMENTAL']: env.pop(key, None)
    command = argv if any(a.startswith(('incremental=', '-Cincremental=')) for a in argv) else [SCCACHE, *argv]
    os.execvpe(command[0], command, env)


def cache_owners():
    """Host-only migration inventory, never charged to individual clients."""
    for directory in Path('/proc').iterdir():
        if not directory.name.isdigit(): continue
        try:
            if directory.joinpath('exe').resolve() != Path(SCCACHE).resolve(): continue
            pid = int(directory.name); identity = birth(pid)
            observed = process_env(pid)
            if observed.get('SCCACHE_DIR') == SCCACHE_DIRECTORY and identity:
                yield {'pid': pid, 'identity': identity, 'environment': observed,
                       'pid_namespace': os.readlink(directory/'ns/pid')}
        except (FileNotFoundError, ProcessLookupError): continue


def verify_request(listener,owner,refusal=None):
    """Consume one bounded packet; every received descriptor closes here."""
    descriptors=[]
    try:
        data,ancillary,flags,_=listener.recvmsg(32,socket.CMSG_SPACE(12)+socket.CMSG_SPACE(4),socket.MSG_CMSG_CLOEXEC)
        credentials=[]
        for level,kind,value in ancillary:
            if level!=socket.SOL_SOCKET:continue
            if kind==socket.SCM_RIGHTS:
                received=array.array('i');received.frombytes(value[:len(value)//received.itemsize*received.itemsize])
                descriptors.extend(received)
            elif kind==socket.SCM_CREDENTIALS:credentials.append(struct.unpack('3i',value))
        if len(descriptors)!=1:return
        descriptor=descriptors.pop()
        try:response=socket.socket(fileno=descriptor)
        except OSError:
            os.close(descriptor);return
        with response:
            try:
                if (data!=b'verify' or flags & (socket.MSG_TRUNC|socket.MSG_CTRUNC)
                        or len(credentials)!=1 or credentials[0][1] not in (0,pwd.getpwnam('var').pw_uid)
                        or response.getsockopt(socket.SOL_SOCKET,socket.SO_TYPE)!=socket.SOCK_SEQPACKET):
                    raise CachePolicyError(detail='invalid atomic authority request')
                if refusal:raise CachePolicyError(detail=refusal)
                reply={'owner':validate_server(expected=owner)}
            except (OSError,RuntimeError) as error:reply={'error':str(error)}
            try:response.send(json.dumps(reply).encode(),socket.MSG_DONTWAIT|socket.MSG_NOSIGNAL)
            except OSError:pass  # The peer owns a bounded, possibly abandoned reply queue.
    finally:
        for descriptor in descriptors:os.close(descriptor)


def serve():
    if os.geteuid() != 0: raise CachePolicyError(detail='host authority requires root')
    require_host_namespace()
    cache_environment(os.environ)
    protected(CACHE_ROOT)
    with open(CACHE_ROOT/'owner.lock', 'a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        account = pwd.getpwnam('var')
        existing = list(cache_owners())
        if existing:
            raise CachePolicyError(existing[0]['environment'].get('SCCACHE_CACHE_SIZE', 'unknown'),
                                   detail='legacy cache owner has not drained')
        directory = CACHE_ENDPOINT.parent
        directory.mkdir(exist_ok=True)
        if CACHE_ENDPOINT.exists():
            # Never replace a live or unknown endpoint on startup.
            raise CachePolicyError(endpoint=CACHE_ENDPOINT, detail='existing endpoint requires recovery')
        os.chown(directory, account.pw_uid, account.pw_gid); directory.chmod(0o700)
        env = cache_environment({'PATH': '/usr/bin:/bin', 'HOME': SCCACHE_DIRECTORY,
                                 'TMPDIR': SCCACHE_DIRECTORY})
        def drop():
            os.sched_setaffinity(0,set(cpu_host_set()))
            os.setgroups(os.getgrouplist('var', account.pw_gid)); os.setgid(account.pw_gid); os.setuid(account.pw_uid)
        try:
            result = subprocess.run([SCCACHE, '--start-server'], env=env, preexec_fn=drop,
                                    capture_output=True, text=True)
            print('[buildbox] host sccache startup: '+result.stdout+result.stderr, file=sys.stderr, flush=True)
            if result.returncode: raise CachePolicyError(detail='host server startup failed')
            owner = validate_server()
            stats = subprocess.run([SCCACHE,'--show-stats','--stats-format','json'],env=env,
                                   preexec_fn=drop,capture_output=True,text=True)
            try: capacity=json.loads(stats.stdout)['max_cache_size'] if stats.returncode==0 else None
            except (ValueError,KeyError,TypeError): capacity=None
            if capacity!=SCCACHE_CACHE_BYTES:
                raise CachePolicyError(capacity if capacity is not None else 'unknown',detail='server-reported capacity')
            validate_server(expected=owner)
        finally:
            # Prevent all clients (including sccache's lazy fallback) from
            # creating/unlinking the canonical endpoint.
            os.chown(directory, 0, 0); directory.chmod(0o755)
        CONTROL.unlink(missing_ok=True)
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as listener:
            listener.bind(str(CONTROL)); CONTROL.chmod(0o666)
            listener.setsockopt(socket.SOL_SOCKET,socket.SO_PASSCRED,1)
            listener.setblocking(False)
            events = selectors.DefaultSelector(); events.register(listener, selectors.EVENT_READ)
            pidfd = os.pidfd_open(owner['pid']); events.register(pidfd, selectors.EVENT_READ)
            notify = os.environ.get('NOTIFY_SOCKET')
            if notify:
                with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as ready:
                    ready.connect('\0'+notify[1:] if notify.startswith('@') else notify)
                    ready.send(b'READY=1')
            reserve=os.open('/dev/null',os.O_RDONLY|os.O_CLOEXEC)
            try:
                while True:
                    for key, _ in events.select():
                        if key.fileobj == pidfd: raise CachePolicyError(detail='host server exited')
                        if key.fileobj == listener:
                            try:
                                verify_request(listener,owner)
                            except BlockingIOError:pass
                            except OSError as error:
                                if error.errno not in (errno.EMFILE,errno.ENFILE):raise
                                os.close(reserve);reserve=None
                                try:verify_request(listener,owner,'authority descriptor pressure')
                                except BlockingIOError:pass
                                finally:reserve=os.open('/dev/null',os.O_RDONLY|os.O_CLOEXEC)
            finally:
                if reserve is not None:os.close(reserve)
                os.close(pidfd); events.close(); CONTROL.unlink(missing_ok=True)


def wait_identity(record):
    wait_process(record['pid'],record['identity'],None)


def drain_admission(root=Path('/mnt/bb/admit')):
    """Caller holds upgrade.lock EX: pin connected clients, including queued
    tickets, then drain their exits before restarting the idle daemon.

    A lease-only scan misses queued tickets and the handoff between two jobs.
    Kernel peer credentials on duplicated client sockets cover that gap, even
    for the deployed daemon predating this cache authority.
    """
    endpoint=root/'admit.sock'
    try:
        with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as connection:
            connection.connect(str(endpoint));daemon_pid,uid,_=peer(connection)
    except (FileNotFoundError,ConnectionRefusedError):
        daemon_pid=None
    clients=[];daemon_fd=None
    try:
        if daemon_pid is not None:
            daemon_birth=birth(daemon_pid)
            if uid!=pwd.getpwnam('var').pw_uid or not daemon_birth:
                raise CachePolicyError(detail='admission peer cannot certify migration')
            daemon_fd=os.pidfd_open(daemon_pid)
            libc=ctypes.CDLL(None,use_errno=True)
            for directory in Path('/proc').iterdir():
                if not directory.name.isdigit() or int(directory.name)==daemon_pid:continue
                descriptor=None
                try:
                    arguments=(directory/'cmdline').read_bytes().split(b'\0')
                    if not any(Path(os.fsdecode(arg)).name=='bb-admit.py' for arg in arguments if arg):continue
                    pid=int(directory.name);identity=birth(pid)
                    descriptor=os.pidfd_open(pid)
                    if not identity or birth(pid)!=identity:continue
                    connected=False
                    for file in (directory/'fd').iterdir():
                        try:
                            if not os.readlink(file).startswith('socket:'):continue
                            duplicate=libc.syscall(438,descriptor,int(file.name),0)
                            if duplicate<0:
                                error=ctypes.get_errno()
                                if error in (9,3):continue
                                raise OSError(error,'pidfd_getfd admission client custody')
                            with socket.socket(fileno=duplicate) as client:
                                try:connected=peer(client)[0]==daemon_pid
                                except OSError:continue
                            if connected:break
                        except FileNotFoundError:continue
                    if connected and birth(pid)==identity:
                        clients.append(descriptor);descriptor=None
                except (FileNotFoundError,ProcessLookupError):pass
                finally:
                    if descriptor is not None:os.close(descriptor)
            for descriptor in clients:select.select([descriptor],[],[])
            if birth(daemon_pid)!=daemon_birth:
                raise CachePolicyError(detail='admission daemon changed during migration')
        for file in root.glob('lease-*.json'):
            try:wait_identity(json.loads(file.read_text()))
            except FileNotFoundError:pass
        if daemon_fd is not None and birth(daemon_pid)==daemon_birth:
            signal.pidfd_send_signal(daemon_fd,signal.SIGTERM)
            select.select([daemon_fd],[],[])
    finally:
        for descriptor in clients:os.close(descriptor)
        if daemon_fd is not None:os.close(daemon_fd)


@contextlib.contextmanager
def _admission_drained():
    with open('/mnt/bb/admit/upgrade.lock', 'a') as admission:
        fcntl.flock(admission, fcntl.LOCK_EX)
        drain_admission()
        yield


@contextlib.contextmanager
def _migration(drained=False):
    """Explicit migration; no cache deletion, numeric-PID kills or live job cutoffs.

    `drained`: the generation installer already holds upgrade.lock EX and has
    drained admission; a second lock description would deadlock against it.
    """
    if os.geteuid() != 0: raise CachePolicyError(detail='recovery requires sudo')
    require_host_namespace()
    cache_environment(os.environ)
    CACHE_ROOT.mkdir(mode=0o755, exist_ok=True); protected(CACHE_ROOT)
    with (contextlib.nullcontext() if drained else _admission_drained()):
        if Path('/etc/systemd/system/buildbox-cache.service').exists():
            subprocess.run(['systemctl', 'stop', 'buildbox-cache.service'], check=True)
        # All admitted jobs have drained. Private-namespace legacy servers
        # must end with their owning jobs; never signal a namespace-local PID.
        own_ns = os.readlink('/proc/self/ns/pid')
        for owner in cache_owners():
            pid, identity = owner['pid'], owner['identity']
            try:
                descriptor = os.pidfd_open(pid)
                try:
                    if not identity or birth(pid) != identity: continue
                    if owner['pid_namespace'] == own_ns:
                        endpoint=owner['environment'].get('SCCACHE_SERVER_UDS')
                        if not endpoint:
                            raise CachePolicyError(owner['environment'].get('SCCACHE_CACHE_SIZE','unknown'),
                                                   detail='legacy owner has no verifiable Unix endpoint')
                        with socket.socket(socket.AF_UNIX) as connection:
                            connection.connect(endpoint)
                            peer_pid,peer_uid,_=peer(connection)
                            if peer_pid!=pid or peer_uid!=pwd.getpwnam('var').pw_uid or birth(pid)!=identity:
                                raise CachePolicyError(endpoint=endpoint,detail='legacy endpoint owner/birth mismatch')
                        signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                    select.select([descriptor], [], [])
                finally: os.close(descriptor)
            except (FileNotFoundError, ProcessLookupError): continue
        CACHE_ENDPOINT.unlink(missing_ok=True); CONTROL.unlink(missing_ok=True)
        # The installer switches the whole generation while admission is still
        # exclusively held. Only then may the new host service become READY.
        yield
        unit = ('[Unit]\nDescription=Birth-validated shared buildbox cache\n'
                '[Service]\nType=notify\nExecStart=/usr/bin/python3 -B /home/var/bin/bb-cache.py serve\n'
                'AllowedCPUs='+','.join(map(str,cpu_host_set()))+'\n'
                'KillMode=process\n[Install]\nWantedBy=multi-user.target\n').encode()
        # Reuse the sole atomic installation implementation.
        import importlib.util
        spec = importlib.util.spec_from_file_location('bb_install', Path(__file__).with_name('bb-install.py'))
        installer = importlib.util.module_from_spec(spec); spec.loader.exec_module(installer)
        installer.publish('/etc/systemd/system/buildbox-cache.service', unit, 0o644)
        subprocess.run(['systemctl', 'daemon-reload'], check=True)
        subprocess.run(['systemctl', 'enable', '--now', 'buildbox-cache.service'], check=True)


@contextlib.contextmanager
def migration(drained=False):
    try:
        with _migration(drained):yield
    except CachePolicyError:raise
    except (OSError,ValueError,KeyError,subprocess.CalledProcessError) as error:
        raise CachePolicyError(detail='host migration: '+str(error)) from error


def recover():
    with migration():pass


if __name__ == '__main__':
    try:
        if sys.argv[1:] == ['serve']: serve()
        elif sys.argv[1:] == ['recover']: recover()
        elif sys.argv[1:2] == ['compiler'] and len(sys.argv) > 2: compiler(sys.argv[2:])
        else: raise CachePolicyError(detail='invalid cache authority command')
    except (OSError, RuntimeError, ValueError, KeyError) as error:
        print(str(error) if isinstance(error, CachePolicyError) else str(CachePolicyError(detail=str(error))), file=sys.stderr)
        raise SystemExit(224)
