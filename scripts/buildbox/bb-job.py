#!/usr/bin/env python3
"""Hermetic bb2 execution, disk admission, and one owned sccache endpoint."""
import ctypes
import contextlib
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
import pwd
import resource
import select
from pathlib import Path
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import uuid
import bb_target_store
from admission_policy import resolve_wrapper, capture_limits, restore_limits, LIMIT_ENV

BIN=Path(__file__).parent
NODE='/home/var/tools/node24/node-v24.3.0-linux-x64/bin/node'
SCCACHE='/home/var/.cargo/bin/sccache'
JOBS=Path('/mnt/bb/jobs')
PIDFD_SIGNAL_PROCESS_GROUP=1 << 2

def identity(pid):
    try:
        fields=Path(f'/proc/{pid}/stat').read_text().split(') ')[-1].split()
        return Path('/proc/sys/kernel/random/boot_id').read_text().strip()+':'+fields[19] if fields[0]!='Z' else None
    except OSError: return None

def pid_namespace():
    return os.readlink('/proc/self/ns/pid')

def atomic(file,value):
    tmp=Path(str(file)+'.'+str(os.getpid())+'.tmp')
    with tmp.open('w') as f: json.dump(value,f); f.flush(); os.fsync(f.fileno())
    os.replace(tmp,file)

def disk():
    readings=[]
    for path in ['/', '/mnt/bb']:
        s=os.statvfs(path); pct=100*s.f_bavail/max(1,s.f_blocks)
        readings.append({'path':path,'free_percent':pct,'free_bytes':s.f_bavail*s.f_frsize})
    return readings

def home_lease(job,exclusive=False):
    directory=Path('/mnt/bb/locks/job-home');directory.mkdir(parents=True,exist_ok=True)
    lease=open(directory/(job.name+'.lock'),'a')
    try:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB if exclusive else fcntl.LOCK_SH)
    except BlockingIOError:
        lease.close();raise RuntimeError('refusing cleanup of leased live job '+str(job))
    return lease

def require_private_proc():
    # The launcher becomes PID 1 only in its new PID namespace, and a fresh
    # proc mount must show that same numbering. Host /proc roots would expose
    # another process's writable view of /home/var around the read-only bind.
    if os.getpid()!=1 or not Path('/proc/self/stat').read_text().startswith('1 ('):
        raise RuntimeError('real HOME requires a private PID namespace and proc mount')

def protect_home(real_home):
    """Verify the inherited private read-only HOME mount, retaining setuid FUSE."""
    real_home=real_home.resolve(strict=True)
    require_private_proc()
    # Even O_RDONLY handles can reopen their old writable mount through
    # /proc/self/fd. Refuse every inherited HOME handle, irrespective of mode.
    for fd in os.listdir('/proc/self/fd'):
        try:
            link=os.readlink(f'/proc/self/fd/{fd}')
            if not link.startswith('/'):continue
            target=Path(link).resolve()
            if target.is_relative_to(real_home):
                raise RuntimeError('inherited real HOME descriptor '+str(fd))
        except FileNotFoundError:pass
    if not os.statvfs(real_home).f_flag & os.ST_RDONLY:
        raise RuntimeError('real HOME requires a private read-only mount')
    # Recursive read-only protects submounts too; a forged environment marker
    # or a writable child mount must never qualify as the boundary.
    mounts=[]
    for line in Path('/proc/self/mountinfo').read_text().splitlines():
        fields=line.split()
        point=Path(__import__('re').sub(r'\\([0-7]{3})',lambda m:chr(int(m[1],8)),fields[4]))
        if point==real_home or point.is_relative_to(real_home):
            if 'ro' not in fields[5].split(','):raise RuntimeError('writable real HOME submount '+str(point))
            mounts.append(str(point))
    if str(real_home) not in mounts:raise RuntimeError('missing real HOME bind mount')
    if 'NoNewPrivs:\t1' in Path('/proc/self/status').read_text():
        raise RuntimeError('inherited no_new_privs disables required setuid FUSE')
    return {'mechanism':'private-read-only-mount','real_home':str(real_home),'writable':False,'recursive':True,'private_proc':True}


def mount_home_namespace():
    """Called as root only inside a newly unshared mount namespace."""
    if os.geteuid()!=0:raise RuntimeError('HOME namespace setup requires root')
    user_path=os.environ.pop('BB_JOB_NAMESPACE_PATH')
    require_private_proc()
    subprocess.run(['mount','--rbind','/home/var','/home/var'],check=True)
    class MountAttr(ctypes.Structure):
        _fields_=[('attr_set',ctypes.c_uint64),('attr_clr',ctypes.c_uint64),('propagation',ctypes.c_uint64),('userns_fd',ctypes.c_uint64)]
    if os.uname().machine not in ['x86_64','aarch64']:raise RuntimeError('unsupported mount_setattr architecture')
    libc=ctypes.CDLL(None,use_errno=True);attr=MountAttr(1,0,0,0)
    if libc.syscall(ctypes.c_long(442),ctypes.c_int(-100),ctypes.c_char_p(b'/home/var'),ctypes.c_uint(0x8000),ctypes.byref(attr),ctypes.sizeof(attr)):
        raise OSError(ctypes.get_errno(),'recursive read-only HOME mount')
    # A cwd inherited from HOME still refers to the original writable mount.
    # Resolve it again through this namespace before starting any payload.
    os.chdir(os.getcwd())
    restore_limits(os.environ[LIMIT_ENV])
    user=pwd.getpwnam('var')
    os.setgroups(os.getgrouplist(user.pw_name,user.pw_gid));os.setgid(user.pw_gid);os.setuid(user.pw_uid)
    # sudo's secure_path applies during privileged setup; restore the caller's
    # toolchain PATH only after dropping privileges, before payload/receipt probes.
    os.environ['PATH']=user_path
    # A host receipt can recover descendants only after this namespace's init
    # has exited. Register the actual constructor, not a caller-supplied marker.
    ns=pid_namespace();registration=JOBS/(ns.removeprefix('pid:[').removesuffix(']')+'.pid-namespace.json')
    record={'namespace':ns,'init_identity':identity(1),'parent_receipt_meta':os.environ['BUILD_SLOT_RECEIPT_META'],
            'registration_file':str(registration),'epoch':uuid.uuid4().hex}
    atomic(registration,record)
    with open(os.environ['BUILD_SLOT_RECEIPT_META']+'.pid-namespaces','a') as index:
        index.write(json.dumps(str(registration))+'\n');index.flush();os.fsync(index.fileno())

def git_base(source):
    for ref in ['refs/remotes/origin/main','refs/heads/main']:
        result=subprocess.run(['git','-C',str(source),'rev-parse','--verify',ref],capture_output=True,text=True)
        if result.returncode==0:return result.stdout.strip()
    return None

def private_git(job,real_home,source):
    """Keep mutable Git state outside HOME without changing the shared copy.

    buildbox already owns a private mount namespace. Bind only this job's .git
    pointer there; source bytes and the host's pointer are unchanged. Objects
    from the real mirror are read-only alternates; new objects/refs/index live
    in the disposable job tree.
    """
    if not source:return None
    def git(*args):
        return subprocess.check_output(['git','-C',str(source),*args],stderr=subprocess.PIPE,text=True).strip()
    probe=subprocess.run(['git','-C',str(source),'rev-parse','--show-toplevel'],capture_output=True,text=True)
    if probe.returncode:return None
    source=Path(probe.stdout.strip())
    common=Path(git('rev-parse','--path-format=absolute','--git-common-dir')).resolve()
    if not common.is_relative_to(real_home.resolve()):return None
    if os.environ.get('BB_JOB_PRIVATE_MOUNT')!='1' or source!=Path('/work/src') or not (source/'.git').is_file():
        raise RuntimeError('HOME-backed Git metadata requires the private buildbox worktree namespace')
    own_ns=os.readlink('/proc/self/ns/mnt')
    require_private_proc()
    if not os.statvfs(real_home).f_flag & os.ST_RDONLY:
        raise RuntimeError('refusing private Git mount without the read-only HOME boundary')
    head=git('rev-parse','HEAD')
    base=git_base(source)
    private=job/'git';subprocess.run(['git','init','--bare','--quiet',str(private)],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    (private/'objects/info/alternates').write_text(str(common/'objects')+'\n')
    def local(*args):subprocess.run(['git','--git-dir='+str(private),*args],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    local('config','core.bare','false');local('config','core.worktree',str(source))
    local('update-ref','--no-deref','HEAD',head);local('read-tree',head)
    if base:local('update-ref','refs/remotes/origin/main',base)
    pointer=job/'git-pointer';pointer.write_text('gitdir: '+str(private)+'\n')
    subprocess.run(['sudo','-n','mount','--bind',str(pointer),str(source/'.git')],check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    return {'private_gitdir':str(private),'read_only_objects':str(common/'objects'),'mount_namespace':own_ns}

def server_identity(endpoint):
    # SO_PEERCRED identifies the server, then /proc birth time protects against
    # PID reuse. A port number or a pid alone is never authority to stop it.
    c=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
    try:
        c.connect(str(endpoint))
        import struct
        pid,uid,gid=struct.unpack('3i',c.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
        return {'pid':pid,'identity':identity(pid),'endpoint':str(endpoint),'pid_namespace':pid_namespace()}
    finally: c.close()

def stop_server(server):
    if server and server.get('pid_namespace')!=pid_namespace():return
    if not server or identity(server['pid'])!=server['identity']: return
    try: env=Path(f'/proc/{server["pid"]}/environ').read_bytes().split(b'\0')
    except FileNotFoundError: return
    if ('SCCACHE_SERVER_UDS='+server['endpoint']).encode() not in env: return
    try:fd=os.pidfd_open(server['pid'])
    except ProcessLookupError:return
    try:
        if identity(server['pid'])==server['identity']:
            try:signal.pidfd_send_signal(fd,signal.SIGTERM)
            except ProcessLookupError:pass
    finally: os.close(fd)

def start_cache(job,env):
    endpoint=job/'sccache.sock'
    env.update(SCCACHE_SERVER_UDS=str(endpoint),SCCACHE_DIR='/mnt/bb/sccache',SCCACHE_CACHE_SIZE='400G',SCCACHE_IDLE_TIMEOUT='120')
    env.pop('SCCACHE_SERVER_PORT',None)
    # User cache locations and XDG overrides cannot redirect the cache to root.
    try:
        r=subprocess.run([SCCACHE,'--start-server'],env=env,capture_output=True,text=True,timeout=15)
    except (OSError, subprocess.TimeoutExpired) as e:
        return None,'startup-failed:'+str(e).replace('\n',' ')[:1000]
    diagnostic=(r.stdout+r.stderr).strip() or f'exit={r.returncode}'
    print('[buildbox] sccache startup: '+diagnostic,file=sys.stderr,flush=True)
    if r.returncode:
        return None,'startup-failed:'+diagnostic.replace('\n',' ')[:1000]
    try: return server_identity(endpoint),'ready'
    except OSError as e: return None,'startup-failed:'+str(e)

def remove_home(job):
    # Repair directory permissions before traversal. shutil's onexc may receive
    # os.open (which needs flags), so blindly retrying fn(path) is incorrect.
    job.chmod(0o700)
    for where, dirs, _ in os.walk(job, topdown=True, followlinks=False):
        for name in dirs:
            path = Path(where)/name
            if not path.is_symlink(): path.chmod(0o700)
    shutil.rmtree(job)


@contextlib.contextmanager
def adopted_child_reaper(command_pid):
    """Reap PID 1's adopted exits while the sole managed command waits.

    Setup and cleanup own their synchronous subprocess waits. During this
    interval only the custody supervisor is managed; its status stays with
    Popen even if it dies unexpectedly. WNOWAIT makes that boundary explicit.
    SIGCHLD drives the drain, with no timers or /proc process-tree polling.
    """
    if os.getpid()!=1:
        yield
        return
    previous=signal.getsignal(signal.SIGCHLD)
    if previous not in (signal.SIG_DFL,signal.SIG_IGN):
        raise RuntimeError('namespace child reaper already owned')
    draining=False
    def drain(number=None,frame=None):
        nonlocal draining
        if draining:return
        draining=True
        try:
            while True:
                try:event=os.waitid(os.P_ALL,0,os.WEXITED|os.WNOHANG|os.WNOWAIT)
                except ChildProcessError:return
                if event is None or event.si_pid==command_pid:return
                os.waitpid(event.si_pid,0)
        finally:draining=False
    signal.signal(signal.SIGCHLD,drain)
    try:
        drain()
        yield
    finally:
        try:drain()
        finally:signal.signal(signal.SIGCHLD,previous)


class OwnedCommand:
    """Keep the command's group leader alive until its custody pipe closes."""
    def __init__(self, argv, env, register):
        control_read, self.control = os.pipe()
        status_read, status_write = os.pipe()
        self.child = None
        self.command = None
        self.status = None
        try:
            # A library bootstrap enters the already-owned command domain.
            # There is no CLI subcommand that bypasses run's HOME constructor.
            bootstrap = '''import importlib.util,sys
from pathlib import Path
sys.path.insert(0,str(Path(sys.argv[1]).resolve().parent))
spec=importlib.util.spec_from_file_location('command_owner',sys.argv[1])
owner=importlib.util.module_from_spec(spec);spec.loader.exec_module(owner)
raise SystemExit(owner.supervise(int(sys.argv[2]),int(sys.argv[3]),sys.argv[4:]))
'''
            self.child = subprocess.Popen(
                [sys.executable, '-c', bootstrap, str(Path(__file__).resolve()),
                 str(control_read), str(status_write), *argv],
                env=env, pass_fds=(control_read, status_write), start_new_session=True)
            self.status = os.fdopen(status_read, 'rb')
            status_read = None
            # Before admission, parent death closes the pipe without allowing
            # the command to run. After admission, the recorded supervisor
            # pins the group identity until every cleanup path stops it.
            self.command = {'pid': self.child.pid, 'identity': identity(self.child.pid),
                            'pid_namespace': pid_namespace()}
            register(self.command)
            os.write(self.control, b'S')
        except BaseException:
            self.close()
            raise
        finally:
            os.close(control_read)
            os.close(status_write)
            if status_read is not None: os.close(status_read)

    def wait(self):
        with adopted_child_reaper(self.child.pid):
            line = self.status.readline()
        if not line:
            raise RuntimeError('command supervisor exited without an outcome')
        return int(line)

    def close(self):
        if self.control is not None:
            os.close(self.control)
            self.control = None
        if self.child is not None:
            stop_command(self.command)
            self.child.wait()
        if self.status is not None: self.status.close()


def supervise(control, status, argv):
    # This supervisor is the session/group leader. Its parent owns the only
    # custody writer; even SIGKILL before registration or during execution
    # therefore reaches the same group-retirement path through EOF.
    def interrupted(number, frame):
        raise SystemExit(128+number)
    for number in [signal.SIGTERM, signal.SIGINT, signal.SIGHUP]:
        signal.signal(number, interrupted)
    try:
        if os.read(control, 1) != b'S': return 224
        child = subprocess.Popen(argv)
        exited = os.pidfd_open(child.pid)
        try:
            ready, _, _ = select.select([control, exited], [], [])
            if control in ready: return 224
            os.write(status, (str(child.wait())+'\n').encode())
            # Preserve the group identity after the command exits, including
            # when a shell exits before its descendants.
            os.read(control, 1)
        finally: os.close(exited)
    finally:
        try: os.killpg(os.getpid(), signal.SIGKILL)
        except ProcessLookupError: pass


def stop_command(command):
    # Numeric process identities are meaningful only in their recorded namespace.
    # A foreign receipt owner retires the namespace through its registered init.
    if not command or command.get('pid_namespace') != pid_namespace(): return
    if identity(command['pid']) != command['identity']: return
    try: descriptor = os.pidfd_open(command['pid'])
    except ProcessLookupError: return
    try:
        if identity(command['pid']) != command['identity']: return
        # Linux >= 6.9 addresses the pinned process group through its leader's
        # pidfd. Retirement cannot depend on that leader running a handler (a
        # stopped supervisor cannot handle TERM), or race a recycled group ID.
        try: signal.pidfd_send_signal(descriptor, signal.SIGKILL, None, PIDFD_SIGNAL_PROCESS_GROUP)
        except ProcessLookupError: pass
        select.select([descriptor], [], [])
    finally: os.close(descriptor)


def compiler_policy(env):
    # Every target is deleted at job exit. Incremental rustc output cannot be
    # reused there, and its -C incremental argument bypasses sccache entirely.
    # Cargo's global setting also wins over inherited per-profile overrides.
    env['CARGO_INCREMENTAL']='0'


def child_cpu():
    usage=resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime+usage.ru_stime


def cache_cpu(server):
    if not server or identity(server['pid'])!=server['identity']:return None
    try:
        fields=Path(f'/proc/{server["pid"]}/stat').read_text().split(') ')[-1].split()
        return sum(float(fields[i]) for i in [11,12,13,14])/os.sysconf('SC_CLK_TCK')
    except (OSError,ValueError,IndexError):return None


def cost_receipt(env,server,started_cpu,started_cache_cpu,started_wall):
    own=max(0,child_cpu()-started_cpu)
    now=cache_cpu(server)
    cache=max(0,now-started_cache_cpu) if now is not None and started_cache_cpu is not None else None
    out={'payload_wall_seconds':max(0,time.monotonic()-started_wall),
         'waited_children_cpu_seconds':own,'owned_cache_cpu_seconds':cache,
         'attributed_cpu_seconds':own+cache if cache is not None else (own if server is None else None),
         'cpu_coverage':'waited-child-tree+owned-sccache; excludes unawaited descendants',
         'compiler_policy':'ephemeral-target-no-incremental-v1'}
    if server and cache is None:
        out['cpu_incomplete_reason']='owned-sccache-unavailable-at-terminal-sample'
        out['sccache_stats_error']='owned-server-unavailable'
    elif server:
        try:
            result=subprocess.run([SCCACHE,'--show-stats','--stats-format','json'],env=env,capture_output=True,text=True,timeout=5)
            if identity(server['pid'])!=server['identity']:out['sccache_stats_error']='owned-server-exited-during-stats'
            elif result.returncode==0:out['sccache']=json.loads(result.stdout)
            else:out['sccache_stats_error']='exit='+str(result.returncode)
        except (OSError,ValueError,subprocess.TimeoutExpired) as error:
            out['sccache_stats_error']=type(error).__name__
    return out


def run(argv,namespace_ready=False):
    meta=os.environ.get('BUILD_SLOT_RECEIPT_META')
    if not meta: raise RuntimeError('bb-job requires a terminal-receipt owner')
    if 'NoNewPrivs:\t1' in Path('/proc/self/status').read_text():
        print('[buildbox] HOME guard INFRA: inherited no_new_privs disables required setuid FUSE',file=sys.stderr,flush=True)
        return 224
    # Every entry, including a nested or forged-active entry, passes the same
    # kernel namespace constructor. No environment marker can bypass it.
    # No no_new_privs: fusermount3 must retain setuid.
    if not namespace_ready:
        if LIMIT_ENV not in os.environ or (os.environ.get('BB_JOB_ACTIVE')=='1' and os.environ.get('BB_CALLER_RLIMITS_READY')!='1'):
            os.environ[LIMIT_ENV]=json.dumps(capture_limits())
        os.environ.pop('BB_CALLER_RLIMITS_READY',None)
        os.environ['BB_JOB_NAMESPACE_PATH']=os.environ.get('PATH','/usr/bin:/bin')
        os.execvp('sudo',['sudo','-n','--preserve-env','unshare','--mount','--propagation','private','--pid','--fork','--kill-child','--mount-proc',sys.executable,str(Path(__file__).resolve()),'home-namespace',*argv])
    compiler_job=Path(argv[0]).name=='cargo' or bool(json.loads(Path(meta).read_text()).get('gate_log'))
    # Nested build-slot reuses the enclosing job's home/admission/server, rather
    # than consuming a second tenant slot or changing the tested environment.
    if os.environ.get('BB_JOB_ACTIVE')=='1':
        # The marker alone is not proof: callers and pre-upgrade parents may
        # supply it without an inherited read-only mount.
        try:protect_home(Path('/home/var'))
        except (OSError,RuntimeError) as error:
            print('[buildbox] HOME guard INFRA: '+str(error),file=sys.stderr,flush=True)
            return 224
        os.environ['CARGO_HOME']='/mnt/bb/toolchains/cargo'
        compiler_policy(os.environ)
        if compiler_job:
            os.environ['RUSTC']='/home/var/bin/bigstack/rustc'
            if os.environ.get('BB_CACHE_DISABLED')=='1':os.environ['RUSTC_WRAPPER']=''
            else:os.environ.setdefault('RUSTC_WRAPPER','/home/var/bin/sccache-rustc')
        try: resolve_wrapper(os.environ, compiler=compiler_job)
        except RuntimeError as error:
            print('[buildbox] '+str(error),file=sys.stderr,flush=True);return 224
        if Path(argv[0]).name=='cargo': argv=['/home/var/bin/bb-cargo/cargo']+argv[1:]
        os.execvpe(argv[0],argv,os.environ)
    JOBS.mkdir(parents=True,exist_ok=True)
    job=Path(tempfile.mkdtemp(prefix='job-',dir=JOBS)); job.chmod(0o700)
    custody=home_lease(job)
    env=dict(os.environ); real_home=Path('/home/var')
    # Export scope belongs to this receipt owner, never to nested commands.
    export_spec=env.pop('BB_JOB_EXPORT_SPEC',None)
    compiler_policy(env)
    # sudo/runuser may strip admission env across the mount-namespace boundary.
    # The admitted wrapper persists it before privilege changes.
    admission_file=Path(meta+'.admission')
    admission=json.loads(admission_file.read_text()) if admission_file.exists() else {'tier':env.get('BB_ADMIT_TIER'),'tenant':env.get('BB_ADMIT_TENANT'),'wait_seconds':env.get('BB_ADMIT_WAIT_SECONDS'),'build_jobs':env.get('CARGO_BUILD_JOBS')}
    if admission.get('build_jobs'):env['CARGO_BUILD_JOBS']=str(admission['build_jobs'])
    runtime={'hermetic_home':str(job),'targets_policy':'job-owned','cache':'not-started','disk_free':disk(),
             'admission':admission, 'caller_rlimits':json.loads(env[LIMIT_ENV]), 'exec_rlimits':capture_limits()}
    runtimefile=Path(meta+'.runtime'); atomic(runtimefile,runtime)
    server=None; child=None; rc=224; units=[]
    def interrupted(n,f):
        for sig in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]: signal.signal(sig,signal.SIG_IGN)
        raise SystemExit(128+n)
    for sig in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]: signal.signal(sig,interrupted)
    try:
        try: runtime['rustc_wrapper']=resolve_wrapper(env, compiler=compiler_job)
        except RuntimeError as error:
            runtime['admission_error']=str(error)
            print('[buildbox] '+str(error),file=sys.stderr,flush=True);return 224
        print('BB_DISK_FREE '+json.dumps(runtime['disk_free']),file=sys.stderr,flush=True)
        if any(x['free_percent']<10 for x in runtime['disk_free']):
            print('BB_DISK_LOW preparation-INFRA: / and /mnt/bb require at least 10% free',file=sys.stderr,flush=True)
            return 224
        env.update(HOME=str(job/'home'),XDG_DATA_HOME=str(job/'data'),XDG_STATE_HOME=str(job/'state'),
                   XDG_CACHE_HOME=str(job/'cache'),XDG_CONFIG_HOME=str(job/'config'),
                   XDG_RUNTIME_DIR=str(job/'runtime'),TMPDIR=str(job/'tmp'),
                   CARGO_HOME='/mnt/bb/toolchains/cargo',RUSTUP_HOME='/home/var/.rustup',
                   BB_JOB_ACTIVE='1',BB_ADMITTED='1',BB_JOB_HOME=str(job/'home'),BB_JOB_TMP=str(job/'tmp'),BB_JOB_TARGET=str(job/'target'),
                   XWIN_CACHE_DIR='/mnt/bb/toolchains/cargo-xwin',
                   CARGO_TARGET_DIR=str(job/'target'),GATE_MAIN_TARGET_DIR=str(job/'gate-main'),
                   GATE_FEATURE_TARGET_DIR=str(job/'gate-features'),
                   GATE_WINDOWS_RUNTIME_TARGET_DIR=str(job/'gate-windows'))
        metadata=json.loads(Path(meta).read_text())
        # The launcher's existing bind is private and owned by this receipt.
        # Stable paths keep build-script OUT_DIR and linked artifacts valid.
        if metadata.get('repo')=='sdk' and bb_target_store.private_target(metadata):
            env.update(CARGO_TARGET_DIR='/work/target', BB_JOB_TARGET='/work/target',
                       GATE_MAIN_TARGET_DIR='/work/target/.gate-main',
                       GATE_FEATURE_TARGET_DIR='/work/target/.gate-features',
                       GATE_WINDOWS_RUNTIME_TARGET_DIR='/work/target/.gate-windows')
            runtime['target_root']='/work/target'
        for key in ['HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_RUNTIME_DIR','TMPDIR']: Path(env[key]).mkdir(mode=0o700)
        runtime['storage_paths']=[bb_target_store.require_data_path(env[key]) for key in
                ['CARGO_TARGET_DIR','GATE_MAIN_TARGET_DIR','GATE_FEATURE_TARGET_DIR','GATE_WINDOWS_RUNTIME_TARGET_DIR','TMPDIR']]
        # Targets are job-owned scratch; only the content-addressed compiler
        # cache and installed toolchains persist. Export requested artifacts
        # before deleting the job tree.
        Path(env['CARGO_TARGET_DIR']).mkdir(mode=0o700,exist_ok=True)
        target=Path(env['CARGO_TARGET_DIR'])
        try:
            parts=target.relative_to('/mnt/bb/targets').parts
            locks=Path('/mnt/bb/locks/unit');locks.mkdir(parents=True,exist_ok=True)
            for name in sorted(set([parts[0],'%'.join(parts[:2])])):
                unit=open(locks/(name+'.lock'),'a');fcntl.flock(unit,fcntl.LOCK_SH);units.append(unit)
        except ValueError: pass
        # Preserve only persistent toolchain directories, never user state.
        env.setdefault('PATH','/home/var/.cargo/bin:/usr/bin:/bin')
        try:
            runtime['git_metadata']=private_git(job,real_home,os.environ.get('BB_JOB_GIT_SOURCE'))
            runtime['home_guard']=protect_home(real_home)
        except (OSError,RuntimeError,subprocess.SubprocessError) as error:
            runtime['home_guard_error']=str(error);atomic(runtimefile,runtime)
            print('[buildbox] HOME guard INFRA: '+str(error),file=sys.stderr,flush=True)
            return 224
        print('GATE_HOME_GUARD '+json.dumps(runtime['home_guard']),file=sys.stderr,flush=True)
        if env.get('BB_CACHE_DISABLED')=='1' or env.get('RUSTC_WRAPPER')=='':
            env['BB_CACHE_DISABLED']='1';runtime['cache']='disabled'
        else:server,runtime['cache']=start_cache(job,env)
        runtime['sccache_server']=server
        atomic(runtimefile,runtime)
        if compiler_job:env['RUSTC']='/home/var/bin/bigstack/rustc'
        if server is None:
            env['RUSTC_WRAPPER']=''; env['BB_CACHE_DISABLED']='1'
            print('[buildbox] '+runtime['cache']+'; building uncached',file=sys.stderr,flush=True)
        elif compiler_job:
            env['RUSTC']='/home/var/bin/bigstack/rustc';env.setdefault('RUSTC_WRAPPER','/home/var/bin/sccache-rustc')
        if not Path(meta+'.exec').exists() and env.get('BUILD_SLOT_EXEC_IN_PAYLOAD')!='1':
            subprocess.run([NODE,str(BIN/'buildbox-receipt.mjs'),'exec',meta],env=env,check=True)
        # Command custody retires descendants before local cleanup; the enclosing
        # PID namespace independently retires them when the receipt owner dies.
        started_cpu=child_cpu(); started_cache_cpu=cache_cpu(server); started_wall=time.monotonic()
        if Path(argv[0]).name=='cargo': argv=['/home/var/bin/bb-cargo/cargo']+argv[1:]
        def register_command(command):
            runtime['command']=command
            atomic(runtimefile,runtime)
        child=OwnedCommand(argv,env,register_command)
        rc=child.wait(); rc=128-rc if rc<0 else rc
        child.close()
        cache_records=Path(meta+'.target-store.jsonl')
        if cache_records.exists():
            runtime['target_store']=[json.loads(line) for line in cache_records.read_text().splitlines() if line]
        runtime['compute']=cost_receipt(env,server,started_cpu,started_cache_cpu,started_wall)
        atomic(runtimefile,runtime)
        if rc==0 and export_spec:
            try:
                from job_exports import prepare
                prepare(json.loads(export_spec), env['CARGO_TARGET_DIR'],
                        cache_reused=any(row.get('status')=='hit' for row in runtime.get('target_store',[])))
            except Exception as error:
                print('[buildbox] export INFRA: '+str(error),file=sys.stderr,flush=True)
                runtime['export_error']=str(error);rc=224
        return rc
    finally:
        # A signal does not leave test children running after their home/cache
        # is removed. The independent receipt watcher covers SIGKILL as well.
        for sig in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]: signal.signal(sig,signal.SIG_IGN)
        if child: child.close()
        stop_server(server)
        try:remove_home(job)
        except OSError as error:
            runtime['cleanup_error']=str(error);atomic(runtimefile,runtime)
            print('[buildbox] cleanup INFRA: '+str(error),file=sys.stderr,flush=True)
            return 224
        runtime['home_removed']=True; runtime['targets_removed']=True; atomic(runtimefile,runtime)
        custody.close()
        for unit in units:unit.close()


def cleanup_runtime(file):
    runtime=json.loads(Path(file).read_text())
    if not runtime.get('hermetic_home'):
        stop_server(runtime.get('sccache_server'));return
    job=Path(runtime['hermetic_home'])
    if job.parent.resolve()!=JOBS.resolve() or not job.name.startswith('job-'):
        raise RuntimeError('refusing cleanup outside /mnt/bb/jobs/job-*')
    with home_lease(job,exclusive=True):
        stop_command(runtime.get('command'))
        stop_server(runtime.get('sccache_server'))
        if job.exists(): remove_home(job)
    runtime['home_removed']=True
    if runtime.get('targets_policy')=='job-owned':runtime['targets_removed']=True
    atomic(file,runtime)

def cleanup_outer(prefix):
    prefix=Path(prefix)
    if prefix.parent.resolve()!=JOBS.resolve() or prefix.name in ['', '.', '..']:
        raise ValueError('refusing outer cleanup outside /mnt/bb/jobs')
    import contextlib
    runtime=Path(str(prefix)+'.receipt-meta.v2.json.runtime')
    metadata=json.loads(runtime.read_text()) if runtime.exists() else {}
    home=metadata.get('hermetic_home')
    with home_lease(Path(home),exclusive=True) if home else contextlib.nullcontext():
        for suffix in ['.targets','.tmp']:
            path=Path(str(prefix)+suffix)
            if path.is_symlink():path.unlink()
            elif path.exists():remove_home(path)
    return {'outer_targets_removed':True,'outer_tmp_removed':True}

def admitted_owner(item,root=Path('/mnt/bb/admit')):
    """The host daemon's lease also proves which PID coordinates it recorded.

    This is useful when independent producers write .job records: an absent
    namespace field alone is never authority to signal a numeric PID.
    """
    try:
        daemon=json.loads((root/'daemon.json').read_text())
        if identity(daemon['pid'])!=daemon['identity'] or os.readlink(f'/proc/{daemon["pid"]}/ns/pid')!=pid_namespace():return False
        for file in root.glob('lease-*.json'):
            try:lease=json.loads(file.read_text())
            except (OSError,ValueError):continue
            if (lease.get('pid'),lease.get('identity'))==(item['pid'],item['identity']):return True
    except (OSError,ValueError,KeyError):pass
    return False

def stop_job(file):
    item=json.loads(Path(file).read_text()); pid=item['pid']
    if item.get('pid_namespace')!=pid_namespace() and not (item.get('pid_namespace') is None and admitted_owner(item)):
        raise RuntimeError('cannot stop a job recorded in another or unknown PID namespace')
    if identity(pid)!=item['identity']: return
    try: fd=os.pidfd_open(pid)
    except ProcessLookupError: return
    try:
        if identity(pid)==item['identity']:
            try: os.killpg(pid,signal.SIGKILL)
            except ProcessLookupError: signal.pidfd_send_signal(fd,signal.SIGKILL)
        import select
        if not select.select([fd],[],[],10)[0]:raise RuntimeError('job still alive after termination; cleanup is unsafe')
    finally:os.close(fd)


if __name__=='__main__':
    if sys.argv[1:2]==['home-namespace']:
        try:mount_home_namespace();result=run(sys.argv[2:],namespace_ready=True)
        except (OSError,RuntimeError,subprocess.SubprocessError) as error:
            print('[buildbox] HOME guard INFRA: '+str(error),file=sys.stderr,flush=True);result=224
        raise SystemExit(result)
    elif sys.argv[1:2]==['cleanup']: cleanup_runtime(sys.argv[2])
    elif sys.argv[1:2]==['cleanup-outer']:print(json.dumps(cleanup_outer(sys.argv[2])))
    elif sys.argv[1:2]==['stop-job']:stop_job(sys.argv[2])
    else: raise SystemExit(run(sys.argv[1:]))
