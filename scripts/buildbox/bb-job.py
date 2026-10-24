#!/usr/bin/env python3
"""Hermetic bb2 execution, disk admission, and one owned sccache endpoint."""
import ctypes
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
import pwd
import resource
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

BIN=Path(__file__).parent
NODE='/home/var/tools/node24/node-v24.3.0-linux-x64/bin/node'
SCCACHE='/home/var/.cargo/bin/sccache'
JOBS=Path('/mnt/bb/jobs')

def identity(pid):
    try:
        fields=Path(f'/proc/{pid}/stat').read_text().split(') ')[-1].split()
        return Path('/proc/sys/kernel/random/boot_id').read_text().strip()+':'+fields[19] if fields[0]!='Z' else None
    except OSError: return None

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

def protect_home(real_home):
    """Verify the inherited private read-only HOME mount, retaining setuid FUSE."""
    real_home=real_home.resolve(strict=True)
    for fd in [0,1,2]:
        try:
            link=os.readlink(f'/proc/self/fd/{fd}')
            if not link.startswith('/'):continue
            target=Path(link).resolve()
            if target.is_relative_to(real_home) and fcntl.fcntl(fd,fcntl.F_GETFL)&os.O_ACCMODE!=os.O_RDONLY:
                raise RuntimeError('writable inherited real HOME descriptor '+str(fd))
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
    return {'mechanism':'private-read-only-mount','real_home':str(real_home),'writable':False,'recursive':True}


def mount_home_namespace():
    """Called as root only inside a newly unshared mount namespace."""
    if os.geteuid()!=0:raise RuntimeError('HOME namespace setup requires root')
    if os.readlink('/proc/self/ns/mnt')==os.readlink('/proc/1/ns/mnt'):
        raise RuntimeError('refusing real HOME mount in the host namespace')
    subprocess.run(['mount','--rbind','/home/var','/home/var'],check=True)
    class MountAttr(ctypes.Structure):
        _fields_=[('attr_set',ctypes.c_uint64),('attr_clr',ctypes.c_uint64),('propagation',ctypes.c_uint64),('userns_fd',ctypes.c_uint64)]
    if os.uname().machine not in ['x86_64','aarch64']:raise RuntimeError('unsupported mount_setattr architecture')
    libc=ctypes.CDLL(None,use_errno=True);attr=MountAttr(1,0,0,0)
    if libc.syscall(ctypes.c_long(442),ctypes.c_int(-100),ctypes.c_char_p(b'/home/var'),ctypes.c_uint(0x8000),ctypes.byref(attr),ctypes.sizeof(attr)):
        raise OSError(ctypes.get_errno(),'recursive read-only HOME mount')
    user=pwd.getpwnam('var')
    os.setgroups(os.getgrouplist(user.pw_name,user.pw_gid));os.setgid(user.pw_gid);os.setuid(user.pw_uid)

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
    host_ns=subprocess.check_output(['sudo','-n','readlink','/proc/1/ns/mnt'],text=True).strip()
    if own_ns==host_ns:raise RuntimeError('refusing private Git mount in the host namespace')
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
        return {'pid':pid,'identity':identity(pid),'endpoint':str(endpoint)}
    finally: c.close()

def stop_server(server):
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


def run(argv):
    meta=os.environ.get('BUILD_SLOT_RECEIPT_META')
    if not meta: raise RuntimeError('bb-job requires a terminal-receipt owner')
    # The kernel mount flags, not a caller-supplied marker, decide whether a
    # namespace is needed. No no_new_privs: fusermount3 must retain setuid.
    if not os.statvfs('/home/var').f_flag & os.ST_RDONLY:
        os.execvp('sudo',['sudo','-n','--preserve-env','unshare','--mount','--propagation','private',sys.executable,str(Path(__file__).resolve()),'home-namespace',*argv])
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
        if Path(argv[0]).name=='cargo': argv=['/home/var/bin/bb-cargo/cargo']+argv[1:]
        os.execvpe(argv[0],argv,os.environ)
    JOBS.mkdir(parents=True,exist_ok=True)
    job=Path(tempfile.mkdtemp(prefix='job-',dir=JOBS)); job.chmod(0o700)
    env=dict(os.environ); real_home=Path('/home/var')
    compiler_policy(env)
    # sudo/runuser may strip admission env across the mount-namespace boundary.
    # The admitted wrapper persists it before privilege changes.
    admission_file=Path(meta+'.admission')
    admission=json.loads(admission_file.read_text()) if admission_file.exists() else {'tier':env.get('BB_ADMIT_TIER'),'tenant':env.get('BB_ADMIT_TENANT'),'wait_seconds':env.get('BB_ADMIT_WAIT_SECONDS'),'build_jobs':env.get('CARGO_BUILD_JOBS')}
    if admission.get('build_jobs'):env['CARGO_BUILD_JOBS']=str(admission['build_jobs'])
    runtime={'hermetic_home':str(job),'targets_policy':'job-owned','cache':'not-started','disk_free':disk(),
             'admission':admission}
    runtimefile=Path(meta+'.runtime'); atomic(runtimefile,runtime)
    server=None; child=None; rc=224; units=[]
    def interrupted(n,f):
        if child:
            try: os.killpg(child.pid,n)
            except ProcessLookupError: pass
        raise SystemExit(128+n)
    for sig in [signal.SIGTERM,signal.SIGINT,signal.SIGHUP]: signal.signal(sig,interrupted)
    try:
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
        # Stay in the admission lease's process group. A killed client causes
        # the daemon/receipt watcher to terminate the entire leased group.
        started_cpu=child_cpu(); started_cache_cpu=cache_cpu(server); started_wall=time.monotonic()
        if Path(argv[0]).name=='cargo': argv=['/home/var/bin/bb-cargo/cargo']+argv[1:]
        child=subprocess.Popen(argv,env=env)
        rc=child.wait(); rc=128-rc if rc<0 else rc
        cache_records=Path(meta+'.target-store.jsonl')
        if cache_records.exists():
            runtime['target_store']=[json.loads(line) for line in cache_records.read_text().splitlines() if line]
        runtime['compute']=cost_receipt(env,server,started_cpu,started_cache_cpu,started_wall)
        atomic(runtimefile,runtime)
        if rc==0 and env.get('BB_JOB_EXPORT_SPEC'):
            try:
                from job_exports import prepare
                prepare(json.loads(env['BB_JOB_EXPORT_SPEC']), env['CARGO_TARGET_DIR'],
                        cache_reused=any(row.get('status')=='hit' for row in runtime.get('target_store',[])))
            except Exception as error:
                print('[buildbox] export INFRA: '+str(error),file=sys.stderr,flush=True)
                runtime['export_error']=str(error);rc=224
        return rc
    finally:
        # A signal does not leave test children running after their home/cache
        # is removed. The independent receipt watcher covers SIGKILL as well.
        if child and child.poll() is None:
            try: child.kill()
            except ProcessLookupError: pass
            child.wait()
        stop_server(server)
        try:remove_home(job)
        except OSError as error:
            runtime['cleanup_error']=str(error);atomic(runtimefile,runtime)
            print('[buildbox] cleanup INFRA: '+str(error),file=sys.stderr,flush=True)
            return 224
        runtime['home_removed']=True; runtime['targets_removed']=True; atomic(runtimefile,runtime)
        for unit in units:unit.close()


def cleanup_runtime(file):
    runtime=json.loads(Path(file).read_text())
    stop_server(runtime.get('sccache_server'))
    if not runtime.get('hermetic_home'):return
    job=Path(runtime['hermetic_home'])
    if job.parent.resolve()!=JOBS.resolve() or not job.name.startswith('job-'):
        raise RuntimeError('refusing cleanup outside /mnt/bb/jobs/job-*')
    if job.exists(): remove_home(job)
    runtime['home_removed']=True
    if runtime.get('targets_policy')=='job-owned':runtime['targets_removed']=True
    atomic(file,runtime)

def cleanup_outer(prefix):
    prefix=Path(prefix)
    if prefix.parent.resolve()!=JOBS.resolve() or prefix.name in ['', '.', '..']:
        raise ValueError('refusing outer cleanup outside /mnt/bb/jobs')
    for suffix in ['.targets','.tmp']:
        path=Path(str(prefix)+suffix)
        if path.is_symlink():path.unlink()
        elif path.exists():remove_home(path)
    return {'outer_targets_removed':True,'outer_tmp_removed':True}

def stop_job(file):
    item=json.loads(Path(file).read_text()); pid=item['pid']
    if identity(pid)!=item['identity']: return
    try: fd=os.pidfd_open(pid)
    except ProcessLookupError: return
    try:
        if identity(pid)==item['identity']:
            try: os.killpg(pid,signal.SIGKILL)
            except ProcessLookupError: signal.pidfd_send_signal(fd,signal.SIGKILL)
        import select
        select.select([fd],[],[],10)
    finally:os.close(fd)


if __name__=='__main__':
    if sys.argv[1:2]==['home-namespace']:
        try:mount_home_namespace();result=run(sys.argv[2:])
        except (OSError,RuntimeError,subprocess.SubprocessError) as error:
            print('[buildbox] HOME guard INFRA: '+str(error),file=sys.stderr,flush=True);result=224
        raise SystemExit(result)
    elif sys.argv[1:2]==['cleanup']: cleanup_runtime(sys.argv[2])
    elif sys.argv[1:2]==['cleanup-outer']:print(json.dumps(cleanup_outer(sys.argv[2])))
    elif sys.argv[1:2]==['stop-job']:stop_job(sys.argv[2])
    else: raise SystemExit(run(sys.argv[1:]))
