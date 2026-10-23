#!/usr/bin/env python3
"""Hermetic bb2 execution, disk admission, and one owned sccache endpoint."""
import ctypes
from datetime import datetime, timezone
import hashlib
import fcntl
import json
import os
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

def inventory(real_home):
    # Comparing each path catches directory growth even when another process
    # deletes files elsewhere; symlinks are never followed.
    result={}
    for root in [real_home/'.local/share/acyclic',real_home/'.cache']:
        if not root.exists(): continue
        for where,dirs,files in os.walk(root,followlinks=False):
            # ~/.cache/sccache belongs to the standing shared sccache server, which other
            # tenants fill concurrently; jobs here use SCCACHE_DIR=/mnt/bb/sccache.
            if where==str(root) and root.name=='.cache': dirs[:]=[d for d in dirs if d!='sccache']
            for name in dirs+files:
                p=Path(where)/name
                try:
                    s=p.lstat(); result[str(p)]=(0 if stat.S_ISDIR(s.st_mode) else s.st_size,s.st_mtime_ns)
                except FileNotFoundError: pass
    return result

def growth(before,after):
    return [p for p,v in after.items() if p not in before or v[0]>before[p][0]]

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
    fd=os.pidfd_open(server['pid'])
    try:
        if identity(server['pid'])==server['identity']: signal.pidfd_send_signal(fd,signal.SIGTERM)
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


def run(argv):
    meta=os.environ.get('BUILD_SLOT_RECEIPT_META')
    if not meta: raise RuntimeError('bb-job requires a terminal-receipt owner')
    # Nested build-slot reuses the enclosing job's home/admission/server, rather
    # than consuming a second tenant slot or changing the tested environment.
    if os.environ.get('BB_JOB_ACTIVE')=='1':
        if Path(argv[0]).name=='cargo':
            os.environ['RUSTC']='/home/var/bin/bigstack/rustc'
            if os.environ.get('BB_CACHE_DISABLED')=='1':os.environ['RUSTC_WRAPPER']=''
            else:os.environ.setdefault('RUSTC_WRAPPER','/home/var/bin/sccache-rustc')
        os.execvpe(argv[0],argv,os.environ)
    JOBS.mkdir(parents=True,exist_ok=True)
    job=Path(tempfile.mkdtemp(prefix='job-',dir=JOBS)); job.chmod(0o700)
    env=dict(os.environ); real_home=Path('/home/var')
    # sudo/runuser may strip admission env across the mount-namespace boundary.
    # The admitted wrapper persists it before privilege changes.
    admission_file=Path(meta+'.admission')
    admission=json.loads(admission_file.read_text()) if admission_file.exists() else {'tier':env.get('BB_ADMIT_TIER'),'tenant':env.get('BB_ADMIT_TENANT'),'wait_seconds':env.get('BB_ADMIT_WAIT_SECONDS'),'build_jobs':env.get('CARGO_BUILD_JOBS')}
    if admission.get('build_jobs'):env['CARGO_BUILD_JOBS']=str(admission['build_jobs'])
    runtime={'hermetic_home':str(job),'targets_policy':'job-owned','cache':'not-started','disk_free':disk(),
             'admission':admission}
    runtimefile=Path(meta+'.runtime'); atomic(runtimefile,runtime)
    server=None; child=None; before=None; rc=224; units=[]
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
                   CARGO_HOME='/home/var/.cargo',RUSTUP_HOME='/home/var/.rustup',
                   BB_JOB_ACTIVE='1',BB_ADMITTED='1',BB_JOB_HOME=str(job/'home'),BB_JOB_TMP=str(job/'tmp'),BB_JOB_TARGET=str(job/'target'),
                   XWIN_CACHE_DIR='/mnt/bb/toolchains/cargo-xwin',
                   CARGO_TARGET_DIR=str(job/'target'),GATE_MAIN_TARGET_DIR=str(job/'gate-main'),
                   GATE_FEATURE_TARGET_DIR=str(job/'gate-features'),
                   GATE_WINDOWS_RUNTIME_TARGET_DIR=str(job/'gate-windows'))
        for key in ['HOME','XDG_DATA_HOME','XDG_STATE_HOME','XDG_CACHE_HOME','XDG_CONFIG_HOME','XDG_RUNTIME_DIR','TMPDIR']: Path(env[key]).mkdir(mode=0o700)
        # Targets are job-owned scratch; only the content-addressed compiler
        # cache and installed toolchains persist. Export requested artifacts
        # before deleting the job tree.
        Path(env['CARGO_TARGET_DIR']).mkdir(mode=0o700)
        target=Path(env['CARGO_TARGET_DIR'])
        try:
            parts=target.relative_to('/mnt/bb/targets').parts
            locks=Path('/mnt/bb/locks/unit');locks.mkdir(parents=True,exist_ok=True)
            for name in sorted(set([parts[0],'%'.join(parts[:2])])):
                unit=open(locks/(name+'.lock'),'a');fcntl.flock(unit,fcntl.LOCK_SH);units.append(unit)
        except ValueError: pass
        # Preserve only persistent toolchain directories, never user state.
        env.setdefault('PATH','/home/var/.cargo/bin:/usr/bin:/bin')
        before=inventory(real_home)
        if env.get('BB_CACHE_DISABLED')=='1' or env.get('RUSTC_WRAPPER')=='':
            env['BB_CACHE_DISABLED']='1';runtime['cache']='disabled'
        else:server,runtime['cache']=start_cache(job,env)
        runtime['sccache_server']=server
        atomic(runtimefile,runtime)
        if server is None:
            env['RUSTC_WRAPPER']=''; env['BB_CACHE_DISABLED']='1'
            print('[buildbox] '+runtime['cache']+'; building uncached',file=sys.stderr,flush=True)
        elif Path(argv[0]).name=='cargo':
            env['RUSTC']='/home/var/bin/bigstack/rustc';env.setdefault('RUSTC_WRAPPER','/home/var/bin/sccache-rustc')
        if not Path(meta+'.exec').exists() and env.get('BUILD_SLOT_EXEC_IN_PAYLOAD')!='1':
            subprocess.run([NODE,str(BIN/'buildbox-receipt.mjs'),'exec',meta],env=env,check=True)
        # Stay in the admission lease's process group. A killed client causes
        # the daemon/receipt watcher to terminate the entire leased group.
        child=subprocess.Popen(argv,env=env)
        rc=child.wait(); rc=128-rc if rc<0 else rc
        if rc==0 and env.get('BB_JOB_EXPORT_SPEC'):
            try:
                from job_exports import prepare
                prepare(json.loads(env['BB_JOB_EXPORT_SPEC']), env['CARGO_TARGET_DIR'])
            except Exception as error:
                print('[buildbox] export INFRA: '+str(error),file=sys.stderr,flush=True)
                runtime['export_error']=str(error);rc=224
        leaked=growth(before,inventory(real_home))
        if leaked:
            print('\nerror: GATE_HOME_LEAK real user state grew: '+', '.join(leaked[:30]),file=sys.stderr,flush=True)
            runtime['home_leaks']=leaked; atomic(runtimefile,runtime)
            if rc==0: rc=2
        else: print('\nGATE_HOME_GUARD rc=0',file=sys.stderr,flush=True)
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
            raise
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
    if sys.argv[1:2]==['cleanup']: cleanup_runtime(sys.argv[2])
    elif sys.argv[1:2]==['stop-job']:stop_job(sys.argv[2])
    else: raise SystemExit(run(sys.argv[1:]))
