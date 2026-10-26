#!/usr/bin/python3 -B
"""Kernel adapter for bb-admit's single CPU pool; this is not a scheduler.

Root retains the slot ceiling and custody record. Only a bounded work subtree
is delegated. A slot is reusable after its exact run subtree is empty, including
orphaned descendants. Native and generated launchers use this same boundary.
"""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import pwd
import select
import signal
import socket
import struct
import subprocess
import sys
import uuid
from datetime import datetime, timezone
import shlex
from admission_policy import (CPU_LEASE_SHAPE, cpu_layout, atomic_json, require_tenant,
                              process_identity, restore_limits, LIMIT_ENV, CACHE_RECOVERY)
from admission_policy import parse_limits

CGROUP_ROOT = Path('/sys/fs/cgroup/bbjobs.slice/buildbox-cpu')
CONTROL_ROOT = Path('/run/buildbox-cpu')
CGROUP_FS = Path('/sys/fs/cgroup')


class LeaseError(RuntimeError):
    def __init__(self, step, detail):
        super().__init__('kind=CPU_LEASE step='+step+' owner=gate-speed recovery='+shlex.quote(shlex.join(CACHE_RECOVERY))+
                         ' detail='+shlex.quote(str(detail)))


def now():
    return datetime.now(timezone.utc).isoformat()


def cpus(text):
    result = set()
    for part in text.strip().split(','):
        if not part: continue
        ends = list(map(int, part.split('-')))
        result.update(range(ends[0], ends[-1]+1))
    return result


def cpu_text(values):
    return ','.join(map(str, sorted(values)))


def read_stats(directory):
    def values(name):
        try: return dict(line.split(maxsplit=1) for line in (directory/name).read_text().splitlines())
        except OSError as error: return {'unavailable': str(error)}
    return {'cpu_stat': values('cpu.stat'), 'cpu_pressure': values('cpu.pressure'),
            'coverage': 'exclusive-lease-cgroup-with-descendants; shared-host-cache-excluded'}


def populated(directory):
    return 'populated 1' in (directory/'cgroup.events').read_text()


def wait_empty(directory):
    # cgroup.events has EPOLLPRI change notification; no sleep/rescan loop.
    with (directory/'cgroup.events').open() as stream:
        poll = select.poll(); poll.register(stream.fileno(), select.POLLPRI | select.POLLERR)
        while True:
            stream.seek(0)
            if 'populated 0' in stream.read(): return
            poll.poll()


def protected(directory):
    for parent in [directory, *directory.parents]:
        info = parent.lstat()
        if parent.is_symlink() or info.st_uid != 0 or info.st_mode & 0o022:
            raise LeaseError('kernel-custody', 'unprotected '+str(parent))


def topology():
    expected = CPU_LEASE_SHAPE[0]*CPU_LEASE_SHAPE[1]*3//2
    actual = cpus(Path('/sys/devices/system/cpu/online').read_text())
    if actual != set(range(expected)) or Path('/sys/devices/system/cpu/smt/active').read_text().strip() != '0':
        raise LeaseError('cpu-topology', 'expected '+str(expected)+' online CPUs with SMT inactive')


def enable(directory):
    requested = {'cpu', 'cpuset', 'memory', 'pids'}
    if not requested.issubset(set((directory/'cgroup.controllers').read_text().split())):
        raise LeaseError('cgroup-controllers', str(directory))
    (directory/'cgroup.subtree_control').write_text(' '.join('+'+name for name in sorted(requested)))


def bound(directory, values):
    (directory/'cpuset.cpus').write_text(cpu_text(values))
    (directory/'cpu.max').write_text(str(len(values)*100000)+' 100000')
    if cpus((directory/'cpuset.cpus.effective').read_text()) != set(values):
        raise LeaseError('effective-cpuset', str(directory))
    if (directory/'cpu.max').read_text().split() != [str(len(values)*100000), '100000']:
        raise LeaseError('effective-cpu-max', str(directory))


def prepare(index):
    if os.geteuid() != 0: raise LeaseError('privileged-boundary', 'root constructor required')
    if CGROUP_ROOT == Path('/sys/fs/cgroup/bbjobs.slice/buildbox-cpu') and Path('/proc/1/comm').read_text().strip() != 'systemd':
        raise LeaseError('host-boundary', 'new host leases cannot start inside a job namespace')
    topology()
    layout = cpu_layout()
    if type(index) is not int or index not in range(len(layout)): raise LeaseError('slot-index', index)
    CONTROL_ROOT.mkdir(mode=0o755, exist_ok=True); protected(CONTROL_ROOT)
    CGROUP_ROOT.mkdir(exist_ok=True); protected(CGROUP_ROOT)
    bound(CGROUP_ROOT, {cpu for lease in layout for cpu in lease}); enable(CGROUP_ROOT)
    slot = CGROUP_ROOT/('slot-'+str(index)); slot.mkdir(exist_ok=True)
    bound(slot, layout[index]); enable(slot)
    return slot


def record_path(token):
    if len(token) != 32 or any(c not in '0123456789abcdef' for c in token): raise LeaseError('lease-token', token)
    return CONTROL_ROOT/(token+'.json')


def verified_record(token, directory_required=True):
    file = record_path(token); protected(file)
    record = json.loads(file.read_text()); index = record['index']
    if type(index) is not int or index not in range(len(cpu_layout())) or record.get('id') != token:
        raise LeaseError('record-schema', token)
    expected = CGROUP_ROOT/('slot-'+str(index))/('run-'+token)
    if (record['generation'] != Path('/proc/sys/kernel/random/boot_id').read_text().strip()
            or record['cgroup'] != str(expected) or record['cpus'] != list(cpu_layout()[index])):
        raise LeaseError('record-identity', token)
    if directory_required: protected(expected)
    return record, expected


def completed(token):
    # Fixed-size terminal slots make completion durable across the race between
    # pidfd notification and record retirement. Absence alone is not success.
    for index in range(CPU_LEASE_SHAPE[0]):
        file=CONTROL_ROOT/('slot-'+str(index)+'.terminal')
        try:
            protected(file);value=json.loads(file.read_text())
            if value.get('id')==token and value.get('empty') is True:return value
        except FileNotFoundError:pass
    raise LeaseError('unknown-completion',token)


def current(required=True):
    actual = CGROUP_FS/Path('/proc/self/cgroup').read_text().strip().split('::', 1)[1].lstrip('/')
    try: relative = actual.relative_to(CGROUP_ROOT)
    except ValueError:
        if not required: return None
        raise LeaseError('current-lease', 'process is not in an admitted lease')
    parts = relative.parts
    if len(parts) < 3 or not parts[0].startswith('slot-') or not parts[1].startswith('run-') or parts[2] != 'work':
        raise LeaseError('current-lease', 'invalid kernel cgroup path '+str(actual))
    record, directory = verified_record(parts[1][4:])
    values = set(record['cpus'])
    if (not cpus((actual/'cpuset.cpus.effective').read_text()).issubset(values)
            or not os.sched_getaffinity(0).issubset(values)):
        raise LeaseError('current-effective-cpuset', str(actual))
    parent = directory.parent
    protected(parent)
    if (cpus((parent/'cpuset.cpus.effective').read_text()) != values or
            (parent/'cpu.max').read_text().split() != [str(len(values)*100000), '100000']):
        raise LeaseError('current-effective-cpu-max', str(parent))
    return record


def retire(token, outcome=None):
    record, directory = verified_record(token, directory_required=False)
    # Only this root-custodied immutable token can name the retired subtree.
    if directory.exists():
        protected(directory)
        if populated(directory): (directory/'cgroup.kill').write_text('1')
        wait_empty(directory)
        result = record.get('retirement') or {
            **record, **read_stats(directory), 'empty': True, 'end': now(),
            'accounting': 'lease subtree total; nested receipts overlap and must not be added'}
        if outcome is not None:
            result = {**result, **outcome}
        elif 'exit_status' not in result:
            result = {**result, 'exit_status': None, 'termination': 'interrupted',
                      'payload_outcome': 'unavailable after lease-manager termination'}
        # Persist the empty subtree's final counters before removing anything.
        # A killed constructor can resume retirement even after its last rmdir.
        atomic_json(record_path(token), {**record, 'retirement': result}, 0o444)
        for child in sorted(directory.rglob('*'), key=lambda path: len(path.parts), reverse=True):
            if child.is_dir(): child.rmdir()
        directory.rmdir()
    else:
        result = record.get('retirement')
        if not result or result.get('id') != token or result.get('empty') is not True:
            raise LeaseError('retirement-evidence', 'missing final counters for '+token)
    # This receipt owns its final counters before the record/slot can retire.
    # A later occupant may overwrite the bounded slot, never this sidecar.
    meta=result.get('receipt_meta')
    if meta: atomic_json(meta+'.cpu-lease.terminal',result,0o444)
    atomic_json(CONTROL_ROOT/('slot-'+str(record['index'])+'.terminal'),result,0o444)
    record_path(token).unlink()
    return result


def observe(token, cancel=False):
    try:record, _ = verified_record(token,directory_required=False)
    except FileNotFoundError:record=None
    if record:
        manager = record['manager']; pid = manager['pid']
        if process_identity(pid) == manager['identity']:
            try: descriptor = os.pidfd_open(pid)
            except ProcessLookupError: descriptor = None
            if descriptor is not None:
                try:
                    if process_identity(pid) == manager['identity']:
                        if cancel: signal.pidfd_send_signal(descriptor, signal.SIGTERM)
                        select.select([descriptor], [], [])
                finally: os.close(descriptor)
    return retire(token) if record_path(token).exists() else completed(token)


def execute(index, token, tenant, argv):
    require_tenant(tenant,argv)
    if LIMIT_ENV not in os.environ:raise LeaseError('caller-rlimits','missing '+LIMIT_ENV)
    parse_limits(os.environ[LIMIT_ENV])
    slot = prepare(index)
    with open(CONTROL_ROOT/('slot-'+str(index)+'.lock'), 'a') as lease:
        fcntl.flock(lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
        for old in slot.glob('run-*'):
            if populated(old): raise LeaseError('slot-still-populated', str(old))
            if record_path(old.name[4:]).exists():retire(old.name[4:])
            else:
                # A killed constructor can leave an empty cgroup before its
                # record publication. No payload may enter before handoff.
                protected(old)
                for child in sorted(old.rglob('*'),key=lambda p:len(p.parts),reverse=True):
                    if child.is_dir():child.rmdir()
                old.rmdir()
        run = slot/('run-'+token); record_path(token); run.mkdir()
        values = cpu_layout()[index]; bound(run, values); enable(run)
        work = run/'work'; work.mkdir(); enable(work)
        leaf = work/'job'; leaf.mkdir()
        user = pwd.getpwnam('var')
        for file in [work, work/'cgroup.procs', work/'cgroup.subtree_control', work/'cgroup.threads', leaf]:
            os.chown(file, user.pw_uid, user.pw_gid)
        for file in leaf.iterdir():
            # Delegation never includes the root-owned slot/run ceiling.
            if file.name not in ('cgroup.kill',): os.chown(file, user.pw_uid, user.pw_gid)
        record = {'id': token, 'index': index, 'generation': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  'cpus': list(values), 'cgroup': str(run), 'tenant': tenant,
                  'tier': int(os.environ.get('BB_ADMIT_TIER','2')), 'cls': os.environ.get('BB_ADMIT_CLASS','compile'),
                  'receipt_meta': os.environ.get('BUILD_SLOT_RECEIPT_META'),
                  'admission_start': os.environ.get('BUILD_SLOT_ADMISSION_START'),
                  'queue_wait_seconds': os.environ.get('BB_ADMIT_WAIT_SECONDS'),
                  'exec_start': now(),
                  'manager': {'pid': os.getpid(), 'identity': process_identity(os.getpid())}}
        atomic_json(record_path(token), record, 0o444)
        env = dict(os.environ); env['BB_CPU_LEASE'] = token; env['BB_CGROUP'] = str(work)
        env['PATH']='/home/var/bin/bbcg:'+env.get('PATH','/usr/bin:/bin')
        requested = env.get('CARGO_BUILD_JOBS','')
        env['CARGO_BUILD_JOBS'] = str(min(len(values), int(requested))) if requested.isdigit() and int(requested)>0 else str(len(values))
        for key in ['RUST_TEST_THREADS', 'NEXTEST_TEST_THREADS', 'RAYON_NUM_THREADS']:
            value = env.get(key, '')
            env[key] = str(min(len(values), int(value))) if value.isdigit() and int(value)>0 else str(len(values))
        meta = env.get('BUILD_SLOT_RECEIPT_META')
        if meta: atomic_json(meta+'.cpu-lease', record, 0o444)
        def enter():
            (leaf/'cgroup.procs').write_text('0')
            restore_limits(env[LIMIT_ENV])
            os.setgroups(os.getgrouplist('var', user.pw_gid)); os.setgid(user.pw_gid); os.setuid(user.pw_uid)
            os.environ.update(USER='var', LOGNAME='var')
        child = None; result = 224; category = 'infra'
        def interrupted(number, frame): raise SystemExit(128+number)
        for number in (signal.SIGTERM, signal.SIGHUP, signal.SIGINT): signal.signal(number, interrupted)
        try:
            rendezvous=env.pop('BB_CPU_ADMISSION_SOCKET',None)
            if rendezvous:
                with socket.socket(socket.AF_UNIX,socket.SOCK_SEQPACKET) as connection:
                    connection.connect('\0'+rendezvous[1:] if rendezvous.startswith('@') else rendezvous)
                    if struct.unpack('3i',connection.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))[1]!=user.pw_uid:
                        raise LeaseError('admission-owner','ready peer does not own admission')
                    connection.send(token.encode())
                    if connection.recv(16)!=b'EXEC':raise LeaseError('admission-handoff','admission ended before durable dispatch')
            child = subprocess.Popen(argv, env={**env, 'USER':'var','LOGNAME':'var'}, preexec_fn=enter)
            result = child.wait()
            result = 128-result if result<0 else result
            category = 'interrupted' if result in (129,130,137,143) else 'command'
            wait_empty(run)
            return result
        except SystemExit as error:
            result = error.code; category = 'interrupted'
            raise
        finally:
            retire(token, {'exit_status': result, 'termination': category})


def launch(index, token, tenant, argv):
    return ['sudo','-n','--preserve-env','/usr/bin/python3','-B',str(Path(__file__).resolve()),
            'exec',str(index),token,tenant,'--',*argv]


def nested_scope(argv):
    owner = current(required=False)
    if owner is None: os.execv('/usr/bin/systemd-run',['/usr/bin/systemd-run',*argv])
    # This adapter preserves the existing heap-cgroup entry point. Unsupported
    # properties refuse inside a lease; they never fall through to host systemd.
    original = list(argv); memory = 'max'; quota = None; tasks = None; scope = False
    memory_properties = {}
    def property_value(value):
        nonlocal memory,quota,tasks
        key,separator,value = value.partition('=')
        if not separator: raise LeaseError('nested-property', original)
        if key=='MemoryMax': memory=value
        elif key=='CPUQuota': quota=value
        elif key=='TasksMax': tasks=value
        elif key in ('MemorySwapMax','MemoryHigh'):memory_properties[key]=value
        elif key!='Delegate': raise LeaseError('nested-property', original)
    while argv:
        value=argv.pop(0)
        if value=='--':break
        if value=='--scope':scope=True
        elif value in ('--user','-q','--quiet','--collect','-G','--same-dir'):pass
        elif value in ('-p','--property') and argv:property_value(argv.pop(0))
        elif value.startswith('--property='):property_value(value.split('=',1)[1])
        elif value.startswith('-p') and len(value)>2:property_value(value[2:])
        elif value.startswith('-'):raise LeaseError('nested-argument',value)
        else:argv.insert(0,value);break
    if not scope or not argv:raise LeaseError('nested-command','--scope and command required')
    def bytes_value(value):
        if value in ('max','infinity'):return 'max'
        power={'k':1,'m':2,'g':3,'t':4}.get(value[-1:].lower(),0)
        return str(int(value[:-1] if power else value)*(1024**power))
    work=Path(owner['cgroup'])/'work'; directory=work/('nested-'+uuid.uuid4().hex)
    directory.mkdir(); leaf=directory/'job'; leaf.mkdir()
    (directory/'memory.max').write_text(bytes_value(memory))
    for key,value in memory_properties.items():
        (directory/({'MemorySwapMax':'memory.swap.max','MemoryHigh':'memory.high'}[key])).write_text(bytes_value(value))
    if tasks is not None:(directory/'pids.max').write_text(tasks)
    if quota is not None:(directory/'cpu.max').write_text(str(int(float(quota.rstrip('%'))*1000))+' 100000')
    enable(directory)
    try:
        child=subprocess.Popen(argv,preexec_fn=lambda:(leaf/'cgroup.procs').write_text('0'))
        return child.wait()
    finally:
        if populated(directory):(directory/'cgroup.kill').write_text('1')
        wait_empty(directory)
        for item in sorted(directory.rglob('*'),key=lambda path:len(path.parts),reverse=True):
            if item.is_dir():item.rmdir()
        directory.rmdir()


if __name__ == '__main__':
    try:
        if sys.argv[1:2] == ['exec'] and sys.argv[5:6] == ['--']:
            raise SystemExit(execute(int(sys.argv[2]),sys.argv[3],sys.argv[4],sys.argv[6:]))
        elif sys.argv[1:2] == ['retire'] and len(sys.argv)==3:
            print(json.dumps(retire(sys.argv[2])))
        elif sys.argv[1:2] in (['observe'], ['cancel']) and len(sys.argv)==3:
            print(json.dumps(observe(sys.argv[2],sys.argv[1]=='cancel')))
        elif sys.argv[1:2] == ['scope']:raise SystemExit(nested_scope(sys.argv[2:]))
        elif sys.argv[1:] == ['work']:
            owner=current(required=False);print(str(Path(owner['cgroup'])/'work') if owner else '')
        elif sys.argv[1:] == ['current']: print(json.dumps(current()))
        else: raise LeaseError('command', 'exec, retire or current required')
    except (OSError,ValueError,KeyError,RuntimeError,subprocess.SubprocessError) as error:
        print(str(error) if isinstance(error, LeaseError) else str(LeaseError('kernel-boundary',error)),file=sys.stderr)
        raise SystemExit(224)
