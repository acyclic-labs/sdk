#!/usr/bin/env python3
"""Bound SDK source copies by last use and count, never evict a leased copy."""
import argparse
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

def remove_tree(path):
    # Test fixtures and tools can leave unreadable directories. Make each
    # directory traversable before descending; never follow a directory link.
    path.chmod(path.stat().st_mode | 0o700)
    for base,dirs,_ in os.walk(path,followlinks=False):
        for name in dirs:
            child=Path(base)/name
            if not child.is_symlink():child.chmod(child.stat().st_mode | 0o700)
    shutil.rmtree(path)

def active_sources(proc=Path('/proc')):
    active=set()
    for p in proc.iterdir():
        if not p.name.isdigit():continue
        try:active.add(os.readlink(p/'cwd'))
        except OSError:pass
        try:
            for entry in (p/'environ').read_bytes().split(b'\0'):
                if entry.startswith(b'BB_HOST_SRC='):active.add(os.fsdecode(entry.split(b'=',1)[1]))
        except OSError:pass
    return active

def prune(root,mirror,locks,max_age=43200,max_count=128,active=None,now=None):
    root=root.resolve();mirror=mirror.resolve();locks.mkdir(parents=True,exist_ok=True)
    if not root.is_dir():return {'removed':[],'remaining':0,'held':[],'errors':[],'over_cap':False}
    active=active_sources() if active is None else active;now=time.time() if now is None else now
    removed=[];held=[];errors=[]
    with (locks/'prune.lock').open('a') as global_lock:
        fcntl.flock(global_lock,fcntl.LOCK_EX)
        groups={}
        for p in root.iterdir():
            if p.is_dir() and not p.is_symlink():
                # Retirement retains the original lease identity. A failed
                # removal must not grow another .prune- prefix on every call.
                name=p.name.removeprefix('.prune-').removesuffix('.prep')
                groups.setdefault(name,[]).append(p)
        rows=[]
        for name,paths in groups.items():
            metadata=mirror/'worktrees'/name
            # A legacy manual pruner may have removed a copy meanwhile.
            paths=[p for p in paths if p.exists()]
            if not paths:continue
            stamps=[p.stat().st_mtime for p in paths]
            for file in [metadata/'index',metadata/'HEAD']:
                try:stamps.append(file.stat().st_mtime)
                except FileNotFoundError:pass
            rows.append((max(stamps),name,paths))
        count=len(rows)
        for used,name,paths in sorted(rows):
            if now-used<=max_age and count<=max_count:continue
            try:
                with (locks/(name+'.lock')).open('a') as lease:
                    try:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
                    except BlockingIOError:held.append(name);continue
                    if any(source==str(p) or source.startswith(str(p)+'/') for source in active for p in paths):
                        held.append(name);continue
                    for p in paths:
                        # Only immediate, resolved children can be renamed/deleted.
                        if p.parent!=root or p.resolve().parent!=root:raise ValueError('unsafe source path')
                        tomb=p if p.name.startswith('.prune-') else root/('.prune-'+p.name)
                        if p!=tomb:
                            try:p.rename(tomb)
                            except FileNotFoundError:continue
                        remove_tree(tomb)
                    count-=1;removed.append(name)
            except OSError as error:
                # A failed tomb remains evidence under its original lease.
                # Keep retiring other copies; admission judges its own source.
                errors.append({'source':name,'error':str(error),'errno':error.errno})
        return {'removed':removed,'remaining':count,'held':held,'errors':errors,'over_cap':count>max_count}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('/mnt/bb/wt-sdk'))
    parser.add_argument('--mirror',type=Path,default=Path('/home/var/mirror/sdk.git'))
    parser.add_argument('--locks',type=Path,default=Path('/mnt/bb/locks/wt-sdk'))
    parser.add_argument('--max-count',type=int,default=128)
    args=parser.parse_args()
    result=prune(args.root,args.mirror,args.locks,max_count=args.max_count)
    print('[buildbox] wt-sdk '+json.dumps(result),file=sys.stderr)
    if result['errors']:
        print('BB_WT_PRUNE_FAILED '+json.dumps(result['errors']),file=sys.stderr)
    if result['over_cap']:
        print('BB_DISK_LOW INFRA: SDK source-copy count cap held by active jobs',file=sys.stderr);sys.exit(224)
    if args.mirror.exists():subprocess.run(['git','-C',str(args.mirror),'worktree','prune'],check=True,stdout=sys.stderr)
