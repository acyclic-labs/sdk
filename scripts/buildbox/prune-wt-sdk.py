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
    if not root.is_dir():return {'removed':[],'remaining':0,'held':[],'over_cap':False}
    active=active_sources() if active is None else active;now=time.time() if now is None else now
    removed=[];held=[]
    with (locks/'prune.lock').open('a') as global_lock:
        fcntl.flock(global_lock,fcntl.LOCK_EX)
        # Retirement is a namespace, not a decoration on a source identity.
        # Both live and retired copies keep the original <source>.lock lease;
        # no filename grows during a failed removal or a later invocation.
        retirement=root/'.prune'
        if retirement.is_symlink():raise ValueError('unsafe retirement namespace')
        retirement.mkdir(exist_ok=True)
        groups={}
        for parent in [retirement,root]:
            for p in parent.iterdir():
                if p==retirement or not p.is_dir() or p.is_symlink():continue
                if p.name.startswith('.prune-'):
                    # Abandoned historical tombs have no reusable source name.
                    # Retire under their existing short, separate identity.
                    name=p.name.removeprefix('.prune-')
                else:name=p.name.removesuffix('.prep')
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
            retiring=any(p.parent==retirement or p.name.startswith('.prune-') for p in paths)
            rows.append((float('-inf') if retiring else max(stamps),name,paths))
        count=len(rows)
        for used,name,paths in sorted(rows):
            if now-used<=max_age and count<=max_count:continue
            with (locks/(name+'.lock')).open('a') as lease:
                try:fcntl.flock(lease,fcntl.LOCK_EX|fcntl.LOCK_NB)
                except BlockingIOError:held.append(name);continue
                if any(source==str(p) or source.startswith(str(p)+'/') for source in active for p in paths):
                    held.append(name);continue
                for p in paths:
                    # Namespace traversal is bounded to these two owned parents.
                    if p.parent not in [root,retirement] or p.resolve().parent!=p.parent:
                        raise ValueError('unsafe source path')
                    tomb=p if p.name.startswith('.prune-') else retirement/p.name
                    if p!=tomb:
                        # A failed prior generation is drained under the same
                        # original lease before another copy can retire here.
                        if tomb.exists():remove_tree(tomb)
                        try:p.rename(tomb)
                        except FileNotFoundError:continue
                    remove_tree(tomb)
                count-=1;removed.append(name)
        if not any(retirement.iterdir()):retirement.rmdir()
        return {'removed':removed,'remaining':count,'held':held,'over_cap':count>max_count}

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',type=Path,default=Path('/mnt/bb/wt-sdk'))
    parser.add_argument('--mirror',type=Path,default=Path('/home/var/mirror/sdk.git'))
    parser.add_argument('--locks',type=Path,default=Path('/mnt/bb/locks/wt-sdk'))
    parser.add_argument('--max-count',type=int,default=128)
    args=parser.parse_args()
    result=prune(args.root,args.mirror,args.locks,max_count=args.max_count)
    print('[buildbox] wt-sdk '+json.dumps(result),file=sys.stderr)
    if result['over_cap']:
        print('BB_DISK_LOW INFRA: SDK source-copy count cap held by active jobs',file=sys.stderr);sys.exit(224)
    if args.mirror.exists():subprocess.run(['git','-C',str(args.mirror),'worktree','prune'],check=True,stdout=sys.stderr)
