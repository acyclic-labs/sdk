#!/usr/bin/env python3
"""Archive declared outputs before ephemeral targets are removed."""
import json
import os
from pathlib import Path
import tarfile

def prepare(spec, target):
    prefix=Path(spec['prefix'])
    if prefix.parent!=Path('/mnt/bb/jobs'):raise ValueError('invalid export prefix')
    output=Path(str(prefix)+'.exports');output.mkdir(mode=0o700,exist_ok=True)
    groups={}
    for raw in spec['paths']:
        p=Path(raw)
        if '..' in p.parts:raise ValueError('parent traversal in export')
        is_target=raw.startswith('/work/target/')
        if p.is_absolute() and not is_target:raise ValueError('export must be relative or under /work/target')
        base=Path(target) if is_target else Path(spec['cwd'])
        relative=Path(raw.removeprefix('/work/target/')) if is_target else p
        mapping=spec.get('targetMapping')
        if is_target and mapping and relative.parts[0] in mapping['profiles']:
            triple=mapping['triple']
            if not triple or '/' in triple or triple in ['.','..']:raise ValueError('invalid target triple')
            base/=triple
        groups.setdefault((base,is_target),[]).append(relative)
    manifest=[];start=Path(str(prefix)+'.t0').stat().st_mtime_ns
    for (base,is_target),paths in groups.items():
        files=set()
        for p in paths:
            absolute=base/p
            if absolute.is_symlink():raise ValueError('declared output is a symlink: '+str(absolute))
            if not absolute.exists():raise FileNotFoundError('missing declared output: '+str(absolute))
            if absolute.is_file():
                if is_target and spec['build'] or absolute.stat().st_mtime_ns>start:files.add(p)
            elif absolute.is_dir():
                for where,dirs,names in os.walk(absolute,followlinks=False):
                    dirs[:]=[d for d in dirs if not (Path(where)/d).is_symlink()]
                    for name in names:
                        file=Path(where)/name
                        if file.is_file() and not file.is_symlink() and file.stat().st_mtime_ns>start:files.add(file.relative_to(base))
        if not files:continue
        archive=output/f'{len(manifest)}.tar.gz'
        with tarfile.open(archive,'w:gz') as tar:
            for p in sorted(files):tar.add(base/p,arcname=str(p),recursive=False)
        manifest.append({'target':is_target,'archive':str(archive),'count':len(files)})
    tmp=output/'manifest.tmp';tmp.write_text(json.dumps(manifest));tmp.replace(output/'manifest.json')
