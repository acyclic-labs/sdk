#!/usr/bin/env python3
"""One prerequisite contract for dispatched and on-box jobs.

This admits bytes, not verdicts. The existing baseline/receipt owners still
qualify their results. Producers declare every reference consumed by the job;
opaque commands cannot infer an empty closure from their script text.
"""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shlex
import stat
import subprocess
import sys
import tempfile

ENV = 'BUILD_SLOT_INPUTS_JSON'
FILE_ENV = 'BUILD_SLOT_INPUTS_FILE'
ROOT_ENV = 'BUILD_SLOT_INPUT_ROOT'
BASE_ENV = 'BUILD_SLOT_INPUT_BASE'
HERE = Path(__file__).resolve()


class InputRefusal(RuntimeError):
    def __init__(self, name, detail, producer):
        super().__init__('kind=INPUT_CLOSURE step=prerequisite-admission input='+shlex.quote(name)+
                         ' detail='+shlex.quote(detail)+' owner=gate-speed recovery='+
                         shlex.quote(shlex.join(producer)))


def command(value):
    return isinstance(value, list) and bool(value) and all(isinstance(x,str) and x and '\0' not in x for x in value)


def parse(value):
    recovery = [sys.executable,'-B',str(HERE),'template','--output','.buildbox-inputs.json']
    if not isinstance(value,dict) or type(value.get('version')) is not int or value['version'] != 1 or value.get('complete') is not True or 'base_sha' not in value:
        raise InputRefusal('declaration','a reviewed complete version-1 input declaration is required',recovery)
    base = value.get('base_sha')
    if base is not None and (not isinstance(base,str) or not re.fullmatch('[0-9a-f]{40}',base)):
        raise InputRefusal('base_sha','expected a full Git commit or null for a non-repository command',recovery)
    rows = value.get('inputs')
    if not isinstance(rows,list):raise InputRefusal('inputs','expected an explicit list, including an explicit empty list',recovery)
    names=set()
    for row in rows:
        if not isinstance(row,dict) or not isinstance(row.get('id'),str) or not row['id'] or row['id'] in names:
            raise InputRefusal('inputs','input identifiers must be nonempty and unique',recovery)
        names.add(row['id'])
        if not command(row.get('producer')):
            raise InputRefusal(row['id'],'producer must be the exact targeted producer argv',recovery)
        if row.get('where') not in ('source','bb2') or not isinstance(row.get('path'),str) or not row['path']:
            raise InputRefusal(row['id'],'where must be source or bb2 and path must be nonempty',row['producer'])
        path=Path(row['path'])
        if row['where']=='source' and (path.is_absolute() or '..' in path.parts):
            raise InputRefusal(row['id'],'source inputs must stay inside the uploaded source root',row['producer'])
        # The declaration names the remote OS explicitly. Windows clients must
        # not reinterpret an absolute Linux reference as a drive-relative path.
        if row['where']=='bb2' and not PurePosixPath(row['path']).is_absolute():
            raise InputRefusal(row['id'],'bb2 references require an absolute immutable path',row['producer'])
        if not isinstance(row.get('sha256'),str) or not re.fullmatch('[0-9a-f]{64}',row['sha256']):
            raise InputRefusal(row['id'],'a producer-qualified SHA256 is required',row['producer'])
    return value


@contextmanager
def input_file(path):
    # One identity check on both OSes. Windows lacks O_NOFOLLOW; compare the
    # opened file's identity to lstat before/after, and reject reparse points.
    def regular(info):
        return stat.S_ISREG(info.st_mode) and not (getattr(info,'st_file_attributes',0) & getattr(stat,'FILE_ATTRIBUTE_REPARSE_POINT',0))
    path=Path(path);before=path.lstat()
    if not regular(before):raise ValueError('input is not a regular non-link file')
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_NOFOLLOW',0)|getattr(os,'O_NONBLOCK',0)|getattr(os,'O_BINARY',0))
    with os.fdopen(descriptor,'rb') as stream:
        opened=os.fstat(stream.fileno())
        if not regular(opened) or (before.st_dev,before.st_ino)!=(opened.st_dev,opened.st_ino):
            raise ValueError('input identity changed while opening')
        yield stream
        after=path.lstat()
        if not regular(after) or (after.st_dev,after.st_ino)!=(opened.st_dev,opened.st_ino):
            raise ValueError('input identity changed during admission')


def digest_file(path):
    with input_file(path) as stream:
        before=os.fstat(stream.fileno())
        if not stat.S_ISREG(before.st_mode):raise ValueError('input is not a regular file')
        digest=hashlib.file_digest(stream,'sha256').hexdigest()
        after=os.fstat(stream.fileno())
    if (before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)!=(after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):
        raise ValueError('input changed while being admitted')
    return digest


def validate(value, root, base, stage='bb2'):
    value=parse(value);root=Path(root).resolve()
    recovery=[sys.executable,'-B',str(HERE),'validate','--stage',stage,'--root',str(root),'--base',base or 'none']
    if value['base_sha'] != base:
        raise InputRefusal('base_sha','declared '+str(value['base_sha'])+' does not match admitted '+str(base),recovery)
    for row in value['inputs']:
        if row['where']=='bb2' and stage=='client':continue
        path=root/row['path'] if row['where']=='source' else Path(row['path'])
        try:
            if path.is_symlink() or (row['where']=='source' and not path.resolve().is_relative_to(root)):
                raise ValueError('input escapes source custody')
            # lstat before open prevents a missing/nonregular path from blocking.
            if not stat.S_ISREG(path.lstat().st_mode):raise ValueError('input is not a regular file')
            if digest_file(path)!=row['sha256']:raise ValueError('input SHA256 differs from declared producer output')
        except (OSError,ValueError) as error:
            raise InputRefusal(row['id'],str(error),row['producer']) from error
    canonical=json.dumps(value,sort_keys=True,separators=(',',':'))
    return {'version':1,'base_sha':base,'sha256':hashlib.sha256(canonical.encode()).hexdigest(),
            'inputs':len(value['inputs']),'stage':stage}


def source_base(root):
    clean={k:v for k,v in os.environ.items() if not k.startswith('GIT_')}
    result=subprocess.run(['git','-C',str(root),'merge-base','HEAD','origin/main'],capture_output=True,text=True,env=clean)
    return result.stdout.strip() if result.returncode==0 else None


def automatic(argv):
    # Direct tools have no driver-owned comparison inputs. Opaque interpreters
    # and executable scripts must provide their closure explicitly.
    name=Path(argv[0]).name if argv else ''
    return name in ('cargo','rustc','rustfmt','git','true','false','echo','cat','ls','pwd','id','uname','stat','sha256sum')


def declare(env, root=None, base=None, inputs=()):
    """Producer-side explicit closure for a job whose references are `inputs`.

    Hermetic fixtures and inline payloads call this instead of inheriting an
    enclosing receipt's source/base/reference declaration.
    """
    value={'version':1,'complete':True,'base_sha':base,'inputs':list(inputs)}
    parse(value)
    env.pop(FILE_ENV,None);env.pop(ROOT_ENV,None)
    env[ENV]=json.dumps(value,sort_keys=True,separators=(',',':'))
    env[BASE_ENV]=base or 'none'
    if root is not None:env[ROOT_ENV]=str(root)
    return env


def admit(argv, env=None, root=None, base=None, stage='bb2'):
    env=os.environ if env is None else env
    root=Path(root or (os.getcwd() if env.get(FILE_ENV) else env.get(ROOT_ENV)) or os.getcwd()).resolve()
    if base is None:base=env.get(BASE_ENV) or source_base(root)
    if base=='none':base=None
    raw=env.get(ENV)
    selected=env.get(FILE_ENV)
    if raw and selected:
        raise InputRefusal('declaration','two declarations are ambiguous',[sys.executable,'-B',str(HERE),'validate','--stage',stage])
    if not raw:
        if not selected:
            # A companion is convenient but still an explicit producer-owned
            # declaration, never a heuristic scan of shell/Python source.
            script=next((x for x in argv[1:] if x.endswith(('.sh','.py','.mjs','.js')) and '\n' not in x),None)
            if script and (root/(script+'.inputs.json')).is_file():selected=script+'.inputs.json'
        if selected:
            try:raw=(root/selected).read_text()
            except OSError as error:
                raise InputRefusal('declaration',str(error),[sys.executable,'-B',str(HERE),'template','--output',selected,'--base',base or 'none']) from error
        elif automatic(argv):raw=json.dumps({'version':1,'complete':True,'base_sha':base,'inputs':[]})
        else:
            raise InputRefusal('declaration','opaque jobs require their full reference closure before dispatch',
                               [sys.executable,'-B',str(HERE),'template','--output',(script+'.inputs.json') if script else '.buildbox-inputs.json','--base',base or 'none'])
    try:value=json.loads(raw)
    except (ValueError,TypeError) as error:
        raise InputRefusal('declaration','invalid JSON: '+str(error),[sys.executable,'-B',str(HERE),'template','--output',selected or '.buildbox-inputs.json','--base',base or 'none']) from error
    value=parse(value)
    for row in value['inputs']:
        if row['producer']==list(argv):raise InputRefusal(row['id'],'recovery must produce this input, not rerun the consuming job',
                                                         [sys.executable,'-B',str(HERE),'template','--output',selected or '.buildbox-inputs.json','--base',base or 'none'])
    proof=validate(value,root,base,stage)
    # Only canonical bytes cross the client/host and nested-command boundaries.
    env[ENV]=json.dumps(value,sort_keys=True,separators=(',',':'))
    env.pop(FILE_ENV,None)
    env[BASE_ENV]=base or 'none'
    env[ROOT_ENV]=str(root)
    return proof


def write(path,value):
    from admission_policy import atomic_json
    atomic_json(path,value)


def snapshot(source,destination,digest):
    destination.parent.mkdir(parents=True,exist_ok=True)
    fd,temporary=tempfile.mkstemp(dir=destination.parent)
    try:
        with os.fdopen(fd,'wb') as output:
            with input_file(source) as original:
                import shutil
                shutil.copyfileobj(original,output)
            output.flush();os.fsync(output.fileno())
        if digest_file(Path(temporary))!=digest:raise ValueError('producer output changed during snapshot')
        os.replace(temporary,destination)
    finally:Path(temporary).unlink(missing_ok=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='action',required=True)
    for action in ['validate', 'run']:
        check=sub.add_parser(action);check.add_argument('--stage',choices=['client','bb2'],default='bb2')
        check.add_argument('--root',default=os.getcwd());check.add_argument('--base');check.add_argument('argv',nargs=argparse.REMAINDER)
    for action in ['template','capture']:
        child=sub.add_parser(action);child.add_argument('--output',type=Path,required=True);child.add_argument('--base',default='none')
        if action=='capture':child.add_argument('--input',nargs=3,action='append',default=[],metavar=('ID','PATH','PRODUCER_JSON'))
    for action in ['restore','path']:
        child=sub.add_parser(action);child.add_argument('--manifest',type=Path,required=True);child.add_argument('--input',required=True)
    args=parser.parse_args()
    if args.action in ['validate', 'run']:
        argv=args.argv[1:] if args.argv[:1]==['--'] else args.argv
        if args.action == 'run' and not command(argv):
            raise InputRefusal('command', 'run requires the exact consuming argv',
                               [sys.executable, '-B', str(HERE), 'run', '--help'])
        proof=admit(argv or ['opaque-job'],root=args.root,base=args.base,stage=args.stage)
        if args.action == 'run':
            # One public boundary, including for callers whose installed host
            # generation predates automatic prerequisite admission. Never
            # contaminate a JSON/binary child's stdout with wrapper protocol.
            print('BB_INPUTS_ADMITTED '+json.dumps(proof,sort_keys=True),file=sys.stderr,flush=True)
            os.chdir(os.environ[ROOT_ENV])
            os.execvpe(argv[0],argv,os.environ)
        print(json.dumps({'proof':proof,'declaration':json.loads(os.environ[ENV])}))
    elif args.action=='template':
        # Incomplete by construction: generating a template cannot assert that
        # a job has no reference inputs. Its owner must review and complete it.
        write(args.output,{'version':1,'complete':False,'base_sha':None if args.base=='none' else args.base,'inputs':[]})
    elif args.action in ['restore','path']:
        value=parse(json.loads(args.manifest.read_text()))
        row=next((row for row in value['inputs'] if row['id']==args.input),None)
        if row is None:raise InputRefusal(args.input,'input is absent from declaration',[sys.executable,'-B',str(HERE),'--help'])
        path=args.manifest.resolve().parent/row['path']
        if args.action=='restore':
            if not row.get('origin'):raise InputRefusal(row['id'],'no captured source; use the targeted original producer',row['producer'])
            try:snapshot(Path(row['origin']),path,row['sha256'])
            except (OSError,ValueError) as error:raise InputRefusal(row['id'],str(error),row['producer']) from error
        print(path)
    else:
        rows=[];root=args.output.resolve().parent
        for name,file,producer in args.input:
            producer=json.loads(producer)
            if producer==[]:producer=[sys.executable,'-B',str(HERE),'restore','--manifest',str(args.output.resolve()),'--input',name]
            if not command(producer):raise InputRefusal(name,'targeted producer argv is required',[sys.executable,'-B',str(HERE),'--help'])
            try:
                digest=digest_file(Path(file))
                # Existing historical test-anchor policy inspects the commit stem.
                directory=root/'.input-closure'/digest
                if not directory.resolve().is_relative_to(root):raise ValueError('snapshot directory escapes source custody')
                directory.mkdir(parents=True,exist_ok=True)
                # Windows gate logs contain CRLF. Their admitted bytes must
                # survive Git's source transport without line-ending filters.
                attributes=directory.parent/'.gitattributes'
                if attributes.is_symlink():raise ValueError('snapshot attributes escape source custody')
                snapshot_bytes=b'* -text\n'
                # Publish filters atomically too; another capture must never
                # observe an empty filter while Git hashes the proof files.
                fd,temporary=tempfile.mkstemp(dir=attributes.parent)
                try:
                    with os.fdopen(fd,'wb') as output:
                        output.write(snapshot_bytes);output.flush();os.fsync(output.fileno())
                    os.replace(temporary,attributes)
                finally:Path(temporary).unlink(missing_ok=True)
                destination=directory/Path(file).name
                snapshot(Path(file),destination,digest)
            except (OSError,ValueError) as error:raise InputRefusal(name,str(error),producer) from error
            rows.append({'id':name,'path':destination.relative_to(root).as_posix(),'where':'source',
                         'sha256':digest,'producer':producer,'origin':str(Path(file).resolve())})
        value={'version':1,'complete':True,'base_sha':None if args.base=='none' else args.base,'inputs':rows}
        validate(value,root,value['base_sha'],'client');write(args.output,value)
        print(json.dumps({row['id']:str(root/row['path']) for row in rows}))
    return 0


if __name__=='__main__':
    try:raise SystemExit(main())
    except (InputRefusal,OSError,ValueError) as error:
        if not isinstance(error,InputRefusal):error=InputRefusal('declaration',str(error),[sys.executable,'-B',str(HERE),'--help'])
        print(str(error),file=sys.stderr);raise SystemExit(224)
