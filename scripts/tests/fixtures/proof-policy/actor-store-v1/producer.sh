#!/usr/bin/env bash
set -euo pipefail
trap 'rc=$?; if [ "$rc" -ne 0 ]; then echo "STEP actors-store-producer failed ($rc); recovery command: bash .bb-a4-store.sh" >&2; fi' EXIT
python3 - <<'PY'
from pathlib import Path
import hashlib,json,subprocess,tarfile,time
prefix='.bb-a4-store-'
recovery='bash .bb-a4-store.sh'
changed=['rust/crates/actors/Cargo.toml','rust/crates/actors/src/lib.rs','rust/crates/actors/src/guest.rs','rust/crates/actors/src/guest/handler.rs','rust/crates/actors/src/local.rs','rust/crates/actors/src/local/tests.rs']
paths=set(filter(None,subprocess.check_output(['git','ls-files','-z']).decode().split('\0')))|set(changed)
sources={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in sorted(paths) if Path(p).is_file()}
Path(prefix+'sources.json').write_text(json.dumps(sources,sort_keys=True))
def command(name,args,expected=0,needle=None):
 start=time.monotonic();log=Path(prefix+name+'.log')
 with log.open('w') as out:r=subprocess.run(args,stdout=out,stderr=subprocess.STDOUT)
 receipt={'command':args,'exit':r.returncode,'expected':expected,'seconds':time.monotonic()-start}
 Path(prefix+name+'.json').write_text(json.dumps(receipt));print(name,json.dumps(receipt),flush=True)
 if r.returncode!=expected or (needle and needle not in log.read_text()):
  raise RuntimeError(f'STEP {name} failed: {log.read_text()[-7000:]}; recovery command: {recovery}')
original=None
try:
 command('inputs',['cargo','metadata','--no-deps','--locked','--format-version=1'])
 command('format',['rustfmt','--edition','2024','--config','skip_children=true',*changed[1:]])
 with tarfile.open(prefix+'formatted.tar.gz','w:gz') as archive:
  for p in changed:archive.add(p)
 formatted={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in changed}
 Path(prefix+'formatted.json').write_text(json.dumps(formatted,sort_keys=True))
 command('positive',['cargo','test','--locked','-p','acyclic-actors','--all-features','--lib','local::tests','--','--nocapture'])
 path=Path('rust/crates/actors/src/local.rs');original=path.read_text()
 controls=[('epoch', '.filter(|state| state.epoch == epoch)', '.filter(|_| epoch != 0)', 'epoch fencing', 'memory_epoch_alarm_pause_fences'),
 ('alarm', 'state.alarm.generation != *generation', '(state.alarm.generation == u64::MAX && *generation == u64::MAX)', 'alarm generation fencing', 'memory_epoch_alarm_pause_fences')]
 for name,old,new,needle,test in controls:
  if original.count(old)!=1:raise RuntimeError(f'STEP {name} mutation site changed; recovery command: {recovery}')
  path.write_text(original.replace(old,new))
  command(name+'-negative',['cargo','test','--locked','-p','acyclic-actors','--all-features','--lib',test,'--','--nocapture'],101,needle)
  path.write_text(original)
 old='conditions.push(condition);'
 if original.count(old)!=1:raise RuntimeError(f'STEP split mutation site changed; recovery command: {recovery}')
 split='''if !mutations.is_empty() {
            self.provider.commit(CommitRequest {
                conditions: std::mem::take(&mut conditions),
                mutations: std::mem::take(&mut mutations),
                idempotency_key: IdempotencyKey::new([key.as_bytes(), b"/split"].concat())?,
            }).await?;
        }
        '''
 path.write_text(original.replace(old,split+old))
 command('split-negative',['cargo','test','--locked','-p','acyclic-actors','--all-features','--lib','memory_atomic_history','--','--nocapture'],101,'one checkpoint/effect transaction')
finally:
 if original is not None:path.write_text(original)
 Path(prefix+'restored.json').write_text(json.dumps({p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for p in changed},sort_keys=True))
 with tarfile.open(prefix+'evidence.tar.gz','w:gz') as archive:
  for p in Path('.').glob(prefix+'*'):
   if p.suffix in ['.log','.json'] or p.name==prefix+'formatted.tar.gz':archive.add(p)
 archive=Path(prefix+'evidence.tar.gz')
 print('EXPORT '+str(archive)+' SHA256 '+hashlib.sha256(archive.read_bytes()).hexdigest(),flush=True)
PY
