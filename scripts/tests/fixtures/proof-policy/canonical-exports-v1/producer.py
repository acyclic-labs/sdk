"""Capture retained sdk-generation output; no compiler/provider fixture is authored."""
from pathlib import Path
import hashlib,json,shutil,sys
ROOT=Path('[private-metadata-removed]')
SRC=Path('D:/codex-sdk-docs-eb24-fe001/windows-lock-assertion-temp/sdk-generation-feature-profile-37848-22')
BUNDLE=SRC.parent/'sdk-generation-feature-profile-output-37848-23'
DEST=ROOT/'docs/proofs/evidence/canonical-exports-v1'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
try:
 m=json.loads((BUNDLE/'generation-manifest.v1.json').read_bytes()); captures=[]
 for kind,root,names in [('bundle',BUNDLE,['generation-manifest.v1.json',*m['artifacts']]),('source',SRC,m['sourceFiles'])]:
  for name in names:
   p=root/name
   expected=(m['artifacts'] if kind=='bundle' else m['sourceFiles']).get(name)
   if expected and expected!='sha256:'+sha(p):raise ValueError('retained bytes differ from actual producer manifest: '+str(p))
   target=DEST/kind/name;target.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(p,target)
   captures.append(dict(path=target.relative_to(DEST).as_posix(),origin=str(p),sha256=sha(p)))
 r=dict(kind='retained_real_sdk_generation_fixture_not_current_api_proof',revision=m['revision'],source_sha256=m['sourceSha256'],manifest_sha256=sha(BUNDLE/'generation-manifest.v1.json'),qualified_modes=[],captures=captures,provenance='Retained real Windows sdk-generation CLI regression output and exact six source inputs. Compiler rustdoc receipts and producer manifests preserved byte-for-byte. No current SDK revision or generator executable qualification inferred.')
 (DEST/'capture.json').write_bytes((json.dumps(r,indent=2)+'\n').encode());shutil.copyfile(__file__,DEST/'producer.py')
 print(json.dumps(dict(status='captured_real_generator_fixture',files=len(captures),manifest_sha256=r['manifest_sha256'])))
except (OSError,ValueError,KeyError) as e:
 print(f'VERIFY_EXPORT_CAPTURE_FAILURE step=capture_real_generator_artifacts error={e}; recovery: python [private-path-removed]',file=sys.stderr);sys.exit(1)
