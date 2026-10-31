"""Prove the actual canonical consumer's refusal rules with unchanged tests."""
from pathlib import Path
from datetime import datetime,timezone
import hashlib,json,shutil,subprocess,sys,tempfile,time
ROOT=Path('[private-metadata-removed]');RECOVERY='python [private-path-removed]';step='preflight_exact_inputs'
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
try:
 names=['scripts/proof_export_join.py','scripts/test-proof-exports.py']+[p.relative_to(ROOT).as_posix() for p in (ROOT/'docs/proofs/evidence/canonical-exports-v1').rglob('*') if p.is_file()]
 for name in names:
  if subprocess.check_output(['git','show',':'+name],cwd=ROOT)!=(ROOT/name).read_bytes():raise ValueError('working/index byte mismatch: '+name)
 out=Path('[private-path-removed]')/('export-join-policy-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'));out.mkdir();shutil.copyfile(__file__,out/'producer.py')
 inputs={n:sha(ROOT/n) for n in names};(out/'source.json').write_bytes((json.dumps(inputs,indent=2)+'\n').encode())
 mutations={
 'omit-revision':("require(manifest['revision'] == revision, 'checked revision differs from generated exports')","require(True, 'revision check omitted')",'test_historical_bundle_cannot_qualify_current_revision_or_source'),
 'omit-source-bytes':("require(sha(local_file(source_root, name)) == expected, 'source input differs: ' + name)","require(True, 'source bytes check omitted')",'test_actual_source_bytes_are_checked'),
 'omit-required-profile-set':("require(set(profiles) == set(required_profiles), 'executed profiles differ from required profiles')","require(True, 'required profile check omitted')",'test_required_profiles_cannot_be_inferred_or_reduced'),
 'omit-availability-closure':("require(seen == set(items), 'canonical occurrence missing profile availability')","require(True, 'availability closure omitted')",'test_rehashed_missing_profile_availability_refuses'),
 'omit-profile-export-closure':("require({row['profile'] for row in joined} == set(required_profiles), 'required profile has zero joined exports')","require(True, 'profile export closure omitted')",'test_rehashed_profile_with_zero_joined_exports_refuses'),
 'drop-feature-signature':('return sorted(joined, key=lambda row:',"return sorted([row for row in joined if row['signature'] != 'pub fn demo::overloaded(value: &str) -> usize'], key=lambda row:",'test_real_feature_signatures_and_all_profiles_survive')}
 runs=[]
 with tempfile.TemporaryDirectory(prefix='verify-export-policy-') as directory:
  export=Path(directory)
  for name in names:
   p=export/name;p.parent.mkdir(parents=True,exist_ok=True);shutil.copyfile(ROOT/name,p)
  source=export/'scripts/proof_export_join.py';original=source.read_bytes()
  for label in ['positive',*mutations]:
   step=label;source.write_bytes(original)
   if label!='positive':
    before,after,witness=mutations[label]
    if original.count(before.encode())!=1:raise ValueError('mutation must select one production consumer site')
    source.write_bytes(original.replace(before.encode(),after.encode()));shutil.copyfile(source,out/(label+'.py'))
   started=time.monotonic();r=subprocess.run([sys.executable,'scripts/test-proof-exports.py'],cwd=export,capture_output=True,timeout=10);elapsed=time.monotonic()-started;raw=r.stdout+r.stderr;text=raw.decode();(out/(label+'.log')).write_bytes(raw)
   if elapsed>=10 or 'Ran 13 tests' not in text:raise ValueError('unexpected selection/runtime')
   if label=='positive':
    if r.returncode or not text.rstrip().endswith('OK'):raise ValueError('positive failed')
   elif r.returncode!=1 or 'FAILED (failures=1)' not in text or 'ERROR:' in text or witness not in text:raise ValueError('control did not fail its unchanged semantic assertion')
   runs.append(dict(label=label,exit=r.returncode,seconds=elapsed,selected=13,log_sha256=sha(out/(label+'.log')),implementation_sha256=sha(source)))
 for name,digest in inputs.items():
  if sha(ROOT/name)!=digest:raise ValueError('source changed while running: '+name)
 receipt=dict(kind='canonical_export_consumer_policy_only_not_api_or_runtime_qualification',source_manifest_sha256=sha(out/'source.json'),producer_sha256=sha(out/'producer.py'),runs=runs,mutations={k:dict(before=v[0],after=v[1],witness=v[2]) for k,v in mutations.items()})
 (out/'receipt.json').write_bytes((json.dumps(receipt,indent=2)+'\n').encode());print(json.dumps(dict(status='export_consumer_controls_pass',receipt=str(out/'receipt.json'),tests=13,mutants=len(mutations))))
except (OSError,ValueError,KeyError,TypeError,subprocess.SubprocessError) as e:
 print(f'VERIFY_EXPORT_POLICY_FAILURE step={step} error={e}; recovery: {RECOVERY}',file=sys.stderr);sys.exit(1)
