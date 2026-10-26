"""Real entry-point prerequisites, using captured plugins/base evidence."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PureWindowsPath
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import input_closure as inputs

HERE=Path(__file__).resolve().parent
FIXTURE=HERE/'fixtures/input-closure'
BASE='b7333953cbd5d3b31adc05cf2f0851f049f6c16a'


class InputClosureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        for row in json.loads((FIXTURE/'origin.json').read_text(encoding='utf-8-sig'))['files']:
            data=(FIXTURE/row['file']).read_bytes()
            if len(data)!=row['bytes'] or hashlib.sha256(data).hexdigest()!=row['sha256']:
                raise AssertionError('captured source changed: '+row['file'])

    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.clean_env={k:v for k,v in os.environ.items() if not k.startswith('BUILD_SLOT_INPUT') and not k.startswith('GIT_')}
        self.reference=self.root/'.bb-plugins-consolidated-main-clippy/qualified.log'
        self.reference.parent.mkdir()
        shutil.copyfile(HERE/'coordinator/fixtures/baseline-admission/main-clippy.txt',self.reference)
        self.producer=['pwsh','-NoProfile','-File','Q:/lanes/bs.ps1','bash','.bb-plugins-consolidated-main-clippy.sh']
        self.value={'version':1,'complete':True,'base_sha':BASE,'inputs':[
            {'id':'main-clippy','where':'source','path':self.reference.relative_to(self.root).as_posix(),
             'sha256':inputs.digest_file(self.reference),'producer':self.producer}]}

    def env(self,value=None):
        return {**self.clean_env,inputs.ENV:json.dumps(self.value if value is None else value),
                inputs.ROOT_ENV:str(self.root),inputs.BASE_ENV:BASE}

    def load(self,name,file):
        spec=importlib.util.spec_from_file_location(name,HERE/file)
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

    def test_public_run_refuses_missing_or_changed_input_before_any_child(self):
        marker=self.root/'launched'
        child=[sys.executable,'-c','from pathlib import Path; Path("launched").write_text("executed")']
        runner=[sys.executable,'-B',str(HERE/'input_closure.py'),'run','--root',str(self.root),'--base',BASE,'--',*child]
        original=self.reference.read_bytes()
        for missing in [True,False]:
            if missing:self.reference.unlink()
            else:self.reference.write_bytes(original+b'changed')
            started=time.monotonic()
            refused=subprocess.run(runner,cwd=self.root,env=self.env(),capture_output=True)
            self.assertEqual(refused.returncode,224,refused.stderr)
            self.assertLess(time.monotonic()-started,1)
            self.assertIn(b'kind=INPUT_CLOSURE step=prerequisite-admission input=main-clippy',refused.stderr)
            self.assertIn(b'recovery=',refused.stderr)
            self.assertEqual(refused.stdout,b'')
            self.assertFalse(marker.exists())
            # Actual former entry point: launching the same command directly
            # does not consult the missing/changed comparison prerequisite.
            subprocess.run(child,cwd=self.root,env=self.clean_env,check=True)
            self.assertTrue(marker.exists());marker.unlink()

    def test_windows_client_admits_captured_absolute_bb2_path_without_reinterpreting_it(self):
        # Real plugins refusal, captured in the owner's 20261026T030800Z steer.
        value=copy.deepcopy(self.value)
        value['inputs'][0].update(where='bb2',path='/mnt/bb/wt-sdk/plugins-f2e372f6.1-1792980493426-30567-b8e274b1/.bb-plugins-retirement-ready/shipping/managed/bin/acyclic')
        with patch.object(inputs,'Path',PureWindowsPath):
            self.assertEqual(inputs.parse(value),value)
        old_source=(HERE/'input_closure.py').read_text().replace(
            "not PurePosixPath(row['path']).is_absolute()",'not path.is_absolute()')
        old={'__file__':str(HERE/'input_closure.py'),'__name__':'old_input_path_control'}
        exec(compile(old_source,'old-input-path-control','exec'),old)
        old['Path']=PureWindowsPath
        with self.assertRaisesRegex(inputs.InputRefusal.__bases__[0],'bb2 references require an absolute immutable path'):
            old['parse'](value)
        for path in ['relative/input','C:/borrowed-client-path']:
            value['inputs'][0]['path']=path
            with self.assertRaises(inputs.InputRefusal):inputs.parse(value)

    def test_public_run_preserves_child_bytes_status_and_canonical_declaration(self):
        child=[sys.executable,'-c',
               'import os,sys,json; assert "BUILD_SLOT_INPUTS_FILE" not in os.environ; '
               'assert json.loads(os.environ["BUILD_SLOT_INPUTS_JSON"])["base_sha"]==sys.argv[1]; '
               'os.write(1,b"{\\\"child\\\":true}\\n\\x00"); os.write(2,b"child-stderr\\n"); sys.exit(int(sys.argv[2]))',BASE]
        manifest=self.root/'inputs.json';manifest.write_text(json.dumps(self.value))
        env={**self.clean_env,inputs.FILE_ENV:str(manifest)}
        for status in [0,23]:
            result=subprocess.run([sys.executable,'-B',str(HERE/'input_closure.py'),'run',
                                   '--root',str(self.root),'--base',BASE,'--',*child,str(status)],
                                  cwd=self.root,env=env,capture_output=True)
            self.assertEqual(result.returncode,status,result.stderr)
            self.assertEqual(result.stdout,b'{"child":true}\n\x00')
            self.assertIn(b'BB_INPUTS_ADMITTED ',result.stderr)
            self.assertTrue(result.stderr.endswith(b'child-stderr\n'))

    def test_deleted_plugins_reference_refuses_before_native_admission_in_under_a_second(self):
        source=(FIXTURE/'.bb-plugins-lifecycle-final.sh.txt').read_text()
        self.assertLess(source.index('cargo test --locked'),source.index('final-attested-clippy'))
        self.assertIn('.bb-plugins-consolidated-main-clippy/qualified.log',source)
        self.assertIn('1302.030s rc4',(FIXTURE/'missing-prerequisite-alert.txt').read_text())
        self.reference.unlink()
        admission=self.load('input_admission','bb-admit.py')
        for negative in [False,True]:
            calls=[]
            # The mutation removes only the new admission check. Reaching the
            # scheduler is recorded; no real shared admission authority is used.
            with patch.dict(os.environ,self.env(),clear=True),patch.object(admission,'validate_root'),\
                 patch.object(admission.cpu_leases,'current',side_effect=lambda **kw:(calls.append('compile-dispatch'),(_ for _ in ()).throw(RuntimeError('recorded scheduler boundary')))[1]):
                started=time.monotonic()
                with patch.object(admission.input_closure,'admit',return_value={}) if negative else __import__('contextlib').nullcontext():
                    with self.assertRaises(RuntimeError) as failure:
                        admission.client(['lane','graphcoder','--','bash','.bb-plugins-lifecycle-final.sh'])
                elapsed=time.monotonic()-started
            self.assertEqual(calls,['compile-dispatch'] if negative else [])
            self.assertLess(elapsed,1.0,'missing prerequisite must not queue or compile')
            if not negative:
                self.assertIn('input=main-clippy',str(failure.exception))
                self.assertIn('recovery=',str(failure.exception))
                self.assertIn('.bb-plugins-consolidated-main-clippy.sh',str(failure.exception))
                self.assertNotIn('recovery=\'bash .bb-plugins-lifecycle-final.sh',str(failure.exception))
                print('INPUT_REFUSAL_SECONDS '+str(round(elapsed,6)),file=sys.stderr)

    def test_runtime_entry_refuses_before_cpu_cache_or_child(self):
        runtime=self.load('input_runtime','bb-job.py');self.reference.unlink()
        with patch.dict(os.environ,self.env(),clear=True),patch.object(runtime.cpu_leases,'current',side_effect=AssertionError('CPU acquisition must not happen')):
            self.assertEqual(runtime.run(['bash','candidate.sh']),224)

    def test_client_entry_refuses_missing_file_and_accepts_complete_closure_once(self):
        # Execute the real shared JavaScript client adapter, not a second parser.
        generator=self.root/'generator.mjs'
        generator.write_text((HERE/'buildbox.mjs').read_text()+'\nexport {admitInputs};\n')
        for name in ['buildbox-receipt.mjs','input_closure.py','admission_policy.py']:
            shutil.copyfile(HERE/name,self.root/name)
        code="import {admitInputs} from './generator.mjs';import fs from 'node:fs';if(!admitInputs(['bash','candidate.sh'],process.cwd(),'"+BASE+"'))process.exit(224);fs.appendFileSync('compiled','dispatch\\n');"
        for present in [False,True]:
            if not present:self.reference.unlink()
            else:shutil.copyfile(HERE/'coordinator/fixtures/baseline-admission/main-clippy.txt',self.reference)
            started=time.monotonic()
            result=subprocess.run(['node','--input-type=module','-e',code],cwd=self.root,env=self.env(),capture_output=True,text=True)
            self.assertEqual(result.returncode,0 if present else 224,result.stdout+result.stderr)
            if not present:
                self.assertLess(time.monotonic()-started,1.0)
                self.assertFalse((self.root/'compiled').exists())
                self.assertIn('input=main-clippy',result.stderr)
        self.assertEqual((self.root/'compiled').read_text(),'dispatch\n')
        # A broken policy interpreter/module must still give the owner a
        # typed step and recovery command, rather than a bare traceback.
        (self.root/'input_closure.py').write_text('raise RuntimeError("policy load failed")\n')
        broken=subprocess.run(['node','--input-type=module','-e',code],cwd=self.root,env=self.env(),capture_output=True,text=True)
        self.assertEqual(broken.returncode,224,broken.stdout+broken.stderr)
        self.assertIn('kind=INPUT_CLOSURE step=policy-exec',broken.stderr)
        self.assertIn('recovery=',broken.stderr)
        self.assertEqual((self.root/'compiled').read_text(),'dispatch\n')
        # Guard ordering is part of the real production source, before replay,
        # remote host selection and all source/candidate dispatch.
        source=(HERE/'buildbox.mjs').read_text()
        guard=source.index('const prerequisites = admitInputs')
        self.assertLess(guard,source.index('const k = verdictKey',guard))
        self.assertLess(guard,source.index('await pickHost()',guard))

    def test_base_hash_schema_and_targeted_producer_are_all_required(self):
        variants=[]
        for mutate in [lambda v:v.update(base_sha='0'*40),lambda v:v.pop('base_sha'),lambda v:v.update(complete=False),
                       lambda v:v['inputs'].append(copy.deepcopy(v['inputs'][0])),lambda v:v['inputs'][0].update(sha256='0'*64),
                       lambda v:v['inputs'][0].update(path='../outside'),lambda v:v['inputs'][0].update(producer=[]),
                       lambda v:v['inputs'][0].update(producer=['bash','candidate.sh'])]:
            value=copy.deepcopy(self.value);mutate(value);variants.append(value)
        for value in variants:
            with self.subTest(value=value),self.assertRaises(inputs.InputRefusal):
                inputs.admit(['bash','candidate.sh'],self.env(value),self.root,BASE,'client')
        inputs.admit(['bash','candidate.sh'],self.env(),self.root,BASE,'client')

    def test_missing_and_generated_incomplete_declarations_cannot_infer_empty_closure(self):
        with self.assertRaises(inputs.InputRefusal):inputs.admit(['bash','candidate.sh'],dict(self.clean_env),self.root,BASE)
        manifest=self.root/'candidate.sh.inputs.json'
        subprocess.run([sys.executable,'-B',str(HERE/'input_closure.py'),'template','--output',str(manifest),'--base',BASE],check=True,env=self.clean_env)
        with self.assertRaises(inputs.InputRefusal):inputs.admit(['bash','candidate.sh'],dict(self.clean_env),self.root,BASE)
        self.assertFalse(json.loads(manifest.read_text())['complete'])

    def test_bb2_only_reference_is_checked_before_host_queue_and_rechecked_after_wait(self):
        value=copy.deepcopy(self.value);value['inputs'][0].update(where='bb2',path=str(self.reference))
        self.reference.unlink()
        inputs.admit(['bash','candidate.sh'],self.env(value),self.root,BASE,'client')
        with self.assertRaises(inputs.InputRefusal):inputs.admit(['bash','candidate.sh'],self.env(value),self.root,BASE,'bb2')
        shutil.copyfile(HERE/'coordinator/fixtures/baseline-admission/main-clippy.txt',self.reference)
        inputs.admit(['bash','candidate.sh'],self.env(value),self.root,BASE,'bb2')
        self.reference.write_text('changed during queue wait')
        with self.assertRaises(inputs.InputRefusal):inputs.admit(['bash','candidate.sh'],self.env(value),self.root,BASE,'bb2')

    def test_snapshot_is_the_compared_input_and_restore_never_rebuilds(self):
        manifest=self.root/'job.inputs.json'
        result=subprocess.run([sys.executable,'-B',str(HERE/'input_closure.py'),'capture','--output',str(manifest),'--base',BASE,
                               '--input','clippy',str(self.reference),'[]'],capture_output=True,text=True,env=self.clean_env)
        self.assertEqual(result.returncode,0,result.stderr)
        value=json.loads(manifest.read_text());snapshot=Path(json.loads(result.stdout)['clippy'])
        self.assertNotEqual(snapshot,self.reference)
        self.assertEqual(snapshot.name,self.reference.name)
        inputs.validate(value,self.root,BASE)
        snapshot.unlink()
        with self.assertRaises(inputs.InputRefusal):inputs.validate(value,self.root,BASE)
        restored=subprocess.run(value['inputs'][0]['producer'],capture_output=True,text=True,env=self.clean_env)
        self.assertEqual(restored.returncode,0,restored.stderr)
        self.reference.unlink();inputs.validate(value,self.root,BASE)
        self.assertFalse((self.root/'compiled').exists())

    def test_real_metadata_stdout_stays_json_without_compilation(self):
        (self.root/'Cargo.toml').write_text('[package]\nname="input-closure-query"\nversion="0.0.0"\nedition="2021"\n')
        (self.root/'src').mkdir();(self.root/'src/lib.rs').write_text('pub fn source() {}\n')
        env=dict(self.clean_env);inputs.admit(['cargo','metadata','--format-version','1','--no-deps'],env,self.root,None)
        result=subprocess.run(['cargo','metadata','--format-version','1','--no-deps'],cwd=self.root,env=env,capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(json.loads(result.stdout)['packages'][0]['name'],'input-closure-query')
        with self.assertRaises(json.JSONDecodeError):json.loads('BB_INPUTS_ADMITTED bad stdout\n'+result.stdout)
        self.assertFalse((self.root/'target').exists())

    def test_ignored_declared_proof_is_part_of_the_real_uploaded_git_identity(self):
        for args in [['init','-q'],['-c','commit.gpgsign=false','-c','user.name=Fixture','-c','user.email=fixture@example.invalid','commit','--allow-empty','-qm','input fixture']]:
            subprocess.run(['git',*args],cwd=self.root,env=self.clean_env,check=True)
        (self.root/'.gitignore').write_text('*.log\n')
        # The production factory has a host cache path. Relocate only that
        # storage root (BUILD_SLOT_CLIENT_CACHE), retaining actual Git/index/ignore/upload code.
        cache=self.root/'client-cache';cache.mkdir()
        source=(HERE/'buildbox.mjs').read_text()
        (self.root/'generator.mjs').write_text(source+'\nexport {treeIdentity};\n')
        shutil.copyfile(HERE/'buildbox-receipt.mjs',self.root/'buildbox-receipt.mjs')
        probe="import {treeIdentity} from './generator.mjs';console.log(JSON.stringify(treeIdentity({top:process.cwd()})));"
        result=subprocess.run(['node','--input-type=module','-e',probe],cwd=self.root,env={**self.env(),'BUILD_SLOT_CLIENT_CACHE':str(cache)},capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        value=json.loads(result.stdout);file=self.value['inputs'][0]['path']
        self.assertIn(file,value['files']);self.assertIn(file,value['forced'])
        # Actual former identity omitted ignored proof files; delete only the
        # new force-add to execute that old path against the same real repo.
        (self.root/'generator.mjs').write_text(source.replace("if (inputFiles.length) g(['add', '-f', '--', ...inputFiles], env);",'')+'\nexport {treeIdentity};\n')
        previous=subprocess.run(['node','--input-type=module','-e',probe],cwd=self.root,env={**self.env(),'BUILD_SLOT_CLIENT_CACHE':str(cache)},capture_output=True,text=True)
        self.assertEqual(previous.returncode,0,previous.stderr)
        self.assertNotIn(file,json.loads(previous.stdout)['files'])

    def test_captured_native_log_bytes_survive_windows_git_filters(self):
        raw=(FIXTURE/'main-native-prefix.bin').read_bytes();self.assertIn(b'\r\n',raw)
        reference=self.root/(BASE+'.log');reference.write_bytes(raw)
        for args in [['init','-q'],['config','core.autocrlf','true']]:
            subprocess.run(['git',*args],cwd=self.root,env=self.clean_env,check=True)
        manifest=self.root/'job.inputs.json'
        result=subprocess.run([sys.executable,'-B',str(HERE/'input_closure.py'),'capture','--output',str(manifest),'--base',BASE,
                               '--input','tests',str(reference),'[]','--input','features',str(reference),'[]'],capture_output=True,text=True,env=self.clean_env)
        self.assertEqual(result.returncode,0,result.stderr)
        captured=json.loads(result.stdout);self.assertEqual(captured['tests'],captured['features'],'identical proofs share one snapshot')
        snapshot=Path(captured['tests']);self.assertEqual(snapshot.read_bytes(),raw)
        self.assertEqual(snapshot.stem,BASE,'historical-anchor identity must survive capture')
        def blob(filtered):
            argv=['git','hash-object',*(['--path',str(snapshot.relative_to(self.root))] if filtered else ['--no-filters']),str(snapshot)]
            return subprocess.check_output(argv,cwd=self.root,env=self.clean_env).strip()
        self.assertEqual(blob(True),blob(False))
        (self.root/'.input-closure/.gitattributes').unlink()
        self.assertNotEqual(blob(True),blob(False),'former snapshot without binary custody changes the real native log bytes')

    def test_neutral_file_policy_without_linux_flags_or_resource_keeps_identity_custody(self):
        # Exercise the actual neutral factory with the APIs Windows lacks.
        # Linux-only rlimit admission still refuses instead of returning {}.
        with patch.dict(sys.modules,{'resource':None}):
            policy=self.load('neutral_input_policy','admission_policy.py')
            with self.assertRaises(ModuleNotFoundError):exec('import resource',{})
        destination=self.root/'atomic.json'
        with patch.object(os,'fchmod',None),patch.object(os,'O_DIRECTORY',None):
            policy.atomic_json(destination,{'complete':True})
        self.assertEqual(json.loads(destination.read_text()),{'complete':True})
        with self.assertRaisesRegex(RuntimeError,'RLIMIT_UNAVAILABLE'):policy.capture_limits()
        expected=hashlib.sha256(self.reference.read_bytes()).hexdigest()
        link=self.root/'proof-link';link.symlink_to(self.reference)
        other=self.root/'other.log';other.write_text('substituted')
        real_open=os.open
        def replace_before_open(path,flags,*args,**kwargs):
            self.reference.unlink();self.reference.symlink_to(other)
            return real_open(path,flags,*args,**kwargs)
        with patch.dict(os.__dict__):
            os.__dict__.pop('O_NOFOLLOW');os.__dict__.pop('O_NONBLOCK')
            self.assertEqual(inputs.digest_file(self.reference),expected)
            with self.assertRaises(ValueError):inputs.digest_file(link)
            with self.assertRaises(AttributeError):exec('os.O_NOFOLLOW',{'os':os})
            with patch.object(os,'open',side_effect=replace_before_open):
                with self.assertRaisesRegex(ValueError,'identity changed'):inputs.digest_file(self.reference)
            # The former fallback of merely opening a path accepts the swap.
            self.assertEqual(self.reference.read_bytes(),b'substituted')


if __name__=='__main__':unittest.main(verbosity=2)
