import assert from 'node:assert/strict';
import {test} from 'node:test';
import {scriptedScenario,runScriptedScenario} from './qualify-scripted.mjs';
import {mkdtemp,readFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';

test('scripted acceptance follows the original process and oracle independently of elapsed time',async t=>{
 const directory=await mkdtemp(join(tmpdir(),'scripted-completion-'));
 let clock=0;
 t.mock.method(performance,'now',()=>clock);
 class MeasuredProcesses extends Processes {
  async runOwned(...args){
   const result=await super.runOwned(...args);
   clock+=30_000;
   return result;
  }
 }
 const processes=new MeasuredProcesses();
 try{
  for(const [name,code,oracle,expired,accepted] of [
   ['completed',0,true,false,true],['exit-failure',3,true,false,false],
   ['oracle-failure',0,false,false,false],['cancelled',0,true,true,false],
  ]){
   const scenario={name,selection:name,args:['-c',`print('original output'); exit(${code})`],
    accepts:run=>oracle&&run.stdout==='original output\n'};
   const receipt=await runScriptedScenario({scenario,processes,directory,env:{PATH:process.env.PATH},
    expired:()=>expired});
   assert.equal(receipt.accepted,accepted,name);
   assert.equal(receipt.seconds,30);
   assert.equal(receipt.exit,code);
   assert.equal(receipt.expired,expired);
   assert.equal(await readFile(join(directory,name+'.log'),'utf8'),'original output\n');
   assert.equal(processes.children.size,0,'qualification joins original output and process retirement');
  }
 }finally{
  await processes.close();
  await rm(directory,{recursive:true,force:true});
 }
});

test('framing admits only its CLI artifact without Codex or GraphCoder',()=>{
 const artifacts={artifacts:{acyclic:{path:process.execPath},
  get graphcoder(){throw new Error('unselected GraphCoder dependency');}}};
 const scenario=scriptedScenario(artifacts,undefined,'managed:bounded-framing');
 assert.equal(scenario.selection,'managed:bounded-framing');
 assert.equal(scenario.args.length,2);
 assert.equal(scenario.args[1],process.execPath);
 assert.ok(scenario.args[0].endsWith('mcp-framing.py'));
});

test('scale does not require Codex; managed sandbox scenarios refuse its absence',()=>{
 const artifacts={artifacts:{acyclic:{path:process.execPath},graphcoder:{path:process.execPath}}};
 for(const host of ['codex','claude-code','pi'])
  assert.equal(scriptedScenario(artifacts,undefined,'scale:'+host).args.at(-1),host);
 for(const name of ['lifecycle','recursive','resume'])
  assert.throws(()=>scriptedScenario(artifacts,undefined,'managed:'+name),
   /step=admit managed sandbox executable; recovery:/);
});
