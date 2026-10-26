// Credential-free Linux process fixtures. Managed-model qualification is owned
// by the coordinator; these exercise the production adapters and native sandbox.
import assert from 'node:assert/strict';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {stageArtifacts} from '../../../scripts/graphcoder-e2e/artifacts.mjs';
import {Processes} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment,scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
const here=fileURLToPath(new URL('.',import.meta.url));
export const scriptedSelections=Object.freeze([
  ...['lifecycle','recursive','resume','bounded-framing'].map(name=>'managed:'+name),
  ...['codex','claude-code','pi'].map(name=>'scale:'+name),
]);

// The caller admits the artifact closure once. Descriptors contain only native
// scenario arguments and the corresponding oracle, never profile/budget policy.
export function scriptedScenario(artifacts,codex,selection){
 assert.ok(scriptedSelections.includes(selection),
   'step=select scripted host scenario; recovery: select '+scriptedSelections.join('|'));
 const [kind,name]=selection.split(':');
 const binary=artifacts.artifacts.acyclic.path;
 if(name==='bounded-framing')return {name,selection,
  args:[join(here,'mcp-framing.py'),binary],
  accepts:run=>JSON.parse(run.stdout).cases.length===9};
 const graphcoder=artifacts.artifacts.graphcoder.path;
 if(kind==='scale')return {name,selection,
  args:[join(here,'shared-mount-scale.py'),binary,graphcoder,name],
  accepts:run=>run.stdout.includes('"sessions_alive_at_both_samples": true')};
 assert.ok(codex,'step=admit managed sandbox executable; recovery: supply Codex 0.164 or later for '+selection);
 return {name,selection,
  args:[join(here,'managed-session.py'),binary,graphcoder,resolve(codex),name],
  accepts:run=>run.stdout.includes('"suite": "managed-native-session-'+name+'"')};
}

// E2E callers provide their existing process scope, environment and cancellation
// observation. Exactly one scenario runs; no nested staging, profile or timer.
export async function runScriptedScenario({scenario,processes,directory,env,
  cwd=process.cwd(),expired=()=>false}){
 const started=performance.now();
 const run=await processes.runOwned('python3',scenario.args,{cwd,env,scopeMountRoot:env.HOME});
 const seconds=(performance.now()-started)/1000;
 await writeFile(join(directory,scenario.name+'.log'),run.stdout+run.stderr,{flag:'wx'});
 const receipt={schema:'acyclic.plugin.scripted-scenario/v1',selection:scenario.selection,
   name:scenario.name,seconds,exit:run.code,expired:expired(),
   accepted:!expired()&&seconds<10&&run.code===0&&scenario.accepts(run)};
 return receipt;
}

async function main(){
 const [cargo,output,hostOrMode,selected,...extra]=process.argv.slice(2);
 const mode=selected??hostOrMode,codex=selected?hostOrMode:undefined;
 const recovery='node plugin/tests/hosts/qualify-scripted.mjs CARGO_JSON FRESH_OUTPUT [CODEX] managed|scale|SELECTED_SCENARIO';
 assert.ok(cargo&&output&&!extra.length&&
   (['managed','scale'].includes(mode)||scriptedSelections.includes(mode)),
   'step=admit scripted host qualification; recovery: '+recovery);
 const selections=scriptedSelections.includes(mode)?[mode]:scriptedSelections.filter(name=>name.startsWith(mode+':'));
 assert.ok(codex||selections.every(name=>name==='managed:bounded-framing'||name.startsWith('scale:')),
  'step=admit managed sandbox executable; recovery: '+recovery);
 const directory=resolve(output);await mkdir(directory);
 const records=await readFile(cargo,'utf8');
 const binaries=selections.every(name=>name==='managed:bounded-framing')?['acyclic']:['acyclic','graphcoder'];
 const artifacts=await stageArtifacts(binaries.map(name=>({
   name,target:name,test:false,records,
 })),join(directory,'bin'));
 const receipts=[];
 for(const selection of selections){
  const entry=scriptedScenario(artifacts,codex,selection);
  const home=scratchDirectory('fixture-'+entry.name+'-');
  const env=environment(home,{PATH:process.env.PATH,RUST_TEST_THREADS:'4'});
  const scope=new Processes();
  const {run,seconds,expired}=await withOwnedCase({processes:scope,
   run:()=>runScriptedScenario({scenario:entry,processes:scope,directory,env})});
  const receipt={...run,seconds,expired,accepted:run.accepted&&!expired&&seconds<10};
  receipts.push(receipt);await writeFile(join(directory,'results.json'),JSON.stringify(receipts,null,2));
  console.log(JSON.stringify(receipt));
 }
 assert.ok(receipts.every(row=>row.accepted),
  'step=qualify scripted host adapters; recovery: '+recovery);
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))await main();
