// Deterministic native-host process pairs. Setup is outside each sample. This
// uses the installed scenario's ONE package/owner/provider/policy path and the
// shared process pump; it does not claim warm-turn or shell-execution timing.
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import {isAbsolute,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {recordedRequests,requests} from '../../../scripts/graphcoder-e2e/assertions.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {installedHost,launchNativeHost} from './native-model.mjs';
import {ProcessPhases,pairPhases} from './process-phases.mjs';

const digest=async path=>createHash('sha256').update(await readFile(path)).digest('hex');
const quote=text=>"'"+text.replaceAll("'","'\\''")+"'";
export async function nativePair({host,binary,mock,plugin,output,source}) {
 assert.ok(['codex','claude-code'].includes(host),'step=pair native host; recovery=select codex or claude-code; Pi keyless admission is unsupported');
 assert.match(source??'',/^[0-9a-f]{40}$/,'step=bind pair source; recovery=set E2E_SOURCE to the exact admitted source commit');
 for(const path of [binary,mock,plugin,output])assert.ok(isAbsolute(path),'step=admit pair paths; recovery=supply absolute GRAPHCODER MOCK_MODEL PLUGIN OUTPUT');
 const workspace=join(output,'workspace'),settings=join(output,'profile');
 await mkdir(workspace,{recursive:true});await mkdir(join(output,'transcripts'));
 const env=environment(settings),fixture={id:'native-process-pair',binary,workspace,env,output};
 env.E2E_PLUGIN_BINARY=plugin;
 let collector,server,events,pair;
 const scope=new Processes({transcripts:join(output,'transcripts'),onProgress:event=>collector?.observe(event)});
 const captures=[],samples=[];
 const json=(path,value)=>writeFile(path,JSON.stringify(value,null,2)+'\n');
 const prompt='PAIRED_NATIVE_SHELL',answer='PAIRED_NATIVE_DONE';
 const helper=fileURLToPath(new URL('./native-effects.mjs',import.meta.url));
 const command=[process.execPath,helper,'parent',workspace,host].map(quote).join(' ');
 const steps=[{when:{user:prompt},tools:[host==='codex'
  ?{id:'paired_shell',name:'exec_command',namespace:'functions',arguments:{cmd:command}}
  :{id:'paired_shell',name:'Bash',arguments:{command}}]},
  {when:{user:prompt,tool_result:'NATIVE_EFFECT_PARENT'},text:[answer]}];
 const scenario={input_tokens:64,steps};
 try {
  const result=await withOwnedCase({processes:scope,prepare:async()=>{
   await json(join(output,'script.json'),{cases:{bare:scenario,installed:scenario}});
   server=scope.start(mock,['--script',join(output,'script.json'),'--record',join(output,'requests.jsonl')],{env});
   events=new Spool(server.child.stdout,server.closed,{transcript:join(output,'events.jsonl'),
    key:event=>event.event==='ready'?['ready']:[]});
   env.E2E_MOCK_URL=`http://127.0.0.1:${(await events.keyed('ready')).port}/installed/v1`;
  },run:()=>installedHost({host,processes:scope,fixture,capture:record=>captures.push(record),tools:[],
   nativePermissions:['Bash'],prompt,answer,execute:async({native,configuration})=>{
    const binding={source,host,host_entry_sha256:await digest(native.entry),
     host_lock_sha256:await digest(fileURLToPath(new URL('./package-lock.json',import.meta.url))),
     artifacts:{graphcoder:await digest(binary),mock:await digest(mock),plugin:await digest(plugin)},
     workspace,profile:settings,prompt_sha256:createHash('sha256').update(prompt).digest('hex'),
     policy:host==='codex'?'never/workspace-write':'default/Bash',effect:'PARENT',scenario_sha256:createHash('sha256').update(JSON.stringify(scenario)).digest('hex')};
    await json(join(output,'binding.json'),binding);
    for(const mode of ['bare','installed']) {
     await rm(join(workspace,'parent.txt'),{force:true});
     const operation=randomUUID();collector=new ProcessPhases(operation);
     const legEnv={...configuration.env};
     const launch=await launchNativeHost(host,{...configuration,env:legEnv,
      url:env.E2E_MOCK_URL.replace('/installed/','/'+mode+'/'),installed:mode==='installed',input:true});
     legEnv.ACYCLIC_HOST_ARGS=JSON.stringify(launch.policy);
     // Direct launch is essential: the owned wrapper's stdin is a cancellation
     // control, not this host's prompt pipe. The caller must also retain its
     // enclosing physical-custody receipt; Node close cannot supply that fact.
     const completed=await scope.run(native.program,[...native.args,...launch.args],
      {cwd:workspace,env:legEnv,scopeOperation:operation},prompt+'\n');
     captures.push({mode,...completed});
     assert.equal(completed.code,0,completed.stderr);
     assert.ok(completed.stdout.includes(answer),'native terminal answer missing');
     const effect=await readFile(join(workspace,'parent.txt'),'utf8');assert.equal(effect,'PARENT');
     samples.push({mode,binding,effect,physical:collector.complete(completed),transcripts:completed.transcripts});
     collector=undefined;
    }
    pair=pairPhases(...samples);
   }})});
  assert.equal(result.expired,false,'original case envelope expired');
  server.child.kill('SIGTERM');await server.closed;
  const records=await recordedRequests(events,join(output,'requests.jsonl'));
  for(const mode of ['bare','installed'])requests({assert:{requests:2},model:{steps}},records.filter(row=>row.case===mode));
  await json(join(output,'pair.json'),{...pair,case:result,physical_custody:'requires enclosing original completion receipt',
   setup_and_case_seconds:result.total_seconds,requests:records});
  return pair;
 } finally {await scope.close();await json(join(output,'captures.json'),captures);}
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const [host,binary,mock,plugin,output,...extra]=process.argv.slice(2);
 try {
  assert.equal(extra.length,0);
  await nativePair({host,binary,mock,plugin,output,source:process.env.E2E_SOURCE});
 } catch(error) {
  console.error(`step=deterministic native pair: ${error.stack}; recovery=node plugin/tests/hosts/native-pairs.mjs HOST GRAPHCODER MOCK_MODEL PLUGIN NEW_OUTPUT with exact admitted E2E_SOURCE`);
  process.exitCode=1;
 }
}
