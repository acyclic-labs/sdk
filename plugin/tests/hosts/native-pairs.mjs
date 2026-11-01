// Native-host process pairs. Setup is outside each sample. This
// uses the installed scenario's ONE package/owner/provider/policy path and the
// shared process pump; it does not claim warm-turn or shell-execution timing.
import assert from 'node:assert/strict';
import {createHash,randomUUID} from 'node:crypto';
import {appendFile,mkdir,readFile,rm,writeFile} from 'node:fs/promises';
import {isAbsolute,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {recordedRequests,requests} from '../../../scripts/graphcoder-e2e/assertions.mjs';
import {environment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {assertNativeCompletion,installedHost,launchNativeHost} from './native-model.mjs';
import {ProcessPhases,pairOrders,pairPhases} from './process-phases.mjs';
import {assertNativeResume} from './native-session-proof.mjs';

const digest=async path=>createHash('sha256').update(await readFile(path)).digest('hex');
const quote=text=>"'"+text.replaceAll("'","'\\''")+"'";
export async function nativePair({host,binary,mock,plugin,output,source,artifactSource=source,order,live}) {
 assert.ok(order?.length===2&&((order[0]==='bare'&&order[1]==='installed')||
  (order[0]==='installed'&&order[1]==='bare')),
  'step=admit native pair order; recovery=supply one bare and one installed leg');
 assert.ok(['codex','claude-code',...(live?['pi']:[])].includes(host),'step=pair native host; recovery=select codex or claude-code, or Pi with an explicit supported native login');
 assert.match(source??'',/^[0-9a-f]{40}$/,'step=bind pair source; recovery=set E2E_SOURCE to the exact admitted source commit');
 assert.match(artifactSource??'',/^[0-9a-f]{40}$/,'step=bind pair artifact source; recovery=set E2E_ARTIFACT_SOURCE to the original executable source');
 for(const path of [binary,...(live?[]:[mock]),plugin,output])assert.ok(isAbsolute(path),'step=admit pair paths; recovery=supply absolute GRAPHCODER MOCK_MODEL PLUGIN OUTPUT');
 const workspace=join(output,'workspace'),settings=join(output,'profile');
 await mkdir(workspace,{recursive:true});await mkdir(join(output,'transcripts'));
 const env=environment(settings),fixture={id:'native-process-pair',binary,workspace,env,output,live};
 env.E2E_PLUGIN_BINARY=plugin;
 let collector,server,events,pair,setupSeconds;
 const scope=new Processes({transcripts:join(output,'transcripts'),onProgress:event=>collector?.observe(event)});
 const captures=[],samples=[];
 const json=(path,value)=>writeFile(path,JSON.stringify(value,null,2)+'\n');
 const answer='PAIRED_NATIVE_DONE';
 const helper=fileURLToPath(new URL('./native-effects.mjs',import.meta.url));
 const command=[process.execPath,helper,'parent',workspace,host].map(quote).join(' ');
 const prompt=live?`Run exactly this command once with your native shell tool: ${command}\nAfter it succeeds with NATIVE_EFFECT_PARENT, reply ${answer}. Do not use coordination tools, delegate, or inspect credentials.`:'PAIRED_NATIVE_SHELL';
 const steps=[{when:{user:prompt},tools:[host==='codex'
  ?{id:'paired_shell',name:'exec_command',namespace:'functions',arguments:{cmd:command}}
  :{id:'paired_shell',name:'Bash',arguments:{command}}]},
  {when:{user:prompt,tool_result:'NATIVE_EFFECT_PARENT'},text:[answer]}];
 const scenario={input_tokens:64,steps},setupOrigin=performance.now();
 try {
  const result=await withOwnedCase({processes:scope,prepare:async()=>{
   if(live)return;
   await json(join(output,'script.json'),{cases:{bare:scenario,installed:scenario}});
   server=scope.start(mock,['--script',join(output,'script.json'),'--record',join(output,'requests.jsonl')],{env});
   events=new Spool(server.child.stdout,server.closed,{transcript:join(output,'events.jsonl'),
    key:event=>event.event==='ready'?['ready']:[]});
   fixture.url=`http://127.0.0.1:${(await events.keyed('ready')).port}/installed/v1`;
  },run:()=>installedHost({host,processes:scope,fixture,capture:record=>captures.push(record),tools:[],
   nativePermissions:['Bash'],prompt,answer,execute:async({native,configuration,run})=>{
    const version=await run(native.program,[...native.args,'--version']);assert.equal(version.code,0,version.stderr);
    const binding={source,artifact_source:artifactSource,host,platform:process.platform,architecture:process.arch,host_entry_sha256:await digest(native.entry),
     host_version:version.stdout.trim(),
     host_lock_sha256:await digest(fileURLToPath(new URL('./package-lock.json',import.meta.url))),
     artifacts:{graphcoder:await digest(binary),mock:live?null:await digest(mock),plugin:await digest(plugin)},
     provider:live?.provider??'mock',model:live?.model??'tiny',
     workspace,profile:live?.profile??settings,prompt_sha256:createHash('sha256').update(prompt).digest('hex'),
     policy:host==='codex'?'never/workspace-write':'default/Bash',effect:'PARENT',scenario_sha256:createHash('sha256').update(JSON.stringify(scenario)).digest('hex')};
    await json(join(output,'binding.json'),binding);
    setupSeconds=(performance.now()-setupOrigin)/1000;
    let pluginInstalled=true;
    for(const mode of order) {
     // CLI config overrides do not remove the pinned host's advertised plugin
     // tools. Use its original plugin manager, outside both measured launches.
     if(host==='codex'&&pluginInstalled!==(mode==='installed')) {
      const changed=mode==='bare'
       ?await run(native.program,[...native.args,'plugin','remove','acyclic@acyclic','--json'])
       :await run(plugin,['install','codex','--plugin-root',configuration.packageRoot]);
      assert.equal(changed.code,0,changed.stderr);
      pluginInstalled=mode==='installed';
     }
     if(live&&host==='codex'&&mode==='bare') {
      const listed=await run(native.program,[...native.args,'plugin','list','--json']);assert.equal(listed.code,0,listed.stderr);
      assert.deepEqual(JSON.parse(listed.stdout).installed.filter(row=>row.enabled).map(row=>row.pluginId),
       [],'step=admit paired native plugins; recovery=use an exclusive native login profile with no unrelated enabled plugins');
     }
     await rm(join(workspace,'parent.txt'),{force:true});
     const operation=randomUUID();collector=new ProcessPhases(operation);
     const legEnv={...configuration.env};
     const launch=await launchNativeHost(host,{...configuration,env:legEnv,
      url:live?undefined:fixture.url.replace('/installed/','/'+mode+'/'),installed:mode==='installed',input:true});
     legEnv.ACYCLIC_HOST_ARGS=JSON.stringify(launch.policy);
     // Direct launch is essential: the owned wrapper's stdin is a cancellation
     // control, not this host's prompt pipe. The caller must also retain its
     // enclosing physical-custody receipt; Node close cannot supply that fact.
     const completed=await scope.run(native.program,[...native.args,...launch.args],
      {cwd:workspace,env:legEnv,scopeOperation:operation},prompt+'\n');
     captures.push({mode,...completed});
     assertNativeCompletion(host,completed,answer);
     if(live&&host==='claude-code') {
      // The original startup record describes the actual admitted plugin set.
      // Missing metadata cannot qualify a bare leg by absence of evidence.
      const initialized=completed.stdout.split('\n').filter(Boolean).map(JSON.parse)
       .find(event=>event.type==='system'&&event.subtype==='init');
      assert.ok(initialized,'native Claude initialization missing');
      assert.equal(initialized.model,live.model,'native Claude model differs from approved selection');
      assert.deepEqual(initialized.plugins.map(plugin=>plugin.name),mode==='installed'?['acyclic']:[],
       'native Claude plugin set differs from paired mode');
      assert.deepEqual(initialized.mcp_servers.map(server=>({name:server.name,status:server.status})),
       mode==='installed'?[{name:'plugin:acyclic:acyclic-sessions',status:'connected'}]:[],
       'native Claude MCP connections differ from paired mode');
     }
     const effect=await readFile(join(workspace,'parent.txt'),'utf8');assert.equal(effect,'PARENT');
     samples.push({mode,binding,effect,physical:collector.complete(completed),transcripts:completed.transcripts});
     collector=undefined;
    }
    pair=pairPhases(...samples);
   }})});
  assert.equal(result.expired,false,'original case envelope expired');
  let records=null;
  if(!live) {
   server.child.kill('SIGTERM');await server.closed;
   records=await recordedRequests(events,join(output,'requests.jsonl'));
   for(const mode of ['bare','installed'])requests({assert:{requests:2},model:{steps}},records.filter(row=>row.case===mode));
   const namespace=host==='codex'?'mcp__acyclic_sessions':'mcp__plugin_acyclic_acyclic-sessions__';
   assert.ok(JSON.stringify(records.filter(row=>row.case==='installed')).includes(namespace),'installed native model never observed original coordination namespace');
   assert.ok(!JSON.stringify(records.filter(row=>row.case==='bare')).includes(namespace),'bare native model still observed plugin coordination');
  }
  await json(join(output,'pair.json'),{...pair,case:result,physical_custody:'requires enclosing original completion receipt',
   fixture_setup_seconds:setupSeconds,setup_and_case_seconds:result.total_seconds,requests:records});
  return pair;
 } finally {
  try {await scope.close();}
  finally {
   await json(join(output,'captures.json'),captures);
   await json(join(output,'phases.json'),{samples,current:collector?.snapshot()});
  }
 }
}

// One installed live coding attempt, using the same host admission/install,
// policy, process custody and filesystem/native-session oracle as mock cases.
// Whole-workflow duration is deliberately not a warm-turn latency sample.
export async function nativeWorkflow({host,binary,plugin,output,source,artifactSource=source,live}) {
 assert.ok(live,'step=admit live workflow; recovery=supply an approved native login configuration');
 assert.match(source??'',/^[0-9a-f]{40}$/,'step=bind workflow source; recovery=supply exact E2E_SOURCE');
 assert.match(artifactSource??'',/^[0-9a-f]{40}$/,'step=bind workflow artifacts; recovery=supply exact E2E_ARTIFACT_SOURCE');
 for(const path of [binary,plugin,output])assert.ok(isAbsolute(path),'step=admit workflow paths; recovery=supply absolute executable and output paths');
 const workspace=join(output,'workspace');await mkdir(workspace,{recursive:true});
 await mkdir(join(output,'transcripts'));
 const env=environment(join(output,'profile'));env.E2E_PLUGIN_BINARY=plugin;
 const fixture={id:'native-live-workflow',binary,workspace,env,output,live};
 const scope=new Processes({transcripts:join(output,'transcripts')}),captures=[];
 const helper=fileURLToPath(new URL('./native-effects.mjs',import.meta.url));
 const command=phase=>[process.execPath,helper,phase,workspace,host].map(quote).join(' ');
 const answer='NATIVE_PLUGIN_MODEL_DONE',tools=['spawn','wait','resume','archive'];
 const child=`Run exactly this command with your native shell tool: ${command('child')}\nAfter it succeeds reply NATIVE_PLUGIN_CHILD_DONE. Do not inspect credentials.`;
 const resumed=`Run exactly this command with your native shell tool: ${command('resumed')}\nAfter it succeeds reply NATIVE_PLUGIN_CHILD_RESUMED. Do not inspect credentials.`;
 const prompt=`Use your native shell and installed Acyclic coordination tools to perform these steps sequentially. Do not use built-in subagents, impersonate a child, or inspect credentials.
1. Run ${command('parent')} in your native shell.
2. Call the installed Acyclic spawn tool with reference native-child and prompt ${JSON.stringify(child)}.
3. Call the installed Acyclic wait tool for native-child and require its successful NATIVE_PLUGIN_CHILD_DONE result.
4. Run ${command('merge')} in your native shell and require success.
5. Call the installed Acyclic resume tool with the SAME reference native-child and prompt ${JSON.stringify(resumed)}.
6. Call the installed Acyclic wait tool for native-child and require its successful NATIVE_PLUGIN_CHILD_RESUMED result.
7. Run ${command('finish')} in your native shell and require success.
8. Archive native-child using the installed Acyclic tool, then reply ${answer}.`;
 let outcome,failure;
 try {
  await writeFile(join(output,'binding.json'),JSON.stringify({source,artifact_source:artifactSource,host,platform:process.platform,architecture:process.arch,
   model:live.model,provider:live.provider,profile:live.profile,
   artifacts:{graphcoder:await digest(binary),plugin:await digest(plugin)},
   prompt_sha256:createHash('sha256').update(prompt).digest('hex')},null,2)+'\n');
  outcome=await withOwnedCase({processes:scope,run:()=>installedHost({host,processes:scope,fixture,
   capture:record=>captures.push(record),tools,nativePermissions:['Bash'],prompt,answer})});
  assert.equal(outcome.expired,false,'original installed workflow envelope expired');
  for(const [file,bytes] of [['parent.txt','PARENT'],['child.txt','CHILD'],['resumed.txt','RESUMED']])
   assert.equal(await readFile(join(workspace,file),'utf8'),bytes,'original native effect missing: '+file);
  const session=async phase=>JSON.parse(await readFile(join(workspace,'.volumes/scratch',`native-${phase}-session.json`),'utf8'));
  assertNativeResume(await session('initial'),await session('resumed'));
  return outcome;
 } catch(error) {failure={code:error.code??error.cause?.code??'NATIVE_WORKFLOW_FAILED',message:error.message};throw error;}
 finally {
  try {await scope.close();}
  finally {
   await writeFile(join(output,'captures.json'),JSON.stringify(captures,null,2)+'\n');
   await writeFile(join(output,'workflow.json'),JSON.stringify({host,model:live.model,result:failure?'FAIL':'PASS',
    outcome,failure,warm_turn_pairs:0,physical_custody:'requires enclosing original completion receipt'},null,2)+'\n');
  }
 }
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const [host,binary,mock,plugin,output,count,...extra]=process.argv.slice(2);
 try {
  assert.ok(extra.length===0||((extra.length===2||extra.length===3)&&extra[0]==='--live'
   &&(extra.length===2||extra[2]==='--workflow')),
   'step=admit pair arguments; recovery=supply HOST GRAPHCODER MOCK_MODEL PLUGIN NEW_OUTPUT PAIRS [--live NONSECRET_NATIVE_LOGIN_JSON [--workflow]]');
  const live=extra.length?JSON.parse(await readFile(extra[1],'utf8')):undefined;
  for(const {index,order} of pairOrders(Number(count))) {
   const directory=`pair-${index}`;
   await nativePair({host,binary,mock,plugin,output:join(output,directory),order,
    source:process.env.E2E_SOURCE,artifactSource:process.env.E2E_ARTIFACT_SOURCE??process.env.E2E_SOURCE,live});
   // Raw samples stay in their own original pair directory. Append only the
   // completed pair's index; never retain earlier captures or phase objects.
   await appendFile(join(output,'pairs.jsonl'),JSON.stringify({index,order,pair:`${directory}/pair.json`})+'\n');
  }
  if(extra[2]==='--workflow')await nativeWorkflow({host,binary,plugin,output:join(output,'workflow'),
   source:process.env.E2E_SOURCE,artifactSource:process.env.E2E_ARTIFACT_SOURCE??process.env.E2E_SOURCE,live});
 } catch(error) {
  console.error(`step=native pairs: ${error.stack}; recovery=node plugin/tests/hosts/native-pairs.mjs HOST GRAPHCODER MOCK_MODEL PLUGIN NEW_OUTPUT PAIRS [--live NONSECRET_NATIVE_LOGIN_JSON] with exact admitted E2E_SOURCE`);
  process.exitCode=1;
 }
}
