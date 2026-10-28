// Installed real native hosts against the shipping mock; no authentication fixture.
import assert from 'node:assert/strict';
import {randomUUID} from 'node:crypto';
import {mkdir,readFile,writeFile} from 'node:fs/promises';
import {dirname,delimiter,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Processes,Events} from '../../../scripts/graphcoder-e2e/process.mjs';
import {withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';
import {environment,scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {stagePlugin} from './package.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';

export function assertCodexChildRoundTrip(requests,childMarker='NATIVE_PLUGIN_CHILD_DONE') {
 const generations=requests.filter(row=>row.path==='responses');
 assert.ok(generations.every(row=>row.status===200),'unexpected native generation refusal');
 assert.deepEqual(generations.map(row=>row.step).sort(),[0,1,2,3],'native parent/child generation coverage');
 const consumed=generations.find(row=>row.step===2).body.input.find(item=>
  item.type==='function_call_output'&&item.call_id==='native_wait');
 assert.ok(consumed,'parent did not consume the plugin wait result');
 assert.ok(JSON.stringify(consumed).includes(childMarker),'native child answer missing from parent tool result');
 assert.ok(JSON.stringify(consumed).includes('Succeeded'),'native child did not settle successfully');
}

export async function nativeModel({host,binary,graphcoder,mock,native,output,packageSource}) {
 assert.ok(['codex','pi'].includes(host),'step=admit native host; recovery=select codex or pi');
 output=resolve(output);await mkdir(output);
 const root=scratchDirectory('installed-native-model-'),env=environment(join(root,'profile'));
 env.PATH=dirname(native)+delimiter+env.PATH;
 const workspace=join(root,'workspace');await mkdir(workspace);
 const scope=new Processes(),captures=[];const started=performance.now();let step='stage native package',result;
 const run=async(program,args,options={})=>{
  const record={program,args,...await scope.runOwned(program,args,{cwd:workspace,env,scopeMountRoot:root,...options})};
  captures.push(record);return record;
 };
 try {
  const receipt=await withOwnedCase({processes:scope,run:async()=>{
   const packageRoot=join(root,'package'),executable=await stagePlugin(packageSource,binary,packageRoot);
   step='install through shipping plugin';
   const settings=join(env.PI_CODING_AGENT_DIR,'extensions/acyclic.ts');
   const installation=await run(executable,['install',host,'--plugin-root',packageRoot,...(host==='pi'?['--settings',settings]:[])]);
   assert.equal(installation.code,0,installation.stderr);
   if(host==='codex') {
    const listed=await run(native,['plugin','list','--json']);assert.equal(listed.code,0,listed.stderr);
    assert.ok(JSON.parse(listed.stdout).installed.some(row=>row.pluginId==='acyclic@acyclic'&&row.enabled));
   } else assert.ok((await readFile(settings,'utf8')).includes(executable),'native extension does not bind admitted shipping binary');
   step='start shipping workspace owner';
   await startWorkspaceOwner({scope,binary:graphcoder,workspace,state:join(root,'state'),env,root,
    capture:record=>captures.push({role:'owner',...record})});
   step='start shipping mock model';
   const marker='NATIVE_PLUGIN_MODEL_DONE',script=join(output,'script.json');
   const childMarker='NATIVE_PLUGIN_CHILD_DONE';
   const tool=(name,args)=>({id:'native_'+name,namespace:'mcp__acyclic_sessions',name,arguments:args});
   const steps=host==='codex'?[
    {when:{user:'NATIVE_PLUGIN_MODEL'},tools:[tool('spawn',{reference:'native-child',prompt:'NATIVE_PLUGIN_CHILD'})]},
    {when:{tool_result:'inbox'},tools:[tool('wait',{reference:'native-child'})]},
    {when:{tool_result:childMarker},text:[marker]},
    {when:{user:'NATIVE_PLUGIN_CHILD'},text:[childMarker]},
   ]:[{when:{user:'NATIVE_PLUGIN_MODEL'},text:[marker]}];
   await writeFile(script,JSON.stringify({cases:{native:{steps}}}));
   const model=scope.start(mock,['--script',script,'--record',join(output,'requests.jsonl')],{env});model.child.stdin.end();
   const events=new Events(model.child.stdout,model.closed),{port}=await events.wait(event=>event.event==='ready');
   step='complete installed native model request';let args;
   if(host==='codex') {
    const initialized=await run('git',['init','--quiet']);assert.equal(initialized.code,0,initialized.stderr);
    // Open through the same workspace API as the native launcher. The runtime
    // supplies the task; the fixture never invents an agent or execution claim.
    const opened=await scope.run(executable,['__workspace'],{cwd:workspace,env},
     JSON.stringify({kind:'open',host:'codex',session:randomUUID()}));
    captures.push({role:'open-native-root',...opened});assert.equal(opened.code,0,opened.stderr);
    env.ACYCLIC_WORKSPACE=workspace;env.ACYCLIC_TASK=JSON.parse(opened.stdout).task;
    const policy=['--config','model_provider="mock"','--config','model_providers.mock.name="Local deterministic fixture"',
    '--config',`model_providers.mock.base_url="http://127.0.0.1:${port}/native/v1"`,'--config','model_providers.mock.wire_api="responses"',
    '--config','model_providers.mock.requires_openai_auth=false','--model','tiny',
    '--ask-for-approval','never','--sandbox','workspace-write','--add-dir',workspace];
    env.ACYCLIC_HOST_ARGS=JSON.stringify(policy);
    args=[...policy,'exec','--json','NATIVE_PLUGIN_MODEL'];
   }
   else {
    env.ACYCLIC_E2E_MODEL_URL=`http://127.0.0.1:${port}/native/v1`;
    args=['--mode','json','--print','--no-session','--no-skills','--no-prompt-templates','--no-themes',
     '--extension',fileURLToPath(new URL('./keyless-pi-provider.mjs',import.meta.url)),
     '--provider','fixture','--model','tiny','NATIVE_PLUGIN_MODEL'];
   }
   const completed=await run(native,args);assert.equal(completed.code,0,completed.stderr);
   assert.ok(completed.stdout.includes(marker),'native model answer missing');
   const requests=(await readFile(join(output,'requests.jsonl'),'utf8')).trim().split('\n').filter(Boolean).map(JSON.parse);
   const generation=requests.find(row=>row.path==='responses'&&row.status===200);assert.ok(generation,'missing actual model request');
   const tools=generation.body.tools;
   if(host==='codex') assert.ok(JSON.stringify(tools).includes('acyclic'),'installed Acyclic MCP tools not advertised');
   else for(const name of ['spawn','send','wait','archive','cancel','resume']) assert.ok(tools.some(t=>(t.name??t.function?.name)===name&&JSON.stringify(t).includes('separate native Pi process in an admitted workspace view')),'missing installed Pi coordination tool '+name);
   if(host==='codex') assertCodexChildRoundTrip(requests,childMarker);
   result={host,plugin_installed:true,native_model_answer:true,plugin_child_round_trip:host==='codex',advertised_tools:tools.map(t=>t.name??t.function?.name)};
  }});
  assert.equal(receipt.expired,false,'installed native host deadline');result={...result,status:'pass',seconds:receipt.seconds};
 } catch(error) {result={host,status:'fail',step,seconds:(performance.now()-started)/1000,error:error.message,recovery:'inspect original native model result and captures; run a changed-source diagnostic only'};}
 await writeFile(join(output,'captures.json'),JSON.stringify(captures,null,2));
 await writeFile(join(output,'result.json'),JSON.stringify(result,null,2));return result;
}

if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)) {
 const [host,binary,graphcoder,mock,native,output,packageSource]=process.argv.slice(2);
 try {const result=await nativeModel({host,binary,graphcoder,mock,native,output,packageSource});console.log(JSON.stringify(result));process.exitCode=result.status==='pass'?0:1;}
 catch(error){console.error('step=admit native model case: '+error.message+'; recovery=node plugin/tests/hosts/native-model.mjs HOST PLUGIN GRAPHCODER MOCK NATIVE FRESH_OUTPUT PACKAGE');process.exitCode=1;}
}
