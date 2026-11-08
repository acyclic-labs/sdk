import assert from 'node:assert/strict';
import {parseArgs} from 'node:util';
import {existsSync,mkdirSync,mkdtempSync,readFileSync,readdirSync,rmSync,symlinkSync,writeFileSync} from 'node:fs';
import {join,resolve,dirname,relative,delimiter} from 'node:path';
import {pathToFileURL,fileURLToPath} from 'node:url';
import {fixtureEnvironment,workspaceOwner,checked,run,start} from './native-fixture.mjs';
import {Spool} from '../../../scripts/graphcoder-e2e/process.mjs';

export function codexSandbox(binary,workspace){
 return [binary,'sandbox','-c','sandbox_mode="workspace-write"','-c','approval_policy="never"','-c',
  'sandbox_workspace_write.writable_roots='+JSON.stringify([workspace]),'--'];
}
export async function piCommand(binary,command,{cwd,env,prefix=[]}){
 const [executable,...args]=[...prefix,binary,'--mode','rpc','--no-session','--no-skills','--no-prompt-templates','--no-themes'];
 const native=start(executable,args,{cwd,env});
 const output=new Spool(native.child.stdout,native.closed,{key:message=>message.id==='sandbox'&&message.type==='response'?['completed']:[],unique:true});
 try{
  native.child.stdin.write(JSON.stringify({id:'sandbox',type:'bash',command})+'\n');
  const response=await output.keyed('completed');
  assert.equal(response.command,'bash');assert.ok(response.success,JSON.stringify(response));assert.equal(response.data.exitCode,0,JSON.stringify(response));
  native.child.stdin.end();assert.equal((await native.closed).code,0,native.stderr);
 }finally{await native.stop();}
}
async function inside(config){
 const cfg=JSON.parse(readFileSync(config,'utf8')),{workspace,view,binary,parent,child}=cfg;
 assert.equal(process.cwd(),workspace);assert.ok(!relative(join(workspace,'.volumes/agents'),view).startsWith('..'));
 assert.equal(readFileSync(join(view,'parent.txt'),'utf8'),'parent');writeFileSync(join(view,'child.txt'),'sandbox-child');
 let blocked=false;try{writeFileSync(cfg.outside,'must be refused by the host');}catch{blocked=true;}
 assert.equal(blocked,cfg.outside_write_blocked,'host sandbox boundary changed');
 const cli=async(args,cwd=workspace,payload='')=>{
  const env={...process.env};delete env.ACYCLIC_TASK;return await checked(binary,args,{cwd,env},payload);
 };
 await cli(['agents'],view);
 assert.notEqual(JSON.parse(await cli(['__hook',cfg.host,'Stop'],view,JSON.stringify({cwd:view,task:child}))).continue,false);
 await cli(['--task',child,'capture'],view);await cli(['--task',parent,'fs','merge','agents/sandbox-child']);
 assert.equal(readFileSync(join(workspace,'child.txt'),'utf8'),'sandbox-child');
 await cli(['--task',parent,'agents']);await cli(['--task',parent,'log']);await cli(['--task',parent,'discard','agents/sandbox-child']);
 assert.equal(readFileSync(join(workspace,'child.txt'),'utf8'),'sandbox-child');
 assert.deepEqual(JSON.parse(await cli(['doctor',cfg.host])).additional_sandbox_roots,[]);
 for(const args of [['agents'],['log'],['discard','agents/sandbox-child'],['fs','merge','agents/sandbox-child'],['capture']]){
  assert.notEqual((await run(binary,['--task',parent,...args],{cwd:cfg.negative})).code,0,'external data endpoint admitted');
 }
 assert.equal(JSON.parse(await cli(['__hook',cfg.host,'Stop'],cfg.negative,JSON.stringify({cwd:cfg.negative,task:parent}))).continue,false);
 assert.deepEqual(readdirSync(join(cfg.external_data,'requests')),[]);assert.ok(!existsSync(join(cfg.external_data,'status')));
 writeFileSync(join(workspace,'sandbox-result.json'),JSON.stringify({host:cfg.host,commands:true,captured:true,merged:true,discarded:true,
  hook_command:true,external_data_rejected:true,outside_write_blocked:blocked,model_calls:0}));
}
async function main(){
 const names=['inside','host','binary','graphcoder','native','package','scratch','pi-sandbox-codex'];
 const {values:args}=parseArgs({options:Object.fromEntries(names.map(name=>[name,{type:'string'}]))});
 if(args.inside){await inside(args.inside);return;}
 assert.ok(['codex','pi'].includes(args.host)&&['binary','graphcoder','native','package','scratch'].every(name=>args[name]));
 assert.ok(!args['pi-sandbox-codex']||args.host==='pi');
 const started=performance.now(),root=mkdtempSync(join(resolve(args.scratch),'plugin-sandbox-'));
 const workspace=join(root,'workspace'),state=join(root,'trusted-state');mkdirSync(workspace);mkdirSync(state);
 writeFileSync(join(workspace,'parent.txt'),'parent');
 const env=fixtureEnvironment(join(root,'profile'),{PATH:dirname(resolve(args.native))+delimiter+process.env.PATH,LANG:'C.UTF-8'});
 // These deliberately external destinations must remain empty. They are
 // negative custody inputs inside the same admitted fixture lease.
 for(const key of ['XDG_STATE_HOME','XDG_DATA_HOME','PLUGIN_DATA','CLAUDE_PLUGIN_DATA']){env[key]=join(root,key.toLowerCase());mkdirSync(env[key]);}
 const ownerEnv=fixtureEnvironment(join(root,'owner-profile'),{PATH:env.PATH});
 let stop;
 try{
  stop=await workspaceOwner(resolve(args.graphcoder),workspace,state,root,ownerEnv);
  const action=async value=>JSON.parse(await checked(resolve(args.binary),['__workspace'],{cwd:workspace,env},JSON.stringify(value)));
  const parent=await action({kind:'open',host:args.host,session:'sandbox-commands'});
  const child=await action({kind:'session',parent:parent.task,reference:'sandbox-child',action:{kind:'spawn',prompt:'sandbox'}});
  env.ACYCLIC_TASK=parent.task;
  const negative=join(workspace,'.acyclic-sdk/sandbox-negative'),external=join(root,'legacy-data');
  mkdirSync(negative);mkdirSync(external);mkdirSync(join(external,'requests'));
  const endpoint=JSON.parse(readFileSync(join(workspace,'.acyclic-sdk/host-files/owner.json'),'utf8'));endpoint.workspace=negative;
  writeFileSync(join(external,'owner.json'),JSON.stringify(endpoint));mkdirSync(join(negative,'.acyclic-sdk'));
  symlinkSync(external,join(negative,'.acyclic-sdk/host-files'),'dir');
  const config=join(workspace,'sandbox-config.json');
  writeFileSync(config,JSON.stringify({host:args.host,workspace,view:child.cwd,binary:resolve(args.binary),parent:parent.task,child:child.task,
   negative,external_data:external,outside:join(state,'forbidden'),outside_write_blocked:args.host!=='pi'||Boolean(args['pi-sandbox-codex'])}));
  // Keep the module at its original immutable source location, so all native
  // probes use the same shared fixture/MCP implementation.
  const invocation=[process.execPath,fileURLToPath(import.meta.url),'--inside',config];
  if(args.host==='codex'){
   const [program,...nativeArgs]=codexSandbox(resolve(args.native),workspace);
   await checked(program,[...nativeArgs,...invocation],{cwd:workspace,env});
  }else{
   let prefix=[];
   if(args['pi-sandbox-codex']){env.PI_CODING_AGENT_DIR=join(workspace,'.acyclic-sdk/pi-fixture');mkdirSync(env.PI_CODING_AGENT_DIR);prefix=codexSandbox(resolve(args['pi-sandbox-codex']),workspace);}
   await checked(resolve(args.binary),['install','pi','--plugin-root',resolve(args.package),'--settings',join(env.PI_CODING_AGENT_DIR,'extensions/acyclic.ts')],{cwd:workspace,env});
   const quote=value=>"'"+value.replaceAll("'","'\\''")+"'";
   await piCommand(resolve(args.native),invocation.map(quote).join(' '),{cwd:workspace,env,prefix});
   assert.ok((await action({kind:'hook',host:'pi',event:'NativeSession',input:{task:parent.task}})).id);
  }
  const result=JSON.parse(readFileSync(join(workspace,'sandbox-result.json'),'utf8'));
  for(const name of ['XDG_STATE_HOME','XDG_DATA_HOME','PLUGIN_DATA','CLAUDE_PLUGIN_DATA'])assert.deepEqual(readdirSync(env[name]),[],`client wrote external ${name}`);
  console.log(JSON.stringify({...result,inherited_codex_workspace_write:Boolean(args['pi-sandbox-codex']),seconds:(performance.now()-started)/1000}));
 }finally{try{if(stop)await stop();}finally{rmSync(root,{recursive:true,force:true});}}
}
if(process.argv[1]&&pathToFileURL(resolve(process.argv[1])).href===import.meta.url){
 try{await main();}catch(error){console.error(`step=native host command contract: ${error.stack}; recovery command: ${JSON.stringify(process.argv)}`);process.exitCode=1;}
}
