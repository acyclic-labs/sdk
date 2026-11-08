import assert from 'node:assert/strict';
import {spawn} from 'node:child_process';
import {createHash} from 'node:crypto';
import {readFileSync} from 'node:fs';
import {createServer,connect} from 'node:net';
import {once} from 'node:events';
import {fileURLToPath} from 'node:url';
import {McpClient} from '../../shared/mcp-client.mjs';
import {environment,nativeEnvironment} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {Processes,Spool} from '../../../scripts/graphcoder-e2e/process.mjs';
import {startWorkspaceOwner} from './workspace-owner.mjs';
export {Spool};

export function fixtureEnvironment(root,variables={}) {
 const inherited=nativeEnvironment();
 if(variables.PATH!==undefined)inherited.PATH=variables.PATH;
 const profile=environment(root,inherited);
 const {PATH,...extra}=variables;
 assert.ok(Object.keys(extra).every(key=>!Object.hasOwn(profile,key)),
  'step=admit native fixture variables; recovery: remove profile overrides and use the shared environment factory');
 return {...profile,...extra};
}

// Outer acceptance custody owns this process tree. Each pipe and exit retain
// their original producer; this adapter adds no timer or completion synthesis.
export function start(command,args,{cwd,env=process.env}={}) {
 const child=spawn(command,args,{cwd,env,stdio:'pipe',windowsHide:true});
 let stdout='',stderr='';
 child.stderr.setEncoding('utf8');child.stderr.on('data',chunk=>{stderr+=chunk;});
 child.stdin.on('error',error=>{if(error.code!=='EPIPE')child.emit('error',error);});
 const closed=new Promise((yes,no)=>{
  child.once('error',no);child.once('close',(code,signal)=>yes({code,signal,get stdout(){return stdout;},get stderr(){return stderr;}}));
 });
 closed.catch(()=>{});
 return {child,closed,collect(){child.stdout.setEncoding('utf8');child.stdout.on('data',chunk=>{stdout+=chunk;});},
  async stop(){child.kill();await closed;},get stderr(){return stderr;}};
}

export async function run(command,args,options,input='') {
 const process=start(command,args,options);process.collect();process.child.stdin.end(input);
 return await process.closed;
}
export async function checked(command,args,options,input='') {
 const result=await run(command,args,options,input);
 assert.equal(result.code,0,`step=run native command: ${result.stderr}; recovery command: ${JSON.stringify([command,...args])}`);
 return result.stdout;
}
export async function workspaceOwner(binary,workspace,state,root,env) {
 const scope=new Processes();
 try{await startWorkspaceOwner({scope,binary,workspace,state,root,env});}
 catch(error){await scope.close();throw error;}
 return ()=>scope.close();
}
export function coordination(process,recovery) {
 const client=new McpClient(process.child,{name:'native-session-conformance',version:'1',recovery});
 return (name,reference,prompt)=>client.request(name,{reference,...(prompt===undefined?{}:{prompt})}).then(result=>{
  assert.ok(!result?.isError,`step=complete MCP tool: ${JSON.stringify(result)}; recovery=${recovery}`);
  const decoded=JSON.parse(result.content[0].text);
  assert.ok(decoded&&typeof decoded==='object'&&!Array.isArray(decoded),`step=decode MCP result; recovery=${recovery}`);
  return decoded;
 });
}
export function nativeEventFixture(host) {
 const file=new URL(`fixtures/native/${host}.jsonl`,import.meta.url);
 const bytes=readFileSync(file);
 const receipt=JSON.parse(readFileSync(new URL('fixtures/native/provenance.json',import.meta.url),'utf8')).find(row=>row.host===host);
 assert.equal(createHash('sha256').update(bytes).digest('hex'),receipt.fixture_sha256,
  `step=admit captured native event; recovery: restore ${fileURLToPath(file)}`);
 const event=JSON.parse(bytes),identifier=event.thread_id??event.session_id??event.id;
 assert.ok(typeof identifier==='string'&&identifier);
 return {path:fileURLToPath(file),identifier};
}

// One connection event per parked child. The consumer is registered before
// spawn; child settlement races the same waiter, so death cannot hang readiness.
export async function barrier() {
 const server=createServer(),sockets=new Set();
 await new Promise((yes,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',yes);});
 server.on('connection',socket=>{sockets.add(socket);socket.once('close',()=>sockets.delete(socket));});
 return {port:server.address().port,
  next(signal){return once(server,'connection',{signal}).then(([socket])=>socket);},
  async close(){for(const socket of sockets)socket.destroy();await new Promise((yes,no)=>server.close(error=>error?no(error):yes()));}};
}
export async function ready(connection,terminal,abort,recovery) {
 try{return await Promise.race([connection,terminal.then(result=>{
  throw new Error(`step=observe native readiness: child settled before readiness: ${JSON.stringify(result)}; recovery=${recovery}`);
 })]);}finally{abort.abort();}
}
export async function socketLine(socket) {
 let pending='';
 for await(const chunk of socket.iterator({destroyOnReturn:false})) {
  pending+=chunk.toString();const newline=pending.indexOf('\n');
  if(newline>=0)return pending.slice(0,newline);
 }
 throw new Error('step=observe native identity: producer closed; recovery: run the original native scenario');
}
export async function hold(message,answer=false) {
 const socket=connect({host:'127.0.0.1',port:Number(process.env.FIXTURE_BARRIER)});
 await once(socket,'connect');socket.write(message+'\n');
 for await(const chunk of socket) {
  for(const byte of chunk){
   if(answer&&byte===63)socket.write('!');
   else if(answer){socket.end();return;}
   else throw new Error('step=hold native fixture: unexpected readiness input; recovery: run the original managed session scenario');
  }
 }
 if(!answer)throw new Error('step=hold native fixture: readiness owner closed; recovery: run the original managed session scenario');
}
