import assert from 'node:assert/strict';
import {parseArgs} from 'node:util';
import {rmSync} from 'node:fs';
import {resolve} from 'node:path';
import {once} from 'node:events';
import {fixtureEnvironment,start} from './native-fixture.mjs';
import {scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {Spool} from '../../../scripts/graphcoder-e2e/process.mjs';

const recovery='node plugin/tests/hosts/mcp-framing.mjs ACYCLIC [--host codex|claude-code|pi] [--case CASE]';
const cases=['framed','oversized-open','oversized-delimited','initialized','uninitialized','invalid-frame','closed-producer'];
try{
 const {positionals,values}=parseArgs({allowPositionals:true,options:{host:{type:'string'},case:{type:'string'}}});
 assert.equal(positionals.length,1);assert.ok(values.host===undefined||['codex','claude-code','pi'].includes(values.host));assert.ok(values.case===undefined||cases.includes(values.case));
 const binary=resolve(positionals[0]),root=scratchDirectory('plugins-mcp-framing-'),started=performance.now(),results=[];
 try{
  const env=fixtureEnvironment(root);
  for(const host of values.host?[values.host]:['codex','claude-code','pi'])for(const name of values.case?[values.case]:cases){
   const running=start(binary,['__mcp',host],{cwd:root,env}),child=running.child;
   const spool=new Spool(child.stdout,running.closed,{key:message=>[message.id??'error'],unique:true});
   const send=async bytes=>{
    if(child.stdin.write(bytes))return;
    const abort=new AbortController();
    try{await Promise.race([once(child.stdin,'drain',{signal:abort.signal}),running.closed.then(()=>{
     throw Object.assign(new Error(`step=write MCP frame: producer closed; recovery=${recovery}`),{code:'MCP_CLOSED'});
    })]);}finally{abort.abort();}
   };
   const message=async value=>send(JSON.stringify(value)+'\n');
   try{
    if(['initialized','invalid-frame','closed-producer'].includes(name)){
     await message({jsonrpc:'2.0',id:0,method:'initialize',params:{protocolVersion:'2025-06-18',capabilities:{},clientInfo:{name:'native-session-conformance',version:'1'}}});
     assert.equal((await spool.keyed(0)).result.protocolVersion,'2025-06-18');
     await message({jsonrpc:'2.0',method:'notifications/initialized'});
     if(name==='initialized'){
      await message({jsonrpc:'2.0',id:1,method:'tools/list'});
      assert.deepEqual((await spool.keyed(1)).result.tools.map(tool=>tool.name).sort(),['spawn','send','wait','cancel','archive','resume'].sort());
     }
     if(name==='invalid-frame'){await send('{\n');assert.equal((await spool.keyed('error')).error.code,-32700);}
     child.stdin.end();
     if(name==='closed-producer')await assert.rejects(spool.keyed(1),/source exited/);
    }else if(name==='uninitialized'){
     await message({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'spawn',arguments:{reference:'child',prompt:'uninitialized'}}});
     const answer=await spool.keyed(1);assert.equal(answer.error.code,-32000);assert.match(answer.error.message,/Initialize first/);child.stdin.end();
    }else if(name==='framed'){
     await send([1,2].map(id=>JSON.stringify({jsonrpc:'2.0',id,method:'ping'})).join('\r\n')+'\n');
     for(const id of [1,2])assert.deepEqual(await spool.keyed(id),{jsonrpc:'2.0',id,result:{}});child.stdin.end();
    }else{
     const frame=Buffer.from(JSON.stringify({jsonrpc:'2.0',id:3,method:'ping',padding:'x'.repeat(65537)}));
     // Keep the peer open while the original server rejects and closes.
     const writing=send(name==='oversized-open'?frame.subarray(0,65537):Buffer.concat([frame,Buffer.from('\n')])).catch(error=>{if(!['EPIPE','MCP_CLOSED'].includes(error.code))throw error;});
     assert.equal((await spool.keyed('error')).error.code,-32700);
     await running.closed;assert.ok(!child.stdin.writableEnded);await writing;
    }
    const result=await running.closed;
    assert.equal(result.code,['framed','initialized','uninitialized','closed-producer'].includes(name)?0:1,result.stderr);
    results.push({host,case:name,passed:true});
   }finally{await running.stop();}
  }
  console.log(JSON.stringify({cases:results,wall_seconds:(performance.now()-started)/1000}));
 }finally{rmSync(root,{recursive:true,force:true});}
}catch(error){console.error(`step=qualify MCP framing: ${error.stack}; recovery command: ${recovery}`);process.exitCode=1;}
