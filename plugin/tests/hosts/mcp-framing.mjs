// Exercise the shipping MCP entry point with the original framing controls.
// Raw frames deliberately bypass initialization and encoding where each control
// requires it. The shared spool owns reply decoding and producer-loss waits.
import assert from 'node:assert/strict';
import {resolve} from 'node:path';
import {Processes,Spool,SpoolEnded} from '../../../scripts/graphcoder-e2e/process.mjs';
import {environment,nativeEnvironment,scratchDirectory} from '../../../scripts/graphcoder-e2e/scratch.mjs';
import {commandCancellation,withOwnedCase} from '../../../scripts/graphcoder-e2e/suite.mjs';

const hosts=['codex','claude-code','pi'];
const cases=['framed','oversized-open','oversized-delimited','initialized',
 'uninitialized','invalid-frame','closed-producer'];
const recovery='node plugin/tests/hosts/mcp-framing.mjs ACYCLIC [--host HOST] [--case CASE]';
const [binary,...options]=process.argv.slice(2);
let selectedHost,selectedCase;
for(let i=0;i<options.length;i+=2){
 const [name,value]=options.slice(i,i+2);
 assert.ok(value&&(name==='--host'||name==='--case'),'step=admit framing selection; recovery='+recovery);
 if(name==='--host')selectedHost=value;else selectedCase=value;
}
assert.ok(binary&&(!selectedHost||hosts.includes(selectedHost))&&(!selectedCase||cases.includes(selectedCase)),
 'step=admit framing arguments; recovery='+recovery);
const started=performance.now(),results=[],cancellation=commandCancellation();
const root=scratchDirectory('plugins-mcp-framing-');
const env=environment(root,nativeEnvironment());
try{
 for(const host of selectedHost?[selectedHost]:hosts){
  for(const name of selectedCase?[selectedCase]:cases){
   const scope=new Processes();
   const failure=step=>'step='+step+' '+host+'/'+name+'; recovery='+recovery;
   await withOwnedCase({processes:scope,signal:cancellation.signal,run:async()=>{
    const running=scope.start(resolve(binary),['__mcp',host],{cwd:root,env});
    const replies=new Spool(running.child.stdout,running.closed,{unique:true,
     key:reply=>[reply.id==null?'protocol-error':reply.id]});
    const response=id=>replies.keyed(id,{signal:cancellation.signal});
    const send=message=>running.input.write(JSON.stringify(message)+'\n');
    if(['initialized','invalid-frame','closed-producer'].includes(name)){
     send({jsonrpc:'2.0',id:0,method:'initialize',params:{
      protocolVersion:'2025-06-18',capabilities:{},
      clientInfo:{name:'native-session-conformance',version:'1'}}});
     assert.equal((await response(0)).result?.protocolVersion,'2025-06-18',failure('initialize MCP'));
     send({jsonrpc:'2.0',method:'notifications/initialized'});
     if(name==='initialized'){
      send({jsonrpc:'2.0',id:1,method:'tools/list'});
      assert.deepEqual(new Set((await response(1)).result.tools.map(tool=>tool.name)),
       new Set(['spawn','send','wait','cancel','archive','resume']),failure('verify initialized MCP tools'));
     }else if(name==='invalid-frame'){
      running.input.write('{\n');
      assert.equal((await response('protocol-error')).error?.code,-32700,failure('retain MCP parse failure'));
     }
     running.input.end();
     if(name==='closed-producer')await assert.rejects(response(1),
      error=>error instanceof SpoolEnded&&error.kind==='eof',failure('observe MCP producer closure'));
    }else if(name==='uninitialized'){
     send({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'spawn',
      arguments:{reference:'child',prompt:'uninitialized'}}});
     const answer=await response(1);
     assert.equal(answer.error?.code,-32000,failure('refuse uninitialized MCP tool call'));
     assert.match(answer.error.message,/Initialize first/,failure('retain initialization refusal'));
     running.input.end();
    }else if(name==='framed'){
     running.input.write([1,2].map(id=>JSON.stringify({jsonrpc:'2.0',id,method:'ping'})).join('\r\n')+'\n');
     for(const id of [1,2])assert.deepEqual(await response(id),{jsonrpc:'2.0',id,result:{}},failure('retain frame boundaries'));
     running.input.end();
    }else{
     // Preserve the valid oversized ping used by the retained removal control.
     const frame=Buffer.from(JSON.stringify({jsonrpc:'2.0',id:3,method:'ping',padding:'x'.repeat(65_537)}));
     running.input.write(name==='oversized-open'?frame.subarray(0,65_537):Buffer.concat([frame,Buffer.from('\n')]));
     assert.equal((await response('protocol-error')).error?.code,-32700,failure('refuse oversized frame'));
     await assert.rejects(response('after-terminal'),
      error=>error instanceof SpoolEnded&&error.kind==='eof',failure('retire failed frame while peer remains open'));
     assert.equal(running.input.writableEnded,false,failure('keep original peer open during rejection'));
    }
    const result=await running.closed;
    const expected=['framed','initialized','uninitialized','closed-producer'].includes(name)?0:1;
    assert.equal(result.code,expected,failure('verify MCP termination')+': '+result.stderr);
    results.push({host,case:name,passed:true});
   }});
  }
 }
 console.log(JSON.stringify({cases:results,wall_seconds:(performance.now()-started)/1000}));
}finally{cancellation.dispose();}
