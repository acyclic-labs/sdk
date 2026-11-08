#!/usr/bin/env node
// Explicit process stand-in. Its wire events are admitted captures; sandbox
// effects below execute through the actual selected native CLI.
import assert from 'node:assert/strict';
import {readFileSync,readSync,writeFileSync,existsSync} from 'node:fs';
import {join,relative} from 'node:path';
const {checked,start,coordination,hold}=await import(process.env.FIXTURE_SUPPORT);
const args=process.argv.slice(2),cwd=process.cwd(),env=process.env;
try{
 if(env.FIXTURE_SCALE){
  const host=env.FIXTURE_HOST,flags=JSON.parse(env.FIXTURE_FLAGS);
  for(let index=0;index<flags.length;index+=2)assert.equal(args[args.indexOf(flags[index])+1],flags[index+1]);
  const child=args.includes({codex:'exec','claude-code':'-p',pi:'--mode'}[host]);
  if(!child){writeFileSync('root.json',JSON.stringify({task:env.ACYCLIC_TASK,flags:JSON.parse(env.ACYCLIC_HOST_ARGS)}));}
  else{
   assert.ok(!relative(join(env.FIXTURE_WORKSPACE,'.volumes/agents'),cwd).startsWith('..'));
   assert.equal(readFileSync('AGENTS.md','utf8'),'shared scale discovery fixture');
   let prompt='';for await(const chunk of process.stdin)prompt+=chunk;
   if(prompt==='exit-before-connect'){process.stderr.write('exited-before-connect');process.exitCode=23;}
   else{
    writeFileSync(prompt+'.txt',prompt);writeFileSync('.volumes/home/private.txt',prompt);
    process.stdout.write(readFileSync(env.FIXTURE_EVENTS));await hold(prompt,true);
    process.stderr.write('e'.repeat(262144)+'diagnostic-tail-'+host);process.exitCode=17;
   }
  }
 }else{
  if(!args.includes('exec')){
   assert.deepEqual(args,['--sandbox','workspace-write','--ask-for-approval','never','--add-dir',env.FIXTURE_WORKSPACE]);
   assert.equal(cwd,env.ACYCLIC_WORKSPACE);assert.equal(cwd,env.FIXTURE_WORKSPACE);
   assert.deepEqual(JSON.parse(env.ACYCLIC_HOST_ARGS),args);
   writeFileSync('root-launch.json',JSON.stringify({task:env.ACYCLIC_TASK,args}));
  }else{
   assert.deepEqual(args.slice(6,8),['exec','--json']);
   assert.equal(args[args.indexOf('--sandbox')+1],'workspace-write');
   assert.equal(args[args.indexOf('--ask-for-approval')+1],'never');
   assert.equal(args[args.indexOf('--cd')+1],cwd);
   assert.ok(!relative(join(env.FIXTURE_WORKSPACE,'.volumes/agents'),cwd).startsWith('..'));
   assert.equal(readFileSync('AGENTS.md','utf8'),'native instruction discovery fixture');
   // Read exactly the first byte before starting the stream reader. The
   // cancellation control must leave the remaining pipe bytes undrained.
   const first=Buffer.alloc(1);
   assert.equal(readSync(0,first,0,1,null),1,'native prompt closed before its first byte');
   if(first[0]===66)await hold(JSON.stringify({cwd}));
   let prompt=first.toString();for await(const chunk of process.stdin)prompt+=chunk;
   assert.ok(['failed','cancelled','recursive','first','resumed'].includes(prompt));
   process.stdout.write(readFileSync(env.FIXTURE_EVENTS));
   const base=['sandbox','-c','sandbox_mode="workspace-write"','-c','approval_policy="never"','-c',
    'sandbox_workspace_write.writable_roots='+JSON.stringify([args[args.indexOf('--add-dir')+1]]),'--'];
   if(prompt==='resumed')assert.equal(readFileSync('.volumes/home/native-private.txt','utf8'),'first');
   const shell='set -eu; "$1" agents >/dev/null; printf "%s" "$4" > .volumes/home/native-private.txt; '+
    'if (printf denied > "$2") 2>/dev/null; then exit 41; fi; test ! -e "$2"; printf native > "$3"; '+
    'if test "$4" = failed; then exit 17; fi';
   const {run}=await import(env.FIXTURE_SUPPORT);
   const result=await run(env.FIXTURE_CODEX,[...base,'sh','-c',shell,'fixture',env.FIXTURE_ACYCLIC,env.FIXTURE_OUTSIDE,prompt+'.txt',prompt],{cwd,env});
   assert.equal(result.code,prompt==='failed'?17:0,result.stderr);
   if(prompt==='recursive'){
    const nested=start(env.FIXTURE_ACYCLIC,['__mcp','codex'],{cwd,env});
    try{
     const tool=coordination(nested,'node plugin/tests/hosts/managed-session.mjs ACYCLIC GRAPHCODER CODEX recursive');
     await tool('spawn','nested','first');const finished=await tool('wait','nested');
     assert.equal(finished.reply.kind,'answered');assert.notEqual(finished.reply.outcome.Succeeded,undefined);
     await checked(env.FIXTURE_CODEX,[...base,env.FIXTURE_ACYCLIC,'fs','merge','agents/nested'],{cwd,env});
     assert.equal(readFileSync('first.txt','utf8'),'native');assert.ok(!existsSync('.acyclic-sdk'));
    }finally{nested.child.stdin.end();assert.equal((await nested.closed).code,0);}
   }
   if(prompt==='cancelled'){
    const child=start(process.execPath,['-e',"process.stdin.resume();process.stdin.on('end',()=>{})"],{cwd,env});
    writeFileSync('descendant.pid',String(child.child.pid));
    await hold(JSON.stringify({cwd}));
   }
   if(prompt==='failed')process.exitCode=17;
   if(prompt==='resumed')assert.equal(args[args.indexOf('resume')+1],JSON.parse(readFileSync(env.FIXTURE_EVENTS,'utf8').split('\n')[0]).thread_id);
  }
 }
}catch(error){
 try{writeFileSync('.volumes/home/native-error.txt',error.stack);}catch{}
 console.error(`step=run native process fixture: ${error.stack}; recovery: run the original native scenario`);process.exitCode=1;
}
