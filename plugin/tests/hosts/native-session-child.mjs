#!/usr/bin/env node
// Native scale and descendant controls. Completion events are admitted captures;
// the shipping owner retains every original process, view and socket.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync} from 'node:fs';
import {dirname,join,relative} from 'node:path';
import {createServer} from 'node:net';
const {hold}=await import(process.env.FIXTURE_SUPPORT);
const args=process.argv.slice(2),cwd=process.cwd(),env=process.env;
try{
 if(args.length===1&&args[0]==='--descendant'){
  // An independent OS listener keeps this process alive after its parent
  // closes stdin. Only process-tree cancellation may retire this child.
  let input;
  const server=createServer(socket=>{input.then(()=>socket.end(server.listening?'alive\n':'retired\n'));});
  input=new Promise(yes=>{process.stdin.once('end',yes);});
  await new Promise((yes,no)=>{server.once('error',no);server.listen(0,'127.0.0.1',yes);});
  console.log(JSON.stringify({port:server.address().port}));
  process.stdin.resume();
 }else if(env.FIXTURE_SCALE){
  const host=env.FIXTURE_HOST,flags=JSON.parse(env.FIXTURE_FLAGS);
  for(let index=0;index<flags.length;index+=2)assert.equal(args[args.indexOf(flags[index])+1],flags[index+1]);
  const child=args.includes({codex:'exec','claude-code':'-p',pi:'--mode'}[host]);
  if(!child){writeFileSync('root.json',JSON.stringify({task:env.ACYCLIC_TASK,flags:JSON.parse(env.ACYCLIC_HOST_ARGS)}));}
  else{
   assert.ok(!relative(join(dirname(env.ACYCLIC_HOST_FILES_DIRECTORY),'.volumes/agents'),cwd).startsWith('..'));
   assert.equal(readFileSync('AGENTS.md','utf8'),'shared scale discovery fixture');
   let prompt='';for await(const chunk of process.stdin)prompt+=chunk;
   if(prompt==='exit-before-connect'){process.stderr.write('exited-before-connect');process.exitCode=23;}
   else{
    writeFileSync(prompt+'.txt',prompt);writeFileSync('.volumes/home/private.txt',prompt);
    process.stdout.write(readFileSync(env.FIXTURE_EVENTS));await hold(prompt,true);
    process.stderr.write('e'.repeat(262144)+'diagnostic-tail-'+host);process.exitCode=17;
   }
  }
 }else throw new Error('expected --descendant or FIXTURE_SCALE');
}catch(error){
 console.error(`step=run native process fixture: ${error.stack}; recovery: run the original native scale scenario`);process.exitCode=1;
}
