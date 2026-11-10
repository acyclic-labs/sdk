#!/usr/bin/env node
// The shipping launcher and owner are real binaries. This process only marks
// entry into the external host, without requiring credentials or a model.
import assert from 'node:assert/strict';
import {writeFileSync} from 'node:fs';
import {spawn} from 'node:child_process';
import {once} from 'node:events';
assert.equal(process.argv.at(-1),'no-FUSE root prompt');
assert.ok(process.env.ACYCLIC_TASK);
assert.equal(process.env.ACYCLIC_WORKSPACE,process.cwd());
writeFileSync('native-root.txt',process.env.ACYCLIC_TASK);
if(process.env.FIXTURE_BARRIER){
 const child=spawn(process.execPath,['--input-type=module','-e',`
  import {connect} from 'node:net';
  import {writeFileSync} from 'node:fs';
  import {once} from 'node:events';
  const socket=connect({host:'127.0.0.1',port:Number(process.env.FIXTURE_BARRIER)});
  await once(socket,'connect');socket.write(String(process.pid)+'\\n');
  await once(socket,'data');
  writeFileSync('before-output-failure.txt','captured');
  process.stdout.write(JSON.stringify({type:'native.large',text:'x'.repeat(131072)})+'\\n');
 `],{stdio:'inherit'});
 await once(child,'close');
 process.exit(0);
}
// A whole native JSON reply can exceed the retained-event allowance. Its loss
// must be explicit and must not discard the following event or checkout write.
console.log(JSON.stringify({type:'native.large',text:'x'.repeat(131072)}));
console.log(JSON.stringify({type:'native.started',task:process.env.ACYCLIC_TASK}));
