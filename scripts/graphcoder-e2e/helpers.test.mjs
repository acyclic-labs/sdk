import {runScriptedScenario} from '../../plugin/tests/hosts/qualify-scripted.mjs';
import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { runHelperSuite } from "./helpers.mjs";
import { Processes } from "./process.mjs";
import { alive } from "./orphan-check.mjs";

test("Node test flags always use Node independently of the parent interpreter", async () => {
  const directory = await mkdtemp(join(tmpdir(), "helper-interpreter-"));
  let observed;
  class ObservedProcesses extends Processes {
    startOwned(command, args, ...options) {
      observed = { command, args };
      return super.startOwned(command, args, ...options);
    }
  }
  try {
    const fixture = join(directory, "interpreter.test.mjs");
    // Program input; the original Node executable produces the qualified TAP.
    await writeFile(fixture, `import test from 'node:test'; import assert from 'node:assert/strict';
      test('original interpreter',()=>assert.equal(process.versions.bun,undefined));`);
    await runHelperSuite([fixture], { processes: new ObservedProcesses(), publish() {} });
    assert.equal(observed.command, "node");
    assert.equal(observed.args[0], "--test");

    // The original child has exited successfully, but cancellation at its
    // publication cut must still prevent that actual summary from qualifying.
    const late = new AbortController();
    await assert.rejects(runHelperSuite([fixture], {
      signal: late.signal,
      publish() { late.abort(new Error("late helper cancellation")); },
    }), /late helper cancellation/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("helper cancellation drains every process and cannot qualify a blocked suite", async () => {
  const directory = await mkdtemp(join(tmpdir(), "helper-cancellation-"));
  const controller = new AbortController();
  let pids;
  class ObservedProcesses extends Processes {
    startOwned(...args) {
      const running = super.startOwned(...args);
      let output = "";
      running.child.stdout.on("data", bytes => {
        output += bytes;
        const ready = output.match(/BLOCKED_HELPER (\[[^\r\n]+\])/);
        if (ready && !pids) {
          pids = JSON.parse(ready[1]);
          controller.abort(new Error("deliberate helper cancellation"));
        }
      });
      return running;
    }
  }
  try {
    const fixture = join(directory, "blocked.test.mjs");
    await writeFile(fixture, `import test from 'node:test'; import {spawn} from 'node:child_process';
      import {once} from 'node:events';
      test('blocked fixture',async()=>{
        const child=spawn(process.execPath,['-e','process.stdin.resume()'],{stdio:['pipe','ignore','ignore'],windowsHide:true});
        await once(child,'spawn'); console.log('BLOCKED_HELPER '+JSON.stringify([process.pid,child.pid]));
        await new Promise(()=>{});
      });`);
    await assert.rejects(runHelperSuite([fixture], { signal: controller.signal,
      processes: new ObservedProcesses(), publish() {} }), /deliberate helper cancellation/);
    assert.equal(pids.length, 2);
    for (const pid of pids) assert.equal(alive(pid), false, `helper process ${pid} survived cancellation`);
  } finally {
    controller.abort();
    await rm(directory, { recursive: true, force: true });
  }
});

test('scripted acceptance follows the original process and oracle independently of elapsed time',async t=>{
 const directory=await mkdtemp(join(tmpdir(),'scripted-completion-'));
 let clock=0;
 t.mock.method(performance,'now',()=>clock);
 class MeasuredProcesses extends Processes {
  async runOwned(...args){
   const result=await super.runOwned(...args);
   clock+=30_000;
   return result;
  }
 }
 const processes=new MeasuredProcesses();
 try{
  for(const [name,code,oracle,expired,accepted] of [
   ['completed',0,true,false,true],['exit-failure',3,true,false,false],
   ['oracle-failure',0,false,false,false],['cancelled',0,true,true,false],
  ]){
   const scenario={name,selection:name,args:['-c',`print('original output'); exit(${code})`],
    accepts:run=>oracle&&run.stdout==='original output\n'};
   const receipt=await runScriptedScenario({scenario,processes,directory,env:{PATH:process.env.PATH},
    expired:()=>expired});
   assert.equal(receipt.accepted,accepted,name);
   assert.equal(receipt.seconds,30);
   assert.equal(receipt.exit,code);
   assert.equal(receipt.expired,expired);
   assert.equal(await readFile(join(directory,name+'.log'),'utf8'),'original output\n');
   assert.equal(processes.children.size,0,'qualification joins original output and process retirement');
  }
 }finally{
  await processes.close();
  await rm(directory,{recursive:true,force:true});
 }
});
