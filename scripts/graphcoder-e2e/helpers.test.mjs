import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
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
