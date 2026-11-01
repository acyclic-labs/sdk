import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Processes, Spool } from "./process.mjs";
import { withOwnedCase } from "./suite.mjs";

test("owned cases join cleanup before returning or propagating failure", async () => {
  for (const failed of [false, true]) {
    const drain = Promise.withResolvers(), started = Promise.withResolvers();
    const failure = new Error("selected scenario failed");
    let settled = false, closes = 0;
    const scope = { async close() { closes++; started.resolve(); await drain.promise; } };
    const running = withOwnedCase({ processes: scope, run: async () => {
      if (failed) throw failure;
      return "scenario result";
    } });
    running.then(() => settled = true, () => settled = true);
    await Promise.resolve();
    assert.equal(closes, 1, "every selected case must join the supplied process owner's drain");
    await started.promise;
    assert.equal(settled, false, "selected case must retain custody until physical drain");
    drain.resolve();
    if (failed) await assert.rejects(running, error => error === failure);
    else {
      const receipt = await running;
      assert.equal(receipt.run, "scenario result");
      assert.ok(receipt.seconds > 0 && receipt.seconds < 10);
    }
  }
});

test("preparation and phase-publication failures retain the original physical cleanup", async () => {
  for (const preparationFails of [false, true]) {
    const failure = new Error(preparationFails ? "preparation refused" : "phase sink refused");
    let closes = 0, runs = 0;
    const scope = { async close() { closes++; } };
    await assert.rejects(withOwnedCase({ processes: scope,
      prepare: () => { if (preparationFails) throw failure; },
      run: () => { runs++; },
      onPhase: () => { if (!preparationFails) throw failure; },
    }), error => error === failure);
    assert.equal(closes, 1, "observation failure must never skip physical retirement");
    assert.equal(runs, 0, "failed preparation or phase admission cannot execute the product sample");
  }
});

test("native preparation completion admits execution through the original process owner", async () => {
  const parent = realpathSync(tmpdir()), root = mkdtempSync(join(parent, "case-preparation-"));
  const processes = new Processes({ transcripts: root });
  let preparing, events, admitted = false;
  try {
    const running = withOwnedCase({ processes, prepare: () => {
      preparing = processes.start(process.execPath, ["-e",
        "console.log(JSON.stringify({ready:true}));process.stdin.resume();process.stdin.on('end',()=>{})"]);
      events = new Spool(preparing.child.stdout, preparing.closed, { key: event => event.ready ? ["ready"] : [] });
      return preparing.closed.then(result => {
        assert.equal(result.code, 0, result.stderr);
        return result;
      });
    }, run: async prepared => {
      admitted = true;
      assert.equal(prepared.code, 0);
      return processes.run(process.execPath, ["-e", "process.stdout.write('selected')"]);
    } });
    running.catch(() => {});
    await events.keyed("ready");
    assert.equal(admitted, false, "a live preparation producer must prevent early admission");
    preparing.child.stdin.end();
    const receipt = await running;
    assert.equal(receipt.run.code, 0, receipt.run.stderr);
    assert.equal(receipt.run.stdout, "selected");
    assert.equal(processes.children.size, 0, "both original children must be reaped before publication");
  } finally {
    await processes.close();
    assert.equal(dirname(root), parent);
    rmSync(root, { recursive: true });
  }
});

test("a real preparation launch refusal publishes its phase after original retirement", async () => {
  const parent = realpathSync(tmpdir()), root = mkdtempSync(join(parent, "case-preparation-refused-"));
  const processes = new Processes({ transcripts: root });
  try {
    const result = await processes.run(process.execPath,
      [fileURLToPath(new URL("./run.mjs", import.meta.url)), join(root, "absent-command"), join(root, "absent-model"), "cli-help"],
      { env: { ...process.env, E2E_SCRATCH_ROOT: root, E2E_OUTPUT: join(root, "results") } });
    assert.equal(result.code, 1, result.stderr);
    const report = JSON.parse(result.stdout);
    assert.equal(report.phases.preparation.outcome, "failed");
    assert.equal(report.phases.retired.outcome, "completed");
    assert.equal(report.phases.admitted, undefined);
    assert.equal(report.phases.run, undefined);
    assert.ok(report.preparation_seconds > 0);
    assert.equal(report.product_seconds, undefined, "preparation refusal has no product latency sample");
    assert.equal(report.results.length, 1);
    assert.equal(report.results[0].phase, "preparation");
    assert.match(report.results[0].error, /absent-model.*ENOENT/);
    assert.equal(processes.children.size, 0);
  } finally {
    await processes.close();
    assert.equal(dirname(root), parent);
    rmSync(root, { recursive: true });
  }
});
