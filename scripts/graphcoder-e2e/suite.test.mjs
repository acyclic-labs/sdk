import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { Processes, Spool } from "./process.mjs";
import { commandCancellation, withOwnedCase } from "./suite.mjs";
import { failureReport, finishCase, retainCapture } from "./report.mjs";
import { conversation } from "./conversation.mjs";

test("conversation rejection retires only its producer and retains independent capture failure", async () => {
  const root = mkdtempSync(join(realpathSync(tmpdir()), "conversation-failure-"));
  try {
    for (const failedObservation of [false, true]) {
      let retirementAtEof, retirementRequested = false, selected, original;
      const captures = [], processes = new Processes({ onProgress(event) {
        if (event.phase === "node-spawn" && event.ordinal === 1 && failedObservation)
          readFileSync(join(root, "missing-progress-sink"));
        if (event.phase === "stdin-finish" && event.ordinal === 1) {
          retirementAtEof = retirementRequested;
          // Independently release a broken implementation too: the negative
          // control reports lost ownership instead of hanging on the socket.
          if (!retirementRequested) selected.child.kill("SIGKILL");
        }
      } });
      try {
        const peer = processes.start(process.execPath, ["-e",
          "require('node:readline').createInterface({input:process.stdin}).on('line',text=>console.log(JSON.stringify({text})));console.log(JSON.stringify({pid:process.pid}))"]);
        const peerEvents = new Spool(peer.child.stdout, peer.closed);
        await peerEvents.next(event => event.pid > 0);
        const running = conversation(processes, process.execPath, {
          id: "rejected-conversation", workspace: root, args: ["-e",
            "const server=require('node:net').createServer();server.listen(0,'127.0.0.1',()=>console.error(JSON.stringify(server.address())));process.stdin.resume()", "--"],
        }, { operation: "selected-operation", turns: [{ id: "turn", text: "unused" }] }, result => captures.push(result),
        async (_, active) => {
          selected = active;
          const kill = active.killExecutable;
          active.killExecutable = () => { retirementRequested = true; return kill(); };
          // The actual bound socket keeps the native child alive after EOF.
          await new Spool(active.child.stderr, active.closed).next(event => event.port > 0);
          try { readFileSync(join(root, "original-missing-input")); }
          catch (error) { original = error; throw error; }
        });
        await assert.rejects(running, error => {
          assert.match(error.message, /drive JSONL conversation.*original-missing-input/);
          if (!failedObservation) assert.equal(error.cause, original);
          else {
            assert.ok(error.cause instanceof AggregateError);
            assert.equal(error.cause.errors[0].cause, original);
            assert.ok(error.cause.errors.slice(1).every(failure =>
              failure.name === "ProcessObservationFailed" && failure.cause.code === "ENOENT"));
            const report = failureReport(error);
            assert.match(report.cause, /original-missing-input/);
            assert.match(report.cause, /missing-progress-sink/);
          }
          return true;
        });
        assert.equal(retirementAtEof, true, "EOF cannot substitute for selected process retirement");
        assert.equal(processes.stopping, false, "one failed conversation cannot close the suite owner");
        assert.equal(processes.children.size, 1);
        assert.ok(processes.children.has(peer.child), "unrelated original producer must remain live");
        peer.input.write("peer survives\n");
        await peerEvents.next(event => event.text === "peer survives");
        const later = await processes.run(process.execPath, ["-e", "process.exitCode=7"]);
        assert.equal(later.code, 7, "a later original case must still be admitted");
        assert.equal(captures.length, failedObservation ? 0 : 1);
      } finally { await processes.close().catch(error => {
        if (!failedObservation || error.name !== "ProcessObservationFailed") throw error;
      }); }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("capture failure preserves the case result and joins every original child", async () => {
  const root = mkdtempSync(join(realpathSync(tmpdir()), "case-capture-"));
  try {
    for (const failed of [false, true]) {
      const processes = new Processes({ onProgress(event) {
        if (event.phase === "node-spawn") readFileSync(join(root, "missing-progress-sink"));
      } });
      let original;
      try { readFileSync(join(root, "original-missing-input")); }
      catch (error) { original = failureReport(error); }
      const prior = { case: "native-capture", status: failed ? "fail" : "pass", ...(failed ? original : {}) };
      const first = processes.start(process.execPath, ["-e", "process.exitCode=7"]);
      const held = processes.start(process.execPath, ["-e", "process.stdin.resume()"]);
      const captures = [], pending = [retainCapture(first.closed, captures), retainCapture(held.closed, captures)];
      const recovery = [process.execPath, "--test", fileURLToPath(import.meta.url)];
      let settled = false;
      const finishing = finishCase(prior, pending, recovery);
      finishing.then(() => settled = true, () => settled = true);
      try {
        await first.exited;
        assert.equal(settled, false, "an earlier capture rejection cannot release a remaining producer");
        held.child.stdin.end();
        const result = await finishing;
        assert.equal(result.status, "fail", "a failed capture cannot qualify a pass");
        assert.equal(result.capture_failures.length, 2);
        for (const failure of result.capture_failures) assert.match(failure.cause, /ENOENT/);
        if (failed) {
          assert.equal(result.error, original.error);
          assert.equal(result.capture_failure.step, "case-capture");
        } else {
          assert.equal(result.phase, "capture");
          assert.equal(result.error, result.capture_failures[0].error);
        }
        assert.equal(processes.children.size, 0);
      } finally {
        held.child.stdin.end();
        await assert.rejects(processes.close(), error => error.name === "ProcessObservationFailed");
        await finishing;
      }
    }
  } finally { rmSync(root, { recursive: true }); }
});

test("native retirement failure preserves the operation cause and remains fatal on its own", async () => {
  const parent = realpathSync(tmpdir()), root = mkdtempSync(join(parent, "case-failure-"));
  try {
    for (const operationFails of [false, true]) {
      const phases = [], processes = new Processes({ onProgress(event) {
        if (event.phase === "node-spawn") readFileSync(join(root, "missing-progress-sink"));
      } });
      let original;
      const running = withOwnedCase({ processes, onPhase: event => phases.push(event), run: async () => {
        const child = processes.start(process.execPath, ["-e", "process.exitCode=7"]);
        const result = await child.exited;
        if (operationFails) {
          try { assert.equal(result.code, 0, "original native exit"); }
          catch (error) { original = error; throw error; }
        }
      } });
      await assert.rejects(running, error => operationFails ? error === original
        : error.name === "ProcessObservationFailed" && error.cause.code === "ENOENT");
      assert.equal(processes.children.size, 0, "failure publication must follow physical retirement");
      const retired = phases.find(event => event.phase === "retired" && event.outcome === "failed");
      assert.match(retired.failure.error, /process observation failed at node-spawn/);
      assert.match(retired.failure.cause, /ENOENT/);
      if (operationFails) {
        const run = phases.find(event => event.phase === "run" && event.outcome === "failed");
        assert.equal(run.failure.error, original.stack);
        assert.equal(run.failure.assertion.actual, "7");
        assert.equal(run.failure.assertion.expected, "0");
      }
    }
  } finally { rmSync(root, { recursive: true }); }
});

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
    await started.promise;
    assert.equal(closes, 1, "every selected case must join the supplied process owner's drain");
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

test("caller cancellation retires and joins the original child during preparation and execution", async () => {
  for (const preparation of [false, true]) {
    const scope = new Processes(), cancellation = commandCancellation();
    let events, child, completed = false;
    const entered = Promise.withResolvers();
    const hold = async () => {
      child = scope.start(process.execPath, ["-e", "console.log(JSON.stringify({ready:true}));process.stdin.resume()"]);
      events = new Spool(child.child.stdout, child.closed, { key: event => event.ready ? ["ready"] : [] });
      entered.resolve();
      await child.closed;
      completed = true;
    };
    const running = withOwnedCase({ processes: scope, signal: cancellation.signal,
      ...(preparation ? { prepare: hold, run: () => assert.fail("cancelled preparation cannot admit execution") }
        : { run: hold }) });
    running.catch(() => {});
    try {
      await entered.promise;
      await events.keyed("ready");
      assert.equal(completed, false);
      process.emit("SIGTERM");
      await assert.rejects(running, /caller cancelled command \(SIGTERM\)/);
      assert.equal(completed, true, "publication joins the original operation after physical retirement");
      assert.equal(scope.children.size, 0);
      const result = await child.closed;
      assert.ok(result.signal || result.code !== 0);
    } finally { cancellation.dispose(); await scope.close(); }
  }
  const cancellation = new AbortController(), scope = new Processes();
  const error = new Error("caller cancelled before admission");
  cancellation.abort(error);
  await assert.rejects(withOwnedCase({ processes: scope, signal: cancellation.signal,
    run: () => assert.fail("a cancelled caller cannot start work") }), caught => caught === error);
  assert.equal(scope.started, 0);
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
