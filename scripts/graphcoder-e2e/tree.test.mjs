import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes, Events } from "./process.mjs";
import { alive } from "./orphan-check.mjs";

const fixture = fileURLToPath(new URL("./tree-fixture.py", import.meta.url));
function reap(pids) {
  for (const pid of pids) {
    try { process.kill(pid, "SIGKILL"); } catch (error) { if (error.code !== "ESRCH") throw error; }
  }
}

test("external acceptance owns service descendants on normal exit and cancellation", async () => {
  const raw = new Processes();
  let pids = [];
  try {
    const interpreter = await raw.run(process.platform === "win32" ? "python.exe" : "python3",
      ["-c", "import sys; print(sys.executable)"]);
    assert.equal(interpreter.code, 0, interpreter.stderr);
    const python = interpreter.stdout.trim();
    // Negative control: root-only ownership leaves both service generations.
    const before = await raw.run(python, [fixture, "exit"]);
    assert.equal(before.code, 0, before.stderr);
    pids = JSON.parse(before.stdout).pids;
    await raw.close();
    for (const pid of pids.slice(0, -1)) assert.equal(alive(pid), true);
    reap(pids); pids = [];

    for (const separateGroup of [false, true]) for (const mode of ["exit", "wait"]) {
      const owned = new Processes();
      const args = [fixture, mode, ...(separateGroup ? ["--separate-group"] : [])];
      try {
        if (mode === "exit") {
          const result = await owned.runOwned(python, args);
          pids = JSON.parse(result.stdout).pids;
          assert.equal(result.code, 0, result.stderr);
        } else {
          const running = owned.startOwned(python, args);
          pids = (await new Events(running.child.stdout, running.closed).wait(event => event.pids)).pids;
          await owned.close();
          const result = await running.closed;
          assert.ok(result.signal || result.code !== 0, "cancellation must remain a failed acceptance run");
        }
        for (const pid of pids) assert.equal(alive(pid), false, `service ${pid} survived ${mode}, separate group=${separateGroup}`);
        pids = [];
      } finally { reap(pids); pids = []; await owned.close(); }
    }
  } finally { reap(pids); await raw.close(); }
});
