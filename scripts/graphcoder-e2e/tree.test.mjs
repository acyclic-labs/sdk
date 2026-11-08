import assert from "node:assert/strict";
import { copyFileSync, mkdirSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { Processes, Spool } from "./process.mjs";
import { alive } from "./orphan-check.mjs";

const fixture = fileURLToPath(new URL("./tree-fixture.py", import.meta.url));
const physicalFixture = fileURLToPath(new URL("./windows-custody-fixture.py", import.meta.url));

test("acceptance Python imports leave source directories unchanged, including descendants", async () => {
  const root = mkdtempSync(join(tmpdir(), "e2e-python-import-"));
  const processes = new Processes({ transcripts: root });
  try {
    for (const directory of ["direct", "descendant"]) {
      mkdirSync(join(root, directory));
      copyFileSync(new URL("./owned_mounts.py", import.meta.url), join(root, directory, "owned_mounts.py"));
    }
    const result = await processes.run(process.platform === "win32" ? "python.exe" : "python3", ["-c", `
import subprocess, sys
sys.path.insert(0, 'direct')
import owned_mounts
print(owned_mounts.__file__, flush=True)
subprocess.run([sys.executable, '-c', 'import owned_mounts; print(owned_mounts.__file__)'], cwd='descendant', check=True)
`], { cwd: root, env: { ...process.env, PYTHONDONTWRITEBYTECODE: "", PYTHONPYCACHEPREFIX: "" } });
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.stdout.trim().split(/\r?\n/).length, 2, "both real imports completed");
    for (const directory of ["direct", "descendant"])
      assert.deepEqual(readdirSync(join(root, directory)), ["owned_mounts.py"],
        "Python imports must not create source-tree bytecode");
  } finally { await processes.close(); rmSync(root, { recursive: true, force: true }); }
});

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
          pids = (await new Spool(running.child.stdout, running.closed).next(event => event.pids)).pids;
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

if (process.platform === "win32") test("native custody retains output until kernel process teardown completes", async () => {
  const processes = new Processes();
  try {
    const interpreter = await processes.run("python.exe", ["-c", "import sys; print(sys.executable)"]);
    assert.equal(interpreter.code, 0, interpreter.stderr);
    const result = await processes.run(interpreter.stdout.trim(), ["-X", "utf8", physicalFixture, process.env.E2E_PROCESS_HOST]);
    assert.equal(result.code, 0, result.stderr);
    const evidence = JSON.parse(result.stdout.trim());
    assert.equal(evidence.root_wait_before_continue, 258);
    assert.equal(evidence.output_lease_held, true);
    assert.equal(evidence.root_wait_after_continue, 0);
  } finally { await processes.close(); }
});
