import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { closeSync, mkdtempSync, openSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { runCommand } from "./run-command.mjs";

test("real commands retain complete output and terminal causes without parent capture", async () => {
  const root = mkdtempSync(join(tmpdir(), "command-output-"));
  const output = join(root, "output"), descriptor = openSync(output, "wx");
  try {
    // Instrument actual spawned streams, never substitute a process or its output.
    const script = `
      import assert from "node:assert/strict";
      import processes from "node:child_process";
      import { syncBuiltinESMExports } from "node:module";
      const spawn = processes.spawn;
      let captured = 0;
      processes.spawn = (...args) => {
        const child = spawn(...args);
        for (const stream of [child.stdout, child.stderr])
          stream?.on("data", bytes => { captured += bytes.length; });
        return child;
      };
      syncBuiltinESMExports();
      const { runCommand } = await import(${JSON.stringify(new URL("./run-command.mjs", import.meta.url).href)});
      const status = await runCommand(process.execPath, ["-e",
        "const {writeSync}=require('node:fs'); writeSync(1,Buffer.alloc(2097153,97)); writeSync(2,Buffer.from([0,255,10])); process.exitCode=23;"]);
      assert.equal(status, 23);
      assert.equal(captured, 0, "command payload must bypass parent JavaScript");
      // Restoring the old pipe-and-capture path must trip that same cost oracle.
      const control = processes.spawn(process.execPath, ["-e", "process.stdout.write('control')"],
        {stdio:["ignore","pipe","ignore"]});
      await new Promise((resolve, reject) => control.once("error", reject).once("close", resolve));
      assert.equal(captured, 7);
    `;
    const result = spawnSync(process.execPath, ["--input-type=module", "-e", script], {
      stdio: ["ignore", descriptor, descriptor], windowsHide: true,
    });
    if (result.error) throw result.error;
    const bytes = readFileSync(output);
    assert.equal(result.status, 0, bytes.subarray(-4096).toString("utf8"));
    assert.equal(bytes.length, 2097156);
    assert.ok(bytes.subarray(0, 2097153).every(byte => byte === 97));
    assert.deepEqual(bytes.subarray(2097153), Buffer.from([0, 255, 10]));
    await assert.rejects(runCommand(join(root, "missing ' command"), ["argument with ' quote"]), error => {
      assert.equal(error.cause.code, "ENOENT");
      assert.match(error.message, /step=command-run; recovery:/);
      assert.ok(error.message.includes("missing"));
      return true;
    });
    assert.equal(await runCommand(process.execPath, ["-e", "process.kill(process.pid,'SIGTERM')"]), 1);
  } finally {
    closeSync(descriptor);
    rmSync(root, { recursive: true, force: true });
  }
});
