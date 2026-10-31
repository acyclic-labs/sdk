// A real native tool descendant. The request commit is the kill barrier; an
// outstanding response keeps the descendant alive without a sleep or a timer.
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const [mode, endpointFile, ...extra] = process.argv.slice(2);
function refused(step, error) {
  console.error(`orphan-probe step=${step} failed: ${error.message}; recovery command: `
    + `node "${fileURLToPath(import.meta.url)}" tree "${endpointFile ?? "orphan-endpoint.txt"}"`);
  process.exitCode = 1;
}
let step = "admit-input";
try {
  if (!endpointFile || extra.length || !["tree", "leaf"].includes(mode))
    throw new Error("an explicit endpoint file is required");
  if (mode === "tree") {
    const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "leaf", endpointFile],
      { windowsHide: true, stdio: "inherit" });
    child.on("error", error => refused("spawn-leaf", error));
    child.on("exit", code => { process.exitCode = code ?? 1; });
  } else {
    step = "read-endpoint";
    const url = `${(await readFile(endpointFile, "utf8")).trim()}/orphan-ready`;
    step = "request-barrier";
    const response = await fetch(url, { method: "POST", headers: { "content-type": "application/json" },
      body: JSON.stringify({ pid: process.pid, parent_pid: process.ppid }) });
    await response.text();
    step = "hold-barrier";
    throw new Error("orphan probe was released instead of fenced");
  }
} catch (error) {
  refused(step, error);
}
