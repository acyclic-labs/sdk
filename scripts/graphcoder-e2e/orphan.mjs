// A real native tool descendant. The request commit is the kill barrier; an
// outstanding response keeps the descendant alive without a sleep or a timer.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const [mode, endpoint] = process.argv.slice(2);
if (!endpoint || !["tree", "leaf"].includes(mode))
  throw new Error("usage: orphan.mjs tree|leaf <mock-url> (explicit endpoint required)");
const url = `${endpoint}/orphan-ready`;
if (mode === "tree") {
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "leaf", endpoint],
    { windowsHide: true, stdio: "inherit" });
  child.on("error", error => { throw error; });
  child.on("exit", code => { process.exitCode = code ?? 1; });
} else if (mode === "leaf") {
  const response = await fetch(url, { method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ pid: process.pid, parent_pid: process.ppid }) });
  await response.text();
  throw new Error("orphan probe was released instead of fenced");
}
