// A real native tool descendant. The request commit is the kill barrier; an
// outstanding response keeps the descendant alive without a sleep or a timer.
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const [mode] = process.argv.slice(2);
const url = `${process.env.E2E_MOCK_URL}/orphan-ready`;
if (mode === "tree") {
  const child = spawn(process.execPath, [fileURLToPath(import.meta.url), "leaf"],
    { windowsHide: true, stdio: "inherit" });
  child.on("error", error => { throw error; });
  child.on("exit", code => { process.exitCode = code ?? 1; });
} else if (mode === "leaf") {
  const response = await fetch(url, { method: "POST", headers: { "content-type": "application/json" },
    body: JSON.stringify({ pid: process.pid, parent_pid: process.ppid }) });
  await response.text();
  throw new Error("orphan probe was released instead of fenced");
} else throw new Error("usage: orphan.mjs tree|leaf (E2E_MOCK_URL required)");
