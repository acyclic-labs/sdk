import { spawn } from "node:child_process";
import { unlink } from "node:fs/promises";
import { resolve } from "node:path";
import { browserTest } from "../../../../scripts/browser-test.mjs";

if (typeof Bun !== "undefined") {
  const built = await Bun.build({ entrypoints: [new URL("remote-session.browser.ts", import.meta.url).pathname], outdir: import.meta.dirname, naming: "remote-session.browser.js", target: "browser", plugins: [{ name: "existing-wasm", setup(build) {
    build.onResolve({ filter: /acyclic_stream_wasm\.js$/ }, () => ({ path: "../generated/wasm/acyclic_stream_wasm.js", external: true }));
  } }] });
  if (!built.success) throw new AggregateError(built.logs, "browser bundle failed");
  const node = spawn("node", [new URL(import.meta.url).pathname], { stdio: "inherit", windowsHide: true });
  const code = await new Promise((resolve, reject) => { node.once("error", reject); node.once("exit", resolve); });
  await unlink(new URL("remote-session.browser.js", import.meta.url));
  process.exit(code ?? 1);
}

const host = spawn(resolve(process.env.CARGO_TARGET_DIR ?? "target", "debug/examples/browser-test-host"), { stdio: ["ignore", "pipe", "inherit"], windowsHide: true });
const exited = new Promise(resolve => host.once("exit", resolve));
let output = "";
try {
  const endpoint = await new Promise((resolve, reject) => {
    host.stdout.on("data", data => { output += data; if (output.includes("\n")) resolve(output.split("\n")[0].trim()); });
    host.once("error", reject); host.once("exit", code => reject(new Error(`host exited ${code}`)));
  });
  console.log("PASS", await browserTest("typescript/packages/stream/test/remote-session.browser.js", endpoint));
} finally {
  if (host.exitCode === null) host.kill(); await exited;
}
