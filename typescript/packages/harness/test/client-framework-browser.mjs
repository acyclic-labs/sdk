// Build actual installed framework consumers, then use the existing browser gate.
import { spawn } from "node:child_process";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import { compile } from "svelte/compiler";

const consumer = process.argv[2];
if (consumer) {
  // The existing driver serves workspace assets. A fresh archive consumer must
  // carry the identical WASM/JS pair before using that served pair in Chromium.
  for (const name of ["acyclic_harness_wasm.js", "acyclic_harness_wasm_bg.wasm"]) {
    const archived = await readFile(join(consumer, "node_modules/@acyclic-labs/harness/generated/wasm", name));
    const served = await readFile(new URL(`../generated/wasm/${name}`, import.meta.url));
    const hash = bytes => createHash("sha256").update(bytes).digest("hex");
    if (hash(archived) !== hash(served)) throw new Error(`installed consumer artifact differs: ${name}`);
  }
}

const built = await Bun.build({
  entrypoints: [consumer ? join(consumer, "client-frameworks.browser.ts") : fileURLToPath(new URL("client-frameworks.browser.ts", import.meta.url))],
  outdir: fileURLToPath(new URL(".", import.meta.url)),
  naming: "client-frameworks.browser.js",
  target: "browser",
  define: { "process.env.NODE_ENV": JSON.stringify("development") },
  plugins: [{ name: "installed-framework-consumer", setup(builder) {
    builder.onResolve({ filter: /acyclic_harness_wasm\.js$/ }, () => ({ path: "../generated/wasm/acyclic_harness_wasm.js", external: true }));
    builder.onResolve({ filter: /^node:/ }, args => ({ path: args.path, external: true }));
    builder.onLoad({ filter: /\.svelte$/ }, async args => ({ contents: compile(await readFile(args.path, "utf8"), { filename: args.path, generate: "client", dev: true }).js.code, loader: "js" }));
  } }],
});
if (!built.success) throw new AggregateError(built.logs, "framework bundle failed");
const driver = spawn("node", [fileURLToPath(new URL("../../filesystem/test/browser-qualify.mjs", import.meta.url)), "../harness/test/browser-client-frameworks.html"], { stdio: "inherit", windowsHide: true });
process.exitCode = await new Promise((resolve, reject) => {
  driver.once("error", reject);
  driver.once("exit", code => resolve(code ?? 1));
});
