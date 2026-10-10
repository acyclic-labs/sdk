// Reuse the existing actual Chromium gate for the installed package consumer.
import { spawn } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { readFile, unlink, copyFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const consumer = process.argv[2];
if (consumer) {
  for (const name of ["acyclic_harness_wasm.js", "acyclic_harness_wasm_bg.wasm"]) {
    const archived = await readFile(resolve(consumer, "node_modules/@acyclic-labs/harness/generated/wasm", name));
    const served = await readFile(new URL(`../generated/wasm/${name}`, import.meta.url));
    const hash = bytes => createHash("sha256").update(bytes).digest("hex");
    if (hash(archived) !== hash(served)) throw new Error(`installed artifact differs: ${name}`);
  }
  await copyFile(new URL("client-demand.browser.ts", import.meta.url), resolve(consumer, "client-demand.browser.ts"));
}
const built = await Bun.build({
  entrypoints: [consumer ? resolve(consumer, "client-demand.browser.ts") : fileURLToPath(new URL("client-demand.browser.ts", import.meta.url))],
  outdir: fileURLToPath(new URL(".", import.meta.url)),
  naming: "client-demand.browser.js", target: "browser",
  plugins: [{ name: "existing-wasm-module", setup(builder) {
    builder.onResolve({ filter: /acyclic_harness_wasm\.js$/ }, () => ({ path: "../generated/wasm/acyclic_harness_wasm.js", external: true }));
    builder.onResolve({ filter: /^node:/ }, args => ({ path: args.path, external: true }));
  } }],
});
if (!built.success) throw new AggregateError(built.logs, "demand bundle failed");
const driver = spawn("node", [fileURLToPath(new URL("../../filesystem/test/browser-qualify.mjs", import.meta.url)), "../harness/test/browser-client-demand.html"], { stdio: "inherit", windowsHide: true });
try {
  process.exitCode = await new Promise((resolve, reject) => {
    driver.once("error", reject);
    driver.once("exit", code => resolve(code ?? 1));
  });
} finally { await unlink(new URL("client-demand.browser.js", import.meta.url)); }
