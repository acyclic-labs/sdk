// Generate the portable Node bundle from the SDK's shared WASM target.
import { execFileSync } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { bazelCommand } from "./bazel-executables.mjs";

const root = fileURLToPath(new URL("../", import.meta.url));
const target = "//wasm/backoff:backoff";
const download = "--remote_download_outputs=toplevel";
/** @param {...string} args */
const bazel = (...args) => execFileSync(bazelCommand(), args, { cwd: root, encoding: "utf8" }).trim();
execFileSync(bazelCommand(), ["build", download, target], { cwd: root, stdio: "inherit" });
const outputs = bazel("cquery", download, "--output=files", target).split(/\r?\n/);
if (outputs.length !== 1 || !outputs[0]) throw new Error(`expected one bindings directory from ${target}`);
const bindings = resolve(bazel("info", "execution_root"), outputs[0]);
const wasm = readFileSync(resolve(bindings, "acyclic_backoff_wasm_bg.wasm")).toString("base64");
const glue = readFileSync(resolve(bindings, "acyclic_backoff_wasm.js"), "utf8").trimEnd();
const exportsDelay = "export function retryDelayMs(";
if (!glue.includes(exportsDelay)) throw new Error("SDK backoff bindings omit retryDelayMs");

writeFileSync(new URL("../arena/packages/arena/src/retry-spacing.mjs", import.meta.url), `// @ts-nocheck
// Generated from //wasm/backoff by scripts/generate-arena-backoff.mjs.
${glue.replace(exportsDelay, "function computeRetryDelayMs(")}

let initialized = false;
/** @param {number} base @param {number} cap @param {number} doublings @param {number} sample */
export function retryDelayMs(base, cap, doublings, sample) {
    if (!initialized) {
        initSync({ module: Buffer.from(${JSON.stringify(wasm)}, "base64") });
        initialized = true;
    }
    return computeRetryDelayMs(base, cap, doublings, sample);
}
`);
