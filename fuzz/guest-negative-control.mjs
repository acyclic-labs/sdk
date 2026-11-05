/** Require the independent metadata oracle to detect real admission mutations. */
import { spawn } from "node:child_process";
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const root = fileURLToPath(new URL(".", import.meta.url));
const source = new URL("../rust/crates/actors/src/guest/runtime.rs", import.meta.url);
const original = readFileSync(source);
const text = original.toString("utf8");
const recovery = "node fuzz/guest-negative-control.mjs";
const cases = [
  ["serde bypass", "Self::new(raw.version, raw.handle)",
    "Ok(Self { version: raw.version, handle: raw.handle })",
    "handle-zero-version.json", "metadata deserialization admission"],
  ["constructor byte bound", "if value.len() > MAX_GUEST_METADATA_BYTES {", "if false {",
    "handle-overlong.json", "metadata constructor admission"],
  ["expiry canonicalization", "if expiry == 0 || !canonical_expiry(&raw.expires_at_ms) {", "if false {",
    "caller-expiry-alias.json", "metadata deserialization admission"],
];
let child;
let interrupted;
const signals = ["SIGINT", "SIGTERM"];
const handlers = signals.map(signal => {
  const handler = () => { interrupted = signal; child?.kill(signal); };
  process.on(signal, handler);
  return handler;
});
try {
  for (const [name, check, mutation, seed, failure] of cases) {
    if (interrupted) break;
    if (!text.includes(check)) {
      throw new Error(`step=${name}: update the mutation for the current source; recovery=${recovery}`);
    }
    try {
      writeFileSync(source, text.replace(check, mutation));
      const result = await new Promise(resolve => {
        const chunks = [];
        let error;
        child = spawn("cargo", ["+nightly", "fuzz", "run", "--dev", "guest_json",
          `corpus/guest_json/${seed}`, "--", "-runs=1"], { cwd: root });
        child.stdout.on("data", chunk => chunks.push(chunk));
        child.stderr.on("data", chunk => chunks.push(chunk));
        child.once("error", cause => { error = cause; });
        child.once("close", status => resolve({ status, error, output: Buffer.concat(chunks).toString("utf8") }));
      });
      child = undefined;
      if (interrupted) break;
      const { output } = result;
      if (result.error || result.status === 0 || !output.includes(failure)) {
        throw new Error(`step=${name}: admission mutation was not detected; recovery=${recovery}\n${output}`,
          { cause: result.error });
      }
      console.log(`negative control: detected ${name}`);
    } finally {
      writeFileSync(source, original);
    }
  }
} finally {
  signals.forEach((signal, index) => process.off(signal, handlers[index]));
  if (interrupted) {
    console.error(`step=interrupted signal=${interrupted}; recovery=${recovery}`);
    process.exitCode = interrupted === "SIGINT" ? 130 : 143;
  }
}
