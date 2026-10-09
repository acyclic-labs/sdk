import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { nativeFamily } from "../native-family.mjs";

/** Synthetic archive evidence only; these bytes do not qualify a WASM runtime. */
export async function writeWasmReceiptFixture(parent, key, source, inventory) {
  const family = nativeFamily(key);
  const hash = bytes => `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
  const wasm = Buffer.from("synthetic module bytes; receipt admission only");
  const wasmReceipt = {
    schema: "acyclic.wasm-build-receipt.v1", family: key, package: family.wasmPackageName, version: family.version,
    source_commit: source, source_sha256: inventory.source_sha256, source_files: inventory.source_files,
    build: {
      cargo: { version: "synthetic cargo" },
      rustc: { invocation: { source: "rustc-invocation", rustc: "fixture-rustc", target: "wasm32-unknown-unknown" }, identity: { command: "fixture-rustc", output: "synthetic rustc", executable_sha256: `sha256:${"f".repeat(64)}` } },
      wasm_bindgen: { version: "synthetic bindgen" }, module_sha256: hash(wasm),
    },
    artifacts: [{ path: "generated/wasm/fixture_bg.wasm", bytes: wasm.length, sha256: hash(wasm) }],
  };
  const wasmReceiptBytes = JSON.stringify(wasmReceipt);
  await mkdir(join(parent, "generated/wasm"), { recursive: true });
  await writeFile(join(parent, "generated/wasm/fixture_bg.wasm"), wasm);
  await writeFile(join(parent, "generated/wasm/producer-receipt.json"), wasmReceiptBytes);
  return { wasm, wasmReceipt, wasmReceiptBytes, wasmReceiptSha256: hash(wasmReceiptBytes) };
}
