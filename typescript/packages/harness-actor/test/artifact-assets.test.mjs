import { expect, spyOn, test } from "bun:test";
import crypto from "node:crypto";
import fs from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { wasmAssetClosure } from "../../../../scripts/artifact-assets.mjs";

test("bundled assets retain importing-module identity and hash each selected snapshot once", async () => {
  const root = fs.mkdtempSync(join(tmpdir(), "artifact-assets-"));
  const source = join(root, "source");
  const staged = join(root, "staged");
  const put = (file, bytes) => {
    fs.mkdirSync(dirname(file), { recursive: true });
    fs.writeFileSync(file, bytes);
  };
  // Opaque asset inputs, not fabricated compiler output or executable WASM fixtures.
  const first = Buffer.from("first immutable module input");
  const second = Buffer.from("other immutable module input");
  const left = join(source, "a/generated/wasm/module_bg.wasm");
  const right = join(source, "b/generated/wasm/module_bg.wasm");
  const unrelated = join(source, "unrelated/generated/wasm/module_bg.wasm");
  const entry = join(source, "entry.js");
  put(left, first);
  put(right, second);
  put(unrelated, "unrelated module with the same basename");
  put(join(source, "a/index.js"), `
    export const first = new URL("./generated/wasm/module_bg.wasm", import.meta.url);
    export const again = new URL("./generated/wasm/../wasm/%6dodule_bg.wasm", import.meta.url);
    export function unused() { return new URL("./generated/wasm/absent.wasm", import.meta.url); }
  `);
  put(join(source, "b/index.js"), `
    export const second = new URL("./generated/wasm/module_bg.wasm", import.meta.url);
  `);
  put(entry, `
    import { first, again } from "./a/index.js";
    import { second } from "./b/index.js";
    export const modules = [first, again, second];
  `);
  const recovery = "bun test ./typescript/packages/harness-actor/test/artifact-assets.test.mjs";
  const bundle = async () => {
    const closure = wasmAssetClosure(recovery);
    const built = await Bun.build({ entrypoints: [entry], target: "browser", format: "esm", plugins: [closure.plugin] });
    if (!built.success) throw new AggregateError(built.logs, `STEP test-bundle failed; recovery: ${recovery}`);
    const output = built.outputs.find(output => output.kind === "entry-point");
    if (!output) throw new Error(`STEP test-bundle failed: no entry; recovery: ${recovery}`);
    return { closure, code: closure.rewrite(await output.text()) };
  };
  // Observe the real filesystem and crypto; no operation returns a mocked result.
  const reads = spyOn(fs, "readFileSync");
  const hashes = spyOn(crypto, "createHash");
  const listings = spyOn(fs, "readdirSync");
  try {
    const built = await bundle();
    const assetReads = reads.mock.calls.map(([file]) => file instanceof URL ? fileURLToPath(file) : file)
      .filter(file => typeof file === "string" && file.endsWith(".wasm"));
    expect(assetReads.sort()).toEqual([left, right].sort());
    expect(listings).toHaveBeenCalledTimes(0);
    expect(hashes).toHaveBeenCalledTimes(2);
    expect(built.closure.assets.size).toBe(2);
    expect(built.code).not.toContain(pathToFileURL(source).href);
    const before = [...built.closure.assets.keys()].sort();
    fs.writeFileSync(right, first);
    const changed = await bundle();
    expect(changed.closure.assets.size).toBe(1);
    expect([...changed.closure.assets.keys()].sort()).not.toEqual(before);
    fs.unlinkSync(right);
    await expect(bundle()).rejects.toThrow(`STEP artifact-assets failed: ${pathToFileURL(right).href}; recovery: ${recovery}`);
    // The admitted output survives complete removal of its builder tree.
    for (const [file, bytes] of built.closure.files) put(join(staged, file), bytes);
    const output = join(staged, "entry.mjs");
    put(output, built.code);
    fs.rmSync(source, { recursive: true, force: true });
    const { modules } = await import(pathToFileURL(output).href);
    expect(modules[0].href).toBe(modules[1].href);
    expect(modules[0].href).not.toBe(modules[2].href);
    expect(Buffer.from(await Bun.file(modules[0]).arrayBuffer())).toEqual(first);
    expect(Buffer.from(await Bun.file(modules[2]).arrayBuffer())).toEqual(second);
    fs.unlinkSync(fileURLToPath(modules[2]));
    await expect(Bun.file(modules[2]).arrayBuffer()).rejects.toHaveProperty("code", "ENOENT");
  } finally {
    reads.mockRestore();
    hashes.mockRestore();
    listings.mockRestore();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
