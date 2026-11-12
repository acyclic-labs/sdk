import assert from "node:assert/strict";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";
import { fileURLToPath } from "node:url";

export function expand(value, bindings) {
  if (typeof value === "string") return value.replace(/\{\{(support|workspace|url|repository|provider|session:[a-z][a-z0-9_-]*)\}\}/g,
    (token, key) => key.startsWith("session:") ? bindings.sessions?.[key.slice(8)] ?? token : bindings[key] ?? token);
  if (Array.isArray(value)) return value.map(item => expand(item, bindings));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .map(([key, item]) => [key, expand(item, bindings)]));
  return value;
}

// Fixture disk latency must not block the process/event owner or serialize
// independent files before the concurrent cases have even started.
export async function seed(root, files = {}, io = { chmod, mkdir, readFile, writeFile }) {
  await Promise.all(Object.entries(files).map(async ([path, value]) => {
    assert.ok(!isAbsolute(path) && !path.split(/[\\/]/).includes(".."), `unsafe seed path ${path}`);
    const target = join(root, path);
    let bytes = value;
    if (typeof value !== "string") {
      if (value.binary_env !== undefined) {
        const binary = process.env[value.binary_env];
        assert.ok(binary && isAbsolute(binary), `missing absolute fixture binary ${value.binary_env}`);
        bytes = await io.readFile(binary);
      } else if (value.source !== undefined) {
        assert.ok(typeof value.source === "string" && !isAbsolute(value.source) &&
          !value.source.split(/[\\/]/).includes(".."), `unsafe fixture source ${value.source}`);
        // Executed support programs belong to the granted workspace fixture,
        // rather than relying on ambient reads of the controller's checkout.
        bytes = await io.readFile(join(dirname(fileURLToPath(import.meta.url)), value.source));
      } else if (value.repeat !== undefined) {
        assert.ok(Number.isSafeInteger(value.count) && value.count >= 0,
          "fixture repetition requires a nonnegative safe byte count");
        assert.ok(typeof value.repeat === "string" && Buffer.byteLength(value.repeat) === 1,
          "fixture repetition requires one byte");
        bytes = Buffer.alloc(value.count, value.repeat);
      } else bytes = Buffer.from(value.base64, "base64");
    }
    await io.mkdir(dirname(target), { recursive: true });
    await io.writeFile(target, bytes);
    if (value.binary_env !== undefined) await io.chmod(target, 0o755);
  }));
}
