import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";

export function expand(value, bindings) {
  if (typeof value === "string") return value.replace(/\{\{(support|workspace|url|repository|shell_tool)\}\}/g,
    (token, key) => bindings[key] ?? token);
  if (Array.isArray(value)) return value.map(item => expand(item, bindings));
  if (value && typeof value === "object") return Object.fromEntries(Object.entries(value)
    .map(([key, item]) => [key, expand(item, bindings)]));
  return value;
}

// Fixture disk latency must not block the process/event owner or serialize
// independent files before the concurrent cases have even started.
export async function seed(root, files = {}, io = { mkdir, writeFile }) {
  await Promise.all(Object.entries(files).map(async ([path, value]) => {
    assert.ok(!isAbsolute(path) && !path.split(/[\\/]/).includes(".."), `unsafe seed path ${path}`);
    const target = join(root, path);
    await io.mkdir(dirname(target), { recursive: true });
    await io.writeFile(target, typeof value === "string" ? value : Buffer.from(value.base64, "base64"));
  }));
}
