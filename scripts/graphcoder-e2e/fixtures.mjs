import assert from "node:assert/strict";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";

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
