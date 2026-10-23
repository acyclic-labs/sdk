// Enumerate the helper suite in Node: shell globbing and inherited stdin cannot
// silently select an empty suite. Qualification rejects every vacuous outcome.
import assert from "node:assert/strict";
import { readdir } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { Processes } from "./process.mjs";
import { qualified } from "./qualification.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const files = (await readdir(directory)).filter(name => name.endsWith(".test.mjs")).sort();
assert.ok(files.length > 0, "helper suite inventory is empty");
const processes = new Processes(), start = performance.now();
try {
  const result = await processes.run(process.execPath, ["--test", "--test-reporter=tap",
    ...(process.platform === "win32" ? ["--test-concurrency=1"] : []),
    ...files.map(name => join(directory, name))]);
  process.stdout.write(result.stdout);
  process.stderr.write(result.stderr);
  qualified(result, "node", files.length);
  assert.ok(performance.now() - start < 10000, "helper suite exceeded ten seconds");
} finally { await processes.close(); }
