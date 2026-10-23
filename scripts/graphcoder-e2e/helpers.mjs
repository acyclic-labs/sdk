// Enumerate the helper suite in Node: shell globbing and inherited stdin cannot
// silently select an empty suite. Qualification rejects every vacuous outcome.
import assert from "node:assert/strict";
import { readdir } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { performance } from "node:perf_hooks";
import { Processes } from "./process.mjs";
import { qualified } from "./qualification.mjs";

export async function runHelperSuite(files, { signal = AbortSignal.timeout(9000),
  processes = new Processes(), publish = result => {
    process.stdout.write(result.stdout); process.stderr.write(result.stderr);
  } } = {}) {
  assert.ok(files.length > 0, "helper suite inventory is empty");
  const start = performance.now();
  let draining;
  const cancel = () => {
    draining ??= processes.close();
    draining.catch(() => {}); // The finally barrier observes cleanup errors.
  };
  signal.addEventListener("abort", cancel, { once: true });
  try {
    signal.throwIfAborted();
    const env = { ...process.env };
    delete env.NODE_TEST_CONTEXT; // A new suite owns its runner, never its caller's worker context.
    const result = await processes.runOwned(process.execPath, ["--test", "--test-reporter=tap",
      ...(process.platform === "win32" ? ["--test-concurrency=1"] : []), ...files], { env });
    publish(result);
    signal.throwIfAborted(); // A late successful summary cannot erase cancellation.
    qualified(result, "node", files.length);
    assert.ok(performance.now() - start < 10000, "helper suite exceeded ten seconds");
    return result;
  } finally {
    signal.removeEventListener("abort", cancel);
    await (draining ?? processes.close());
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const directory = dirname(fileURLToPath(import.meta.url));
  const files = (await readdir(directory)).filter(name => name.endsWith(".test.mjs")).sort();
  await runHelperSuite(files.map(name => join(directory, name)));
}
