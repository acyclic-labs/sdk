import assert from "node:assert/strict";
import { readFile, rename } from "node:fs/promises";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

// The installed closure uses the shipping workspace executable and original
// owner. Metadata comes from this native host's session export, not a mock
// session. Only SDK callback scheduling is controlled here.
const recovery = "node --test --test-concurrency=1 plugin/tests/hosts/opencode-workspace.test.mjs";
let retained;
let announcement;
try {
  const [extension, directory, encoded] = process.argv.slice(2);
  const info = JSON.parse(encoded);
  assert.ok(info.id && !info.parentID, "original native root metadata absent");
  const { default: acyclic } = await import(pathToFileURL(extension).href);
  let reads = 0;
  const hooks = await acyclic({ directory, client: { session: { get: async ({ path }) => {
    assert.equal(path.id, info.id);
    reads++;
    return { data: info };
  } } } });
  for (const original of [undefined, "allow", "ask", "deny",
    { edit: "allow", task: "allow", external_directory: "ask" }]) {
    const permission = original === undefined ? {} : typeof original === "string"
      ? { "*": original } : { ...original };
    const config = original === undefined ? {} : { permission: structuredClone(original) };
    await hooks.config(config);
    assert.deepEqual(config.permission, { ...permission, task: "deny" },
      "native default permissions changed outside the delegation fence");
  }
  await assert.rejects(hooks["tool.execute.before"]({ tool: "task", sessionID: info.id }),
    /step=admit native delegation: use acyclic-sessions_spawn/);
  await hooks["tool.execute.before"]({ tool: "read", sessionID: info.id });
  // Withdraw the actual immutable owner announcement, retaining its original
  // bytes. A fresh shipping client now observes an owner that is not ready.
  announcement = join(directory, ".acyclic-sdk", "host-files", "owner.json");
  const original = join(directory, "retained-owner-announcement.json");
  await rename(announcement, original); retained = original;
  let refusal;
  try {
    await assert.rejects(hooks["chat.message"]({ sessionID: info.id }), error => {
      refusal = error;
      assert.match(error.message, /no running GraphCoder workspace owner/);
      return true;
    });
  } finally {
    await rename(retained, announcement); retained = undefined;
  }
  console.log(JSON.stringify({ event: "native-admission-initial-refused" }));
  // The same extension instance and session must admit after the actual
  // workspace producer recovers. Concurrent callers share the new attempt.
  await Promise.all([hooks["chat.message"]({ sessionID: info.id }),
    hooks["chat.message"]({ sessionID: info.id })]);
  assert.equal(reads, 2, "failed native admission was cached or concurrent admission duplicated");

  const release = Promise.withResolvers();
  const capture = await readFile(new URL("../../../rust/crates/harness/src/local/host_process/fixtures/opencode-events.jsonl", import.meta.url), "utf8");
  const previous = capture.trim().split("\n").map(line => JSON.parse(line))
    .find(event => event.type === "session.created").properties.info;
  assert.notEqual(previous.id, info.id);
  let calls = 0;
  const concurrent = await acyclic({ directory, client: { session: { get: async ({ path }) => {
    calls++;
    if (path.id === previous.id) { await release.promise; throw refusal; }
    assert.equal(path.id, info.id);
    return { data: info };
  } } } });
  const earlier = concurrent["chat.message"]({ sessionID: previous.id });
  const rejected = assert.rejects(earlier, error => error === refusal);
  try {
    await concurrent["chat.message"]({ sessionID: info.id });
  } finally { release.resolve(); }
  await rejected;
  await concurrent["chat.message"]({ sessionID: info.id });
  assert.equal(calls, 2, "an older failure evicted the newer native admission");
  console.log(JSON.stringify({ event: "native-admission-recovered", reads, calls }));
} catch (error) {
  console.error(`step=qualify installed OpenCode admission: ${error.stack}; recovery=${recovery}`);
  process.exitCode = 1;
} finally {
  if (retained) {
    try { await rename(retained, announcement); }
    catch (error) {
      console.error(`step=restore original native owner announcement: ${error.message}; recovery=rename ${JSON.stringify(retained)} to ${JSON.stringify(announcement)}`);
      process.exitCode = 1;
    }
  }
}
