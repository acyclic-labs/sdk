import assert from "node:assert/strict";
import test from "node:test";
import { mkdir, mkdtemp, open, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { seed, expand } from "./fixtures.mjs";

test("fixture expansion reaches nested model arguments without evaluating runtime request templates", () => {
  const model = { steps: [{ tools: [{ name: "shell", arguments: { command: 'node "{{support}}/orphan.mjs"',
    path: "{{workspace}}/file", request: "{{request:/input/0}}" } }] }] };
  const actual = expand(model, { support: "/support", workspace: "/workspace" });
  assert.equal(actual.steps[0].tools[0].name, "shell");
  assert.equal(actual.steps[0].tools[0].arguments.command, 'node "/support/orphan.mjs"');
  assert.equal(actual.steps[0].tools[0].arguments.path, "/workspace/file");
  assert.equal(actual.steps[0].tools[0].arguments.request, "{{request:/input/0}}");
  assert.equal(model.steps[0].tools[0].arguments.path, "{{workspace}}/file", "case data stays immutable");
});

test("fixture files enter I/O together and finish before launching a case", async () => {
  let release, entered = 0;
  const gate = new Promise(resolve => { release = resolve; });
  const written = [];
  const io = {
    mkdir: () => { entered++; return gate; },
    writeFile: async (path, bytes) => { written.push([path, bytes]); },
  };
  const pending = seed("fixture", { "one/a": "text", "two/b": { base64: "AAE=" } }, io);
  try {
    assert.equal(entered, 2, "one slow directory must not hold up another file");
    assert.equal(written.length, 0, "writes must await their directories");
  } finally { release(); await pending; }
  assert.equal(written.length, 2);
  assert.equal(written[0][1], "text");
  assert.deepEqual(written[1][1], Buffer.from([0, 1]));
});

test("unsafe fixture paths fail before their I/O begins", async () => {
  for (const path of ["../escape", "a/../escape", "a\\..\\escape"]) {
    await assert.rejects(seed("fixture", { [path]: "bad" }, {
      mkdir: () => assert.fail("unsafe path reached mkdir"),
      writeFile: () => assert.fail("unsafe path reached writeFile"),
    }), /unsafe seed path/);
  }
});

test("byte fixtures use requested capacity and reject invalid specifications before I/O", async () => {
  const root = await mkdtemp(join(tmpdir(), "fixture-capacity-"));
  const calls = { mkdir: 0, writeFile: 0 };
  const io = {
    mkdir: (...args) => { calls.mkdir++; return mkdir(...args); },
    writeFile: (...args) => { calls.writeFile++; return writeFile(...args); },
  };
  try {
    const count = 4 * 1024 * 1024 + 1;
    await seed(root, { large: { repeat: "x", count } }, io);
    assert.deepEqual(calls, { mkdir: 1, writeFile: 1 });
    const file = await open(join(root, "large"));
    try {
      assert.equal((await file.stat()).size, count);
      const byte = Buffer.alloc(1);
      for (const position of [0, count - 1]) {
        assert.equal((await file.read(byte, 0, 1, position)).bytesRead, 1);
        assert.equal(byte[0], 120);
      }
    } finally { await file.close(); }
    for (const value of [{ repeat: "x", count: Infinity }, { repeat: "x", count: -1 },
      { repeat: "x", count: 0.5 }, { repeat: "λ", count: 1 }]) {
      await assert.rejects(seed(root, { "rejected/file": value }, io), /fixture repetition requires/);
      assert.deepEqual(calls, { mkdir: 1, writeFile: 1 }, "invalid bytes must not enter I/O");
      await assert.rejects(stat(join(root, "rejected")), { code: "ENOENT" });
    }
  } finally { await rm(root, { recursive: true }); }
});
