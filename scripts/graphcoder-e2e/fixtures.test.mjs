import assert from "node:assert/strict";
import test from "node:test";
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

test("large byte fixtures remain compact and bounded", async () => {
  let actual;
  await seed("fixture", { large: { repeat: "x", count: 1_048_576 } }, {
    mkdir: async () => {}, writeFile: async (_, bytes) => { actual = bytes; },
  });
  assert.equal(actual.length, 1_048_576);
  assert.ok(actual.every(byte => byte === 120));
  await assert.rejects(seed("fixture", { large: { repeat: "x", count: Infinity } }, {
    mkdir: async () => {}, writeFile: () => assert.fail("unbounded bytes reached I/O"),
  }), /bounded byte count/);
});
