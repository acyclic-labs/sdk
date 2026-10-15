import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";
import { qualifyMcpLoom } from "./qualify-mcp-loom.mjs";

const artifact = { reason: "compiler-artifact", target: { name: "mcp-loom" }, profile: { test: true }, executable: "model-binary" };
const summary = (passed, ignored = 0, filtered = 0) => `test result: ok. ${passed} passed; 0 failed; ${ignored} ignored; 0 measured; ${filtered} filtered out;\n`;

test("qualification instruments only the selected target and executes its built binary", () => {
  const calls = [];
  qualifyMcpLoom(["--all-features"], (command, args) => {
    calls.push([command, args]);
    return { status: 0, stderr: "", stdout: calls.length === 1 ? JSON.stringify(artifact) : summary(6) };
  });
  assert.deepEqual(calls, [
    ["cargo", ["rustc", "--locked", "--message-format=json", "-p", "acyclic-harness", "--test", "mcp-loom", "--all-features", "--", "--cfg", "loom"]],
    ["model-binary", []],
  ]);
  const gate = readFileSync("scripts/qualify-ci.sh", "utf8").match(/  gate\)\n([\s\S]*?);;/)?.[1];
  assert.match(gate, /node scripts\/qualify-mcp-loom\.mjs --all-features/);
});

test("qualification rejects zero, skipped, filtered and failed model execution", () => {
  for (const [status, output] of [[0, summary(0)], [0, summary(5, 1)], [0, summary(5, 0, 1)], [1, summary(6)]]) {
    assert.throws(() => qualifyMcpLoom(undefined, command => ({
      status: command === "cargo" ? 0 : status,
      stdout: command === "cargo" ? JSON.stringify(artifact) : output,
      stderr: "",
    })), /MCP Loom (qualification requires|tests failed)/);
  }
});

test("qualification rejects failed builds and missing or ambiguous artifacts", () => {
  for (const [status, stdout] of [[1, ""], [0, ""], [0, `${JSON.stringify(artifact)}\n${JSON.stringify(artifact)}`]]) {
    assert.throws(() => qualifyMcpLoom(undefined, () => ({ status, stdout, stderr: "" })),
      /MCP Loom build (failed|must produce)/);
  }
});
