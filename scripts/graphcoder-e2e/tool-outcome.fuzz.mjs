import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { toolOutcome } from "./assertions.mjs";

// The producer's stable public messages are the corpus, rather than copies of
// its spelling in another fixture. Mutations are never recorded as captures.
const executor = readFileSync(new URL("../../rust/crates/harness-core/src/executor/mod.rs", import.meta.url), "utf8");
const messages = [...executor.slice(executor.indexOf("impl ToolFailureKind {"))
  .split("pub const fn precedes_dispatch")[0]
  .matchAll(/"(tool [^"\n]+)"/g)].map(match => match[1]);
const shells = JSON.parse(readFileSync(new URL("./fixtures/live-shell-results.json", import.meta.url))).cases;
assert.ok(messages.length > 0, "missing production failure corpus");
assert.equal(messages.length, [...executor.slice(executor.indexOf("impl ToolFailureKind {"))
  .split("pub const fn precedes_dispatch")[0].matchAll(/Self::\w+\s*=>/g)].length,
  "every production failure arm needs a corpus message");

export function fuzz(data, outcome = toolOutcome) {
  const suffix = Buffer.from(data).toString("hex");
  let value = Buffer.from(data).toString("utf8");
  try { value = JSON.parse(value); } catch (error) { if (!(error instanceof SyntaxError)) throw error; }
  try { outcome(value); } catch (error) { assert.equal(error.code, "ERR_ASSERTION"); }
  for (const message of messages) {
    for (const value of [message, message + ": " + suffix, message + " (conflict): " + suffix]) {
      assert.throws(() => outcome(value), { code: "ERR_ASSERTION" });
      outcome(value, [message]);
      for (const allowance of ["", Object(message), "absent:" + suffix])
        assert.throws(() => outcome(value, [allowance]), { code: "ERR_ASSERTION" });
      outcome("result mentions " + value);
    }
  }
  for (const { status, event } of shells) {
    if (status === "pass") outcome(event.tool_response);
    else {
      assert.throws(() => outcome(event.tool_response), { code: "ERR_ASSERTION" });
      outcome(event.tool_response, ["tool reported success="]);
    }
  }
}
