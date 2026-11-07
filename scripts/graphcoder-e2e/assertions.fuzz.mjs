import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileRead, jsonlReply, messageReply, toolOutcome } from "./assertions.mjs";

const capture = name => JSON.parse(readFileSync(new URL(`./fixtures/${name}.json`, import.meta.url)));
const original = capture("live-nested-settlement-failure").cases.map(({ event }) => event.tool_response);
const vm = capture("live-code-failure").event.tool_response;
const stdout = capture("shipping-output").captures["jsonl-reopen"].stdout;
const reply = JSON.parse(stdout);
const reads = capture("live-file-read-results").cases.map(({ event }) => event);
const shells = capture("live-shell-results").cases.map(({ event }) => event.tool_response);
// Derive diagnostic mutations from the shipped enum's rendering, rather than
// inventing a parallel list of tool failures or another provider capture.
const executor = readFileSync(new URL("../../rust/crates/harness-core/src/executor/mod.rs", import.meta.url), "utf8");
const kinds = /pub enum ToolFailureKind \{([^]*?)\n\}/.exec(executor)[1];
const rendering = executor.slice(executor.indexOf("pub const fn message(self)"), executor.indexOf("pub const fn precedes_dispatch(self)"));
const failures = [...rendering.matchAll(/"(tool [^"]*)"/g)].map(match => match[1]);
assert.equal(failures.length, [...kinds.matchAll(/^\s+\w+,$/gm)].length,
  "each shipped tool failure kind needs its original rendering");

function rejectedReply(value, id) {
  assert.throws(() => messageReply(value, id), { code: "ERR_ASSERTION" });
  assert.throws(() => jsonlReply(JSON.stringify(value), id), { code: "ERR_ASSERTION" });
}

// These are mutations of original captured envelopes, not replacement wire
// fixtures. The independent contracts are exact prompt identity, one terminal,
// and an allowance for EACH retained failure; program values remain data.
export function fuzz(data) {
  const bytes = Buffer.from(data), text = bytes.toString("utf8");
  let value;
  try { value = JSON.parse(text); }
  catch (error) { if (!(error instanceof SyntaxError)) throw error; value = text; }
  toolOutcome({ version: 2, context: [], output: { value } });
  for (const message of failures) for (const failure of [message, message + ": " + vm.output.error + text]) {
    for (const allowed of [undefined, [""], [null]])
      assert.throws(() => toolOutcome(failure, allowed), { code: "ERR_ASSERTION" });
    toolOutcome(failure, [message]);
    toolOutcome("ordinary output: " + failure);
  }
  const selected = { ...reply, id: reply.id + ":" + bytes.toString("hex"), text };
  assert.equal(messageReply(selected, selected.id), selected);
  assert.deepEqual(jsonlReply(JSON.stringify(selected) + "\n", selected.id), selected);
  rejectedReply({ ...selected, id: selected.id + ":" }, selected.id);
  for (const inbox of [undefined, null, -1, 0.5, Number.MAX_SAFE_INTEGER + 1, text])
    rejectedReply({ ...selected, inbox }, selected.id);
  for (const field of ["id", "text"])
    rejectedReply({ ...selected, [field]: { value } }, selected.id);
  rejectedReply({ ...selected, error: { value } }, selected.id);
  // Generated error-variant mutation of the captured reply, retaining the
  // original VM refusal bytes. It must never need a committed inbox or answer.
  const failed = { id: selected.id, error: vm.output.error + text };
  assert.equal(messageReply(failed, failed.id), failed);
  assert.deepEqual(jsonlReply(JSON.stringify(failed), failed.id), failed);
  assert.throws(() => jsonlReply(stdout + stdout + text, reply.id));

  for (const { tool_input, tool_response } of reads) {
    const kind = Object.keys(tool_response.file)[0], source = tool_response.file[kind];
    assert.equal(fileRead(tool_response, tool_input.path, tool_response.text), source);
    for (const mutate of [
      result => { result.text += ":" + text; },
      result => { delete result.file; },
      result => { result.file.other = result.file[kind]; },
      result => { result.file = { other: result.file[kind] }; },
      result => { result.file[kind].path += ":" + text; },
      result => { delete result.file[kind].volume; },
      result => { result.file[kind].volume.id = ""; },
      result => { delete result.file[kind][kind === "authored" ? "generation" : "version"]; },
    ]) {
      const changed = structuredClone(tool_response); mutate(changed);
      assert.throws(() => fileRead(changed, tool_input.path, tool_response.text), { code: "ERR_ASSERTION" });
    }
  }

  toolOutcome(shells[0]);
  for (const shell of [shells[1], { ...shells[1], success: true }, { ...shells[1], exit_code: 0 }]) {
    assert.throws(() => toolOutcome(shell), { code: "ERR_ASSERTION" });
    toolOutcome(shell, ["tool reported success="]);
  }
  for (const error of [vm.output.error + text, "", false, { value }]) {
    assert.throws(() => toolOutcome({ error }), { code: "ERR_ASSERTION" });
    toolOutcome({ error }, [JSON.stringify(error)]);
  }

  const first = original[0].settled[0], second = structuredClone(original[1].settled[0]);
  const allowed = first.phase.done.outcome.failed.message, refused = vm.output.error;
  second.phase.done.outcome.failed = { message: refused };
  for (const settled of [[first, second], [second, first]]) {
    const envelope = { ...original[0], settled };
    assert.throws(() => toolOutcome(envelope, [allowed]), error => error.code === "ERR_ASSERTION"
      && error.message.includes(refused));
    toolOutcome(envelope, [allowed, refused]);
  }
  for (const envelope of [
    { ...original[0], error: allowed, settled: [second] },
    { ...vm, error: allowed },
    { error: allowed, success: false },
  ]) assert.throws(() => toolOutcome(envelope, [allowed]), { code: "ERR_ASSERTION" });
}
