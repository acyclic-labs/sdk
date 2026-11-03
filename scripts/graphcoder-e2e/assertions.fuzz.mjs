import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { jsonlReply, messageReply, toolOutcome } from "./assertions.mjs";

const capture = name => JSON.parse(readFileSync(new URL(`./fixtures/${name}.json`, import.meta.url)));
const original = capture("live-nested-settlement-failure").cases.map(({ event }) => event.tool_response);
const vm = capture("live-code-failure").event.tool_response;
const stdout = capture("shipping-output").captures["jsonl-reopen"].stdout;
const reply = JSON.parse(stdout);

// These are mutations of original captured envelopes, not replacement wire
// fixtures. The independent contracts are exact prompt identity, one terminal,
// and an allowance for EACH retained failure; program values remain data.
export function fuzz(data) {
  const bytes = Buffer.from(data), text = bytes.toString("utf8");
  let value;
  try { value = JSON.parse(text); }
  catch (error) { if (!(error instanceof SyntaxError)) throw error; value = text; }
  toolOutcome({ version: 2, context: [], output: { value } });
  assert.throws(() => messageReply({ ...reply, id: reply.id + ":" + bytes.toString("hex") }, reply.id),
    { code: "ERR_ASSERTION" });
  assert.throws(() => jsonlReply(stdout + stdout + text, reply.id));

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
