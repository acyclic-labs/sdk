import assert from "node:assert/strict";
import { Spool } from "./process.mjs";
import { executionSucceeded, messageReply } from "./assertions.mjs";

// Keep the shipping JSONL connection open across turns. Each next prompt follows
// the previous turn's committed reply.
export async function conversation(processes, binary, fixture, input, capture, beforeTurn = async () => {}) {
  const active = await processes.startReported(binary, [...fixture.args, "--operation", input.operation,
    "--input-format", "jsonl"], { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
  // One reply per turn, keyed by the turn's id: a repeated id fails the spool.
  const replies = new Spool(active.child.stdout, active.closed, { key: event => [event.id], unique: true });
  let result, failures, ended;
  try {
    for (const turn of input.turns) {
      await beforeTurn(turn, active);
      if (turn.after_request) await Promise.race([
        fixture.waitRequest(turn.after_request),
        ended ??= active.closed.then(result => {
          throw new Error(`conversation exited before its next turn: ${JSON.stringify(result)}`);
        }),
      ]);
      assert.ok(!replies.has(turn.id), "committed reply preceded its prompt");
      await new Promise((resolve, reject) => active.input.write(
        JSON.stringify({ id: turn.id, prompt: turn.text }) + "\n",
        error => error ? reject(error) : resolve()));
      const reply = messageReply(await replies.keyed(turn.id), turn.id);
      assert.equal(reply.error, undefined, `failed JSONL turn ${turn.id}: ${reply.error}`);
      if (turn.answer !== undefined) assert.equal(reply.text, turn.answer);
    }
  } catch (error) {
    failures = [error];
    // Retire only this executable: the owner may also hold other cases and
    // their provider. EOF alone need not stop work owned by this executable.
    active.input.end();
    try { active.killExecutable(); }
    catch (error) { failures.push(error); }
  } finally {
    if (!failures) active.input.end();
    try { result = await active.closed; capture(result); }
    catch (error) { (failures ??= []).push(error); }
  }
  if (failures) {
    if (failures.length === 1) throw failures[0];
    throw new AggregateError(failures, "conversation completion failures", { cause: failures[0] });
  }
  // The final keyed reply can precede malformed trailing output. Join the
  // original spool's drained terminal event before admitting the conversation.
  try { await replies.next(() => false); }
  catch (error) { if (error.kind !== "eof") throw error; }
  executionSucceeded(result);
  assert.equal(replies.sequence, input.turns.length,
    "step=verify one committed reply per prompt");
}
