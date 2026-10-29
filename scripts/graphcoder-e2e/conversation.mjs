import assert from "node:assert/strict";
import { Spool } from "./process.mjs";

// Keep the shipping JSONL connection open across turns. Each next prompt follows
// the previous turn's committed reply.
export async function conversation(processes, binary, fixture, input, capture, beforeTurn = async () => {}) {
  const active = processes.start(binary, [...fixture.args, "--operation", input.operation,
    "--input-format", "jsonl"], { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
  // One reply per turn, keyed by the turn's id: a repeated id fails the spool.
  const replies = new Spool(active.child.stdout, active.closed, { key: event => [event.id], unique: true });
  const ended = active.closed.then(result => {
    throw new Error(`conversation exited before its next turn: ${JSON.stringify(result)}`);
  });
  ended.catch(() => {});
  try {
    for (const turn of input.turns) {
      await beforeTurn(turn, active);
      if (turn.after_request) await Promise.race([
        fixture.waitRequest(turn.after_request),
        ended,
      ]);
      active.child.stdin.write(JSON.stringify({ id: turn.id, prompt: turn.text }) + "\n");
      const reply = await replies.keyed(turn.id);
      assert.equal(reply.error, undefined, `failed JSONL turn ${turn.id}: ${reply.error}`);
      assert.ok(Number.isSafeInteger(reply.inbox) && reply.inbox >= 0, "missing committed inbox identity");
      assert.equal(typeof reply.text, "string", "missing committed answer text");
      if (turn.answer !== undefined) assert.equal(reply.text, turn.answer);
    }
  } catch (error) {
    throw new Error(`step=drive JSONL conversation: ${error.message}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`, { cause: error });
  } finally {
    active.child.stdin.end();
    const result = await active.closed;
    capture(result);
  }
  const result = await active.closed;
  const recovery = `node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`;
  assert.equal(result.code, 0, `step=verify conversation exit: ${result.stderr}; recovery=${recovery}`);
  assert.equal(result.signal, null, `step=verify conversation retirement; recovery=${recovery}`);
  assert.equal(replies.sequence, input.turns.length,
    `step=verify one committed reply per prompt; recovery=${recovery}`);
}
