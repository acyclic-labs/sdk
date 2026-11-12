import assert from "node:assert/strict";
import { Spool } from "./process.mjs";
import { executionSucceeded, messageReply } from "./assertions.mjs";

export async function waitFor(promise, signal) {
  let abort;
  const cancelled = new Promise((_, reject) => {
    abort = () => reject(signal.reason);
    signal.addEventListener("abort", abort, { once: true });
    if (signal.aborted) abort();
  });
  // Even an already cancelled caller owns the original producer's rejection.
  // Attach both handlers before choosing cancellation, rather than throwing
  // while an eagerly invoked callback's rejected promise is unobserved.
  try { return await Promise.race([cancelled, promise]); }
  finally { signal.removeEventListener("abort", abort); }
}

// Keep the shipping JSONL connection open across turns. Each next prompt follows
// the previous turn's committed reply.
export async function conversation(processes, binary, fixture, input, capture, beforeTurn = async () => {}, afterTurn = async () => {}) {
  const active = await processes.startReported(binary, [...fixture.args, "--operation", input.operation,
    "--input-format", "jsonl", ...(input.extra_args ?? [])], { cwd: fixture.workspace, env: fixture.env, scopeReport: true });
  // One reply per turn, keyed by the turn's id: a repeated id fails the spool.
  const replies = new Spool(active.child.stdout, active.closed, { key: event => [event.id], unique: true });
  const lifetime = new AbortController();
  active.closed.then(result => lifetime.abort(new Error(`conversation exited: ${JSON.stringify(result)}`)),
    error => lifetime.abort(error));
  const signal = AbortSignal.any([lifetime.signal, ...(active.failure ? [active.failure] : []),
    ...(fixture.signal ? [fixture.signal] : [])]);
  let result, failures, ended;
  try {
    for (const turn of input.turns) {
      await waitFor(beforeTurn(turn, active, signal), signal);
      if (turn.after_request) await waitFor(Promise.race([
        fixture.waitRequest(turn.after_request),
        ended ??= active.exited.then(result => {
          throw new Error(`conversation exited before its next turn: ${JSON.stringify(result)}`);
        }),
      ]), signal);
      assert.ok(!replies.has(turn.id), "committed reply preceded its prompt");
      await new Promise((resolve, reject) => active.input.write(
        JSON.stringify({ id: turn.id, prompt: turn.text }) + "\n",
        error => error ? reject(error) : resolve()));
      const reply = messageReply(await waitFor(replies.keyed(turn.id), signal), turn.id);
      assert.equal(reply.error, undefined, `failed JSONL turn ${turn.id}: ${reply.error}`);
      if (turn.answer !== undefined) assert.equal(reply.text, turn.answer);
      await waitFor(afterTurn(turn, reply, signal), signal);
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
