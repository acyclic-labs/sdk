import init, { WasmReducer, WasmTaskRegistry, WasmTaskRuntime } from "../generated/wasm/acyclic_harness_wasm.js";
export async function taskFixture(id, taskName = "test.browser", configure, capabilities = [
  "timer:wait", "mail:send", "mail:read", "tool:call:test.effect", "model:generate",
]) {
  await init();
  const owner = new WasmReducer({ kind: "task", id }, "browser-task", new Uint8Array(32).fill(23), []);
  const agent = "15151515-1515-1515-1515-151515151515";
  const provider = { namespace: "browser-task", family: "filesystem", version: "2" };
  const volume = { provider, id: "private", class: "agent_private", owner: { kind: "agent", id: agent } };
  const scope = owner.issueScope("owner", [
    "operation:declare", "operation:observe", "operation:cancel", "operation:wake",
    ...capabilities,
    `task:spawn:${taskName}@1`, owner.volumeCapability(volume, "read"), owner.volumeCapability(volume, "write"),
  ], agent);
  const registry = new WasmTaskRegistry();
  const registration = { name: "test.browser", version: "1", digest: new Array(32).fill(41),
    state_schema: { type: "object" }, input_schema: { type: "object" },
    output_schema: { type: "integer" }, requirements: [] };
  const register = () => registry.registerMachine(registration,
    input => input, (state, input) => {
      return input === null && state.complete !== true
        ? { state, commands: [{ operation_id: state.operation, kind: state.kind ?? "acyclic.timer.v1", payload: state.file }], status: { kind: "suspended" } }
        : state.followup && !state.followup_started
          ? { state: { ...state, followup_started: true }, commands: [{ operation_id: state.followup.operation, kind: "acyclic.timer.v1", payload: state.followup.file }], status: { kind: "suspended" } }
        : { state, commands: [], status: { kind: "completed", value: 7 } };
    });
  if (configure) configure(registry, register, registration); else register();
  const options = {
    filesystem_database: `harness-task-fs-${id}`, stream_database: `harness-task-stream-${id}`,
    maximum_object_bytes: 1048576n, maximum_payload_bytes: 65536n, filesystem_provider: provider, volume,
    stream_limits: { commands: 10000n, journal_bytes: 16777216n, paths: 1000, path_bytes: 262144,
      records: 10000, payload_bytes: 16777216, commits: 10000, idempotency_results: 10000 },
    limits: { file_bytes: 1048576n, path_bytes: 4096n, attachments: 32n, render_bytes: 1048576n,
      model_steps: 16n, model_events_per_step: 64n, tool_calls_per_step: 16n, context_messages: 128n },
    run_limits: { concurrency: 1n, max_steps: 16n, deadline_epoch_ms: null },
    session_limits: { active_tasks: 1n, total_tasks: 4n, depth: 1, model_steps: 16n }, concurrency: 1,
  };
  return { owner, registry, options, scope,
    open: () => WasmTaskRuntime.open(options, owner, registry, scope),
    free: () => { registry.free(); owner.free(); } };
}
export const modelCapacity = () => ({ context_tokens: 131072, output_tokens: 8192 });
export const modelTokenCount = (bytes, request_digest) => ({
  request_digest, fixed_tokens: bytes.byteLength,
  message_tokens: JSON.parse(new TextDecoder().decode(bytes)).messages.map(() => 0),
});

export async function qualifyBrowserTask(run) {
  const result = document.querySelector("#result");
  try {
    await run(text => { result.dataset.waiting = text; result.textContent = text; });
    result.dataset.status = "passed";
    result.textContent = "passed";
  } catch (error) {
    result.dataset.status = "failed";
    result.textContent = String(error.stack ?? error);
  }
}
