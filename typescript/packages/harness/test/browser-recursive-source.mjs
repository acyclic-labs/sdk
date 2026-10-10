import { WasmReducer } from "../generated/wasm/acyclic_harness_wasm.js";
import { taskFixture } from "./browser-task-fixture.mjs";

// Public consumers run against real IndexedDB and BrowserStream. A locally
// authenticated reducer is deliberately NOT canonical published source history.
export async function exerciseBrowserRecursiveSource() {
  const fixture = await taskFixture(crypto.randomUUID());
  const agent = fixture.options.volume.owner.id;
  const volume = fixture.options.volume;
  const grants = ["operation:declare", "operation:observe", "operation:cancel", "operation:wake",
    "task:spawn:test.browser@1", "fork:publish",
    fixture.owner.volumeCapability(volume, "read"), fixture.owner.volumeCapability(volume, "write")];
  const runtimeScope = fixture.owner.issueScopeForAgent(agent, "recursive-caller", grants);
  const owner = new WasmReducer({ kind: "conversation", id: crypto.randomUUID() },
    "recursive-source", new Uint8Array(32).fill(71), []);
  let runtime;
  const rejects = async (call, expected) => {
    let caught;
    try { await call(); } catch (error) { caught = error; }
    if (caught === undefined || (expected && !String(caught).includes(expected))) {
      throw new Error(`recursive public rejection differs: ${String(caught)}`);
    }
  };
  try {
    fixture.registry.registerForkPreparation();
    // Open the actual provider with explicit original caller authority.
    const { WasmTaskRuntime } = await import("../generated/wasm/acyclic_harness_wasm.js");
    runtime = await WasmTaskRuntime.open(fixture.options, fixture.owner, fixture.registry, runtimeScope);
    await runtime.initializeVolume();
    const effect = crypto.randomUUID();
    const payload = await runtime.stage(effect, "unbound", new TextEncoder().encode("{}"));
    const caller = crypto.randomUUID();
    const admitted = await runtime.admit(caller, "test.browser", "1", { operation: effect, file: payload, kind: "test.unbound" });
    if (admitted.kind !== "accepted") throw new Error("recursive caller admission failed");
    const work = await runtime.runOperation({ id: "recursive-source-worker", available: {}, labels: {} }, caller, 2);
    if (work?.kind !== "unresolved") throw new Error("expected exact retained caller lease");
    const fence = { reservation_id: work.lease.reservation.id, placement: work.lease.reservation.placement };
    const scope = owner.issueScopeForAgent(agent, "recursive-source", grants);
    owner.apply({ operation_id: crypto.randomUUID(), idempotency_key: "local-only-bind", expected_revision: 0n,
      scope: owner.issueScopeForAgent(agent, "local-only", ["conversation:bind"]), causal_parent: null,
      action: { kind: "bind_conversation", agent } });
    const streamProvider = { namespace: "browser-task", family: "stream", version: "1" };
    const generation = { kind: "generation", provider: volume.provider, key: [1], revision: "1" };
    const source = { task: caller, project: volume, project_generation: generation,
      private_volume: volume, private_generation: generation };
    const child = crypto.randomUUID();
    const config = { child: { kind: "conversation", id: crypto.randomUUID() }, child_project: volume,
      child_private: volume, limits: fixture.options.limits, inherited_messages: 0n, inherited_bytes: 65536n,
      inherited_references: 8, history_limits: { maximum_events: 8, maximum_bytes: 65536n } };
    const prepare = (selectedScope, selectedFence = fence) => runtime.prepareRecursiveForkSource(caller,
      selectedFence, owner, selectedScope, source, streamProvider, crypto.randomUUID(), { id: child, msg: "child" }, config, []);
    await rejects(() => prepare(scope), "fork caller conversation has no agent");
    await rejects(() => prepare(owner.issueScopeForAgent(agent, "upgraded", [...grants, "foreign:read"])),
      "original caller scope");
    await rejects(() => prepare(scope, { ...fence, reservation_id: crypto.randomUUID() }));
    // Exact original local snapshot must not silently substitute current-tail
    // state or bypass the canonical head event check, even with zero history.
    await rejects(() => runtime.restoreRecursiveForkSource(caller, fence, owner, scope, source,
      streamProvider, {}, config.history_limits));
    if (await runtime.outcome(caller) !== null) throw new Error("rejected source operation settled caller");
  } finally {
    runtime?.free(); owner.free(); fixture.free();
  }
}
