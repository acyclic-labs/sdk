import { exerciseForkPolicy } from "./fork-policy-consumer.mjs";
import { BrowserAgentAuthority, initializeBrowserRuntime } from "../dist/browser.js";
import { encodeCanonicalJson, digestCanonicalJson } from "../generated/wasm/acyclic_harness_wasm.js";

// Exact wire codes from harness-core/proto/harness/v2/harness.proto.
const UNSUPPORTED = 3, UNAUTHORIZED = 5;
const equal = (left, right) => {
  const a = encodeCanonicalJson(left), b = encodeCanonicalJson(right);
  return a.length === b.length && a.every((byte, index) => byte === b[index]);
};

/** Shared installed-WASM consumer exercised by Bun and actual Chromium. */
export async function exerciseCheckpoint(options) {
  await initializeBrowserRuntime();
  const harness = new BrowserAgentAuthority(options.authority, options.issuerId, options.issuerKey, options.schemas ?? []);
  const restore = snapshot => BrowserAgentAuthority.restore(snapshot, options.issuerId, options.issuerKey, options.schemas ?? []);
  try {
    const [authority, revision] = harness.head();
    if (!equal(authority, options.authority) || revision !== 0n) {
      throw new Error("fresh command head is invalid");
    }
    const command = {
      operation_id: "72727272-7272-7272-7272-727272727272",
      idempotency_key: "authenticated-checkpoint-transition",
      expected_revision: 0n,
      scope: harness.issueScope("checkpoint", ["lifecycle:manage"], undefined),
      causal_parent: null,
      action: { kind: "transition_lifecycle", to: "active", reason: null },
    };
    if (harness.apply(command).result !== "applied") throw new Error("checkpoint setup replayed");
    const snapshot = structuredClone(harness.snapshot());
    if (!equal(harness.head(), [snapshot.authority, snapshot.revision])) {
      throw new Error("command head disagrees with the checkpoint");
    }
    if (snapshot.format_version !== 6) throw new Error("checkpoint format was not updated");
    for (const format_version of [2, 3, 4, 5]) {
      let rejected = false;
      try { restore({ ...snapshot, format_version }).free(); }
      catch (error) { rejected = error.code === UNSUPPORTED; }
      if (!rejected) throw new Error("obsolete checkpoint format was accepted");
    }
    const restored = restore(snapshot);
    try {
      if (!equal(restored.snapshot(), snapshot)
          || restored.apply(command).result !== "replayed"
          || restored.snapshot().revision !== 1n) throw new Error("checkpoint state/retry restoration failed");
    } finally { restored.free(); }
    const forged = structuredClone(snapshot);
    forged.projection.lifecycle = "completed";
    forged.state_digest = [...digestCanonicalJson([
      forged.format_version, forged.authority, forged.revision, forged.events, forged.projection,
    ])];
    let rejected = false;
    try { restore(forged).free(); }
    catch (error) { rejected = error.code === UNAUTHORIZED; }
    if (!rejected) throw new Error("forged projection with recomputed public digest accepted");
    await exerciseForkPolicy(options);
  } finally { harness.free(); }
}
