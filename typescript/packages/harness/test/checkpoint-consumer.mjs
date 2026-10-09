import { exerciseForkPolicy } from "./fork-policy-consumer.mjs";
import { ErrorCode } from "../generated/proto/harness/v2/harness_pb.js";

/** Shared installed-WASM consumer exercised by Bun and actual Chromium. */
export async function exerciseCheckpoint(Harness, harness, options, contracts) {
  const [authority, revision] = harness.head();
  if (!contracts.canonicalEqual(authority, options.authority) || revision !== 0n) {
    throw new Error("fresh command head is invalid");
  }
  const command = {
    authority: options.authority,
    operation_id: "72727272-7272-7272-7272-727272727272",
    idempotency_key: "authenticated-checkpoint-transition",
    expected_revision: 0n,
    scope: harness.issueScope("checkpoint", ["lifecycle:manage"]),
    causal_parent: null,
    action: { kind: "transition_lifecycle", to: "active", reason: null },
  };
  if (harness.apply(command).result !== "applied") throw new Error("checkpoint setup replayed");
  const snapshot = structuredClone(harness.snapshot());
  if (!contracts.canonicalEqual(harness.head(), [snapshot.authority, snapshot.revision])) {
    throw new Error("command head disagrees with the checkpoint");
  }
  if (snapshot.format_version !== 5) throw new Error("checkpoint format was not updated");
  for (const format_version of [2, 3, 4]) {
    let rejected = false;
    try { (await Harness.restore({ ...snapshot, format_version }, options)).free(); }
    catch (error) { rejected = error.code === ErrorCode.UNSUPPORTED; }
    if (!rejected) throw new Error("obsolete checkpoint format was accepted");
  }
  const restored = await Harness.restore(snapshot, options);
  try {
    if (!contracts.canonicalEqual(restored.snapshot(), snapshot)
        || restored.apply(command).result !== "replayed"
        || restored.snapshot().revision !== 1n) throw new Error("checkpoint state/retry restoration failed");
  } finally { restored.free(); }
  const forged = structuredClone(snapshot);
  forged.projection.lifecycle = "completed";
  forged.state_digest = [...contracts.digestCanonicalJson([
    forged.format_version, forged.authority, forged.revision, forged.events, forged.projection,
  ])];
  let rejected = false;
  try { (await Harness.restore(forged, options)).free(); }
  catch (error) { rejected = error.code === ErrorCode.UNAUTHORIZED && error.message.includes("snapshot admission attestation"); }
  if (!rejected) throw new Error("forged projection with recomputed public digest accepted");
  await exerciseForkPolicy(Harness, options, contracts);
}
