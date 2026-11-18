import { spawnSync } from "node:child_process";
import { bazelExecutables } from "./bazel-executables.mjs";

export function parseContractJson(text, context) {
  try { return JSON.parse(text); }
  catch (cause) {
    throw new Error(`STEP_FAILED ${context}; recovery: bun scripts/generate-contracts.mjs check`, { cause });
  }
}

export function captureContractCommand(root, executable, args, phase) {
  const result = spawnSync(executable, args, {
    cwd: root, encoding: "utf8", maxBuffer: Infinity, stdio: ["ignore", "pipe", "inherit"],
  });
  if (result.error || result.status !== 0) {
    // Exporter refusals reach stdout. Keep the original
    // completion metadata as well as its text, including signal-only exits.
    throw Object.assign(new Error(
      `STEP_FAILED contract-${phase}: ${executable} failed with status ${result.status ?? "unknown"}, signal ${result.signal ?? "none"}${result.error ? `; ${result.error.message}` : ""}; recovery: bun scripts/generate-contracts.mjs check\n${result.stdout ?? ""}`,
      { cause: result.error },
    ), { executable, args, status: result.status, signal: result.signal, stdout: result.stdout });
  }
  return result.stdout;
}

// Bazel is the authority for source, toolchain, lockfile and feature freshness: every contract target is a
// label of the generated BUILD files, built with every feature on, and run as cquery names its executable.
/** @param {{ phase?: string, build_ms?: number }} report */
export function buildContractTargets(root, labels, report = {}, saveProgress = () => {}) {
  if (labels.length === 0) throw new Error("Rust contract target set is empty");
  return bazelExecutables(root, labels, (phase, executable, args) => {
    report.phase = phase;
    saveProgress();
    const started = performance.now();
    try {
      return captureContractCommand(root, executable, args, phase);
    } finally {
      report[`${phase}_ms`] = performance.now() - started;
      saveProgress();
    }
  });
}

export function runContractTarget(root, executable) {
  return captureContractCommand(root, executable, [], "run");
}

// Slots identify executables admitted by the same Bazel build. The selected
// consumers own output only until its last use; bundle consumers share one decode.
export function contractTargetOutputs(root, executables, consumers) {
  const remaining = new Map();
  for (const [slot, payload] of consumers) {
    if (!remaining.has(slot)) remaining.set(slot, new Map());
    const counts = remaining.get(slot);
    counts.set(payload, (counts.get(payload) ?? 0) + 1);
  }
  const executed = new Map();
  return (slot, payload) => {
    const counts = remaining.get(slot);
    if (!counts?.has(payload)) {
      throw new Error(`STEP_FAILED contract-consumption: undeclared or exhausted target ${slot}/${payload ?? "raw"}; recovery: bun scripts/generate-contracts.mjs check`);
    }
    if (!executed.has(slot)) executed.set(slot, { stdout: runContractTarget(root, executables[slot]) });
    const output = executed.get(slot);
    let value;
    if (payload === undefined) value = output.stdout;
    else {
      if (!Object.hasOwn(output, "payloads")) {
        output.payloads = parseContractJson(output.stdout, `contract-payload: malformed bundle from ${executables[slot]}`);
        if (!counts.has(undefined)) delete output.stdout;
        // No selected renderer owns an unrequested bundle member.
        if (output.payloads && typeof output.payloads === "object") {
          for (const key in output.payloads) {
            if (Object.hasOwn(output.payloads, key) && !counts.has(key)) delete output.payloads[key];
          }
        }
      }
      if (!output.payloads || !Object.hasOwn(output.payloads, payload) || typeof output.payloads[payload] !== "string") {
        throw new Error(`STEP_FAILED contract-payload: missing ${payload}; recovery: bun scripts/generate-contracts.mjs check`);
      }
      value = output.payloads[payload];
    }
    const count = counts.get(payload);
    if (count > 1) counts.set(payload, count - 1);
    else {
      counts.delete(payload);
      if (payload === undefined) {
        if (Object.hasOwn(output, "payloads")) delete output.stdout;
      } else delete output.payloads[payload];
    }
    if (counts.size === 0) {
      executed.delete(slot);
      remaining.delete(slot);
    }
    return value;
  };
}
