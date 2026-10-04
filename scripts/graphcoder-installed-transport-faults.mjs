#!/usr/bin/env node

// Exercise malformed framing, correlation failure, and cancellation against
// the installed package export. The child fixture is intentionally faulty;
// this driver proves the production process adapter fails closed and cleans up.

import { createRequire } from "node:module";
import { existsSync } from "node:fs";
import { access, mkdtemp, rm, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { tmpdir } from "node:os";
import { resolve, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const TIMEOUT_MS = 3_000;

function fail(message) { throw new Error(`graphcoder-installed-transport-faults: ${message}`); }

function required(name) {
  const value = process.env[name];
  if (typeof value !== "string" || value.trim() === "") fail(`${name} is required`);
  return resolve(value);
}

function deadline(promise, label) {
  let timer;
  return Promise.race([
    promise,
    new Promise((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`${label} timed out after ${TIMEOUT_MS}ms`)), TIMEOUT_MS); }),
  ]).finally(() => clearTimeout(timer));
}

function waitForDiagnostic(diagnostics, predicate, label) {
  return deadline(new Promise(resolvePromise => {
    const check = () => {
      const found = diagnostics.find(predicate);
      if (found !== undefined) resolvePromise(found);
      else setTimeout(check, 10);
    };
    check();
  }), label);
}

async function runCase({ Bridge, packageRoot, fixture, mode, requestId, expectedDiagnostic }) {
  const diagnostics = [];
  const bridge = new Bridge({
    executable: process.execPath,
    args: [fixture, mode],
    cwd: resolve("."),
    env: { PATH: process.env.PATH ?? "" },
    onDiagnostic: event => diagnostics.push(event),
  });
  try {
    const pending = bridge.request({ request_id: requestId, method: "list_sessions", params: {} });
    if (mode === "cancel") {
      if (bridge.cancel(requestId, "qualification cancellation") !== true) fail(`${mode} request was not cancellable`);
    }
    await assertRejected(pending, mode);
    const diagnostic = await waitForDiagnostic(diagnostics, event => event.kind === expectedDiagnostic, `${mode} diagnostic`);
    return { mode, diagnostic: diagnostic.kind };
  } finally {
    bridge.close(`qualification ${mode} cleanup`);
    await waitForDiagnostic(diagnostics, event => event.kind === "exit", `${mode} process exit`);
  }
}

async function waitForGuardianExit(path, label) {
  return deadline(new Promise(resolvePromise => {
    const check = () => {
      access(path).then(() => resolvePromise(), () => setTimeout(check, 10));
    };
    check();
  }), label);
}

async function removeTree(path) {
  let lastError;
  for (let attempt = 0; attempt < 50; attempt += 1) {
    try {
      await rm(path, { recursive: true, force: true });
      return;
    } catch (error) {
      lastError = error;
      if (!(error instanceof Error) || !["ENOTEMPTY", "EPERM", "EBUSY"].includes(error.code)) throw error;
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  }
  throw lastError;
}

async function runDescendantCase({ Bridge, packageRoot, fixture }) {
  const diagnostics = [];
  const guardianRoot = await mkdtemp(join(tmpdir(), "graphcoder-transport-guardian-"));
  const guardian = join(guardianRoot, "owned.token");
  const guardianDone = join(guardianRoot, "done.marker");
  await writeFile(guardian, "graphcoder-transport-fault-fixture-v1\n", "utf8");
  const bridge = new Bridge({
    executable: process.execPath,
    args: [fixture, "descendant"],
    cwd: resolve("."),
    env: { PATH: process.env.PATH ?? "", GRAPHCODER_GUARDIAN: guardian, GRAPHCODER_GUARDIAN_DONE: guardianDone },
    onDiagnostic: event => diagnostics.push(event),
  });
  let descendantPid;
  let guardianRecovery = false;
  try {
    try {
      const response = await deadline(bridge.request({ request_id: "fault-descendant", method: "list_sessions", params: {} }), "descendant request");
      if (response.ok !== true || !Number.isInteger(response.result?.descendant_pid)) fail("descendant fixture did not return its child pid");
      descendantPid = response.result.descendant_pid;
    } finally {
      bridge.close("qualification descendant cleanup");
      const termination = await waitForDiagnostic(diagnostics, event => event.kind === "termination", "descendant process cleanup outcome");
      if (termination.outcome?.kind === "unknown") {
        // The Node bridge cannot prove a Windows descendant's ownership after
        // the root has exited. Resolve this fixture through its unique guardian
        // token; no PID-only recovery or cleanup claim is made here.
        guardianRecovery = true;
        await rm(guardian, { force: true });
      }
      await waitForDiagnostic(diagnostics, event => event.kind === "exit", "descendant process exit");
    }
    if (descendantPid === undefined) fail("descendant fixture omitted its child pid");
    if (guardianRecovery) await waitForGuardianExit(guardianDone, "descendant process cleanup");
    return { mode: "descendant", descendant_pid: descendantPid, descendant_exited: true, recovery: guardianRecovery ? "guardian-token" : "native-owner" };
  } finally {
    await removeTree(guardianRoot);
  }
}

async function assertRejected(promise, mode) {
  try {
    await deadline(promise, `${mode} request`);
    fail(`${mode} request unexpectedly succeeded`);
  } catch (error) {
    if (error instanceof Error && error.message.includes("unexpectedly succeeded")) throw error;
    if (!(error instanceof Error) || !/bridge|cancel|malformed|unmatched/iu.test(error.message)) fail(`${mode} returned an unexpected error: ${String(error)}`);
  }
}

export async function runInstalledTransportFaults({ packageRoot = required("GRAPHCODER_PACKAGE_ROOT") } = {}) {
  const packageJson = join(packageRoot, "package.json");
  if (!existsSync(packageJson)) fail(`package.json is missing: ${packageJson}`);
  const resolveExport = createRequire(packageJson);
  let nodePath;
  try { nodePath = resolveExport.resolve("@acyclic-labs/graphcoder/node"); }
  catch (error) { fail(`installed ./node export could not be resolved: ${error instanceof Error ? error.message : String(error)}`); }
  const { JsonLineGraphCoderBridge } = await import(`${pathToFileURL(nodePath).href}?faults=${Date.now()}`);
  const fixture = fileURLToPath(new URL("./graphcoder-transport-fault-fixture.mjs", import.meta.url));
  const cases = [
    await runCase({ Bridge: JsonLineGraphCoderBridge, packageRoot, fixture, mode: "malformed", requestId: "fault-malformed", expectedDiagnostic: "malformed_line" }),
    await runCase({ Bridge: JsonLineGraphCoderBridge, packageRoot, fixture, mode: "unmatched", requestId: "fault-unmatched", expectedDiagnostic: "unmatched_response" }),
    await runCase({ Bridge: JsonLineGraphCoderBridge, packageRoot, fixture, mode: "cancel", requestId: "fault-cancel", expectedDiagnostic: "cancelled_response" }),
  ];
  if (process.env.GRAPHCODER_REQUIRE_DESCENDANT_CLEANUP === "1") {
    cases.push(await runDescendantCase({ Bridge: JsonLineGraphCoderBridge, packageRoot, fixture }));
  }
  return { cases };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  runInstalledTransportFaults().then(result => {
    process.stdout.write(`${JSON.stringify({ ok: true, markers: ["malformed-framing", "correlation-rejection", "cancelled-response", "bounded-cleanup", ...(process.env.GRAPHCODER_REQUIRE_DESCENDANT_CLEANUP === "1" ? ["descendant-cleanup"] : []), "installed-package-export"], result }, null, 2)}\n`);
  }).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
