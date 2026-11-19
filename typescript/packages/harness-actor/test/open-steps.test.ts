import { expect, mock, test } from "bun:test";
import { GUEST_STEPS, GuestStepError } from "@acyclic-labs/actors/guest";
import { HARNESS_BINDINGS, MODEL_FETCH } from "../src/bindings.js";

// Each harness readiness call, replaced so exactly one step throws guest text.
let failing = "";
const at = (name: string) => { if (failing === name) throw new Error(`secret-guest-text ${name}`); };
const freed = { free() {} };
mock.module("@acyclic-labs/harness/hosting", () => ({
  initializeHosting: async () => at("hosting"),
  WasmStream: { connectProvider: () => (at("streams"), freed) },
  WasmObjects: class { free() {} constructor() { at("objects"); } },
  streamLedgerPort: () => ({}),
}));
mock.module("@acyclic-labs/harness/browser", () => ({
  BrowserAgentAuthority: class { free() {} constructor() { at("authority"); } },
  BrowserAgentRegistry: class { free() {} bindMachinesWorkspaceSource() { at("workspace"); } },
  BrowserAgentProviders: { injected: () => (at("providers"), freed) },
  AgentGuest: { open: () => (at("open"), { manifest: () => (at("manifest"), new Uint8Array()) }) },
}));
mock.module("@acyclic-labs/harness", () => ({
  ModelRegistry: class {
    constructor() { at("registry"); }
    free() {}
    bindCredentials() { at("credentials"); }
    register() { at("register"); }
    resolve() { at("resolve"); return { install: () => at("install"), free() {} }; }
  },
}));
const { guestManifest, openGuest } = await import("../src/open.js");
const config = { options: {}, model: { provider: "p", spec: {}, selection: "s", limits: undefined, compaction: {} } };
const env = { model() {} };
const failure = async (step: string) => {
  failing = step;
  const error = await openGuest(env as never, config as never).then(() => undefined, (error: unknown) => error);
  // The wire form the host reads; the message and cause never cross.
  const { type, detail } = error as { type: string; detail: unknown };
  expect(JSON.stringify({ type, detail })).not.toContain("secret");
  return { type, detail };
};
const dependency = (name: string) => ({ type: "dependencyUnavailable", detail: { dependency: name } });
const guestStep = (step: string) => ({ type: "guestStep", detail: { step } });

test("hosting initialization names its step", async () => expect(await failure("hosting")).toEqual(guestStep("harness.hosting")));
test("the platform authority names its binding", async () =>
  expect(await failure("authority")).toEqual(dependency(HARNESS_BINDINGS.authority)));
test("model credentials name the model fetch binding", async () =>
  expect(await failure("credentials")).toEqual(dependency(MODEL_FETCH)));
test("model registry, registration, resolution and install name the model step", async () => {
  for (const call of ["registry", "register", "resolve", "install"]) expect(await failure(call)).toEqual(guestStep("harness.model"));
});
test("the Machines workspace names its binding", async () =>
  expect(await failure("workspace")).toEqual(dependency(HARNESS_BINDINGS.workspace)));
test("shared streams name their binding", async () => expect(await failure("streams")).toEqual(dependency(HARNESS_BINDINGS.streams)));
test("objects name their binding", async () => expect(await failure("objects")).toEqual(dependency(HARNESS_BINDINGS.objects)));
test("provider injection names its step", async () => expect(await failure("providers")).toEqual(guestStep("harness.providers")));
test("agent open names its step", async () => expect(await failure("open")).toEqual(guestStep("harness.open")));

test("a guest step is only a declared one", () => {
  for (const step of GUEST_STEPS) expect(new GuestStepError(step).detail.step).toBe(step);
  expect(() => new GuestStepError("sk_live_secretToken" as never)).toThrow(TypeError);
});
test("the opened guest's manifest names its step", async () => {
  failing = "manifest";
  const guest = await openGuest({} as never, config as never);
  const error = await Promise.resolve().then(() => guestManifest(guest)).then(() => undefined, (error: unknown) => error) as GuestStepError;
  expect(JSON.stringify({ type: error.type, detail: error.detail })).toBe('{"type":"guestStep","detail":{"step":"harness.manifest"}}');
});
