import { beforeAll, expect, test } from "bun:test";
import { WasmAgentAuthority, type WasmAuthorityCapability } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureBrowserWasm } from "../src/wasm-runtime.js";
import { fixtureAuthority, initializeFixtureAuthority } from "../../../../rust/crates/conformance/cloudflare/fixture-authority.mjs";
import { materializeGuestAdmission, type GuestAdmissionHost } from "../../auth/src/guest-admission.js";
import type { EventHostAccess } from "../src/event-host.js";

beforeAll(async () => {
  await ensureBrowserWasm();
  await initializeFixtureAuthority();
});

test("hosted admission bindings retain original revocation and refuse missing admission", async () => {
  const host = fixtureAuthority("bound-guest");
  const bindings: GuestAdmissionHost<WasmAuthorityCapability, EventHostAccess> = host.bindings;
  expect(() => materializeGuestAdmission("platformAuthority", undefined)).toThrow("guest admission missing");
  expect(() => materializeGuestAdmission("hostAccess", undefined)).toThrow("guest admission missing");
  // Request/config objects cannot stand in for the host's original admission port.
  expect(() => materializeGuestAdmission("platformAuthority", { admission: host.capability.admission } as never)).toThrow();
  const capability = materializeGuestAdmission("platformAuthority", bindings);
  const access = materializeGuestAdmission("hostAccess", bindings);
  expect(capability).toBe(host.capability);
  expect(materializeGuestAdmission("hostAccess", bindings)).toBe(access);
  const derive = capability.derive.bind(capability);
  let mints = 0;
  capability.derive = (request, signal) => { ++mints; return derive(request, signal); };
  for (const count of [1, 100, 1000]) {
    for (let index = 0; index < count; ++index) {
      expect(materializeGuestAdmission("platformAuthority", bindings)).toBe(capability);
      expect(materializeGuestAdmission("hostAccess", bindings)).toBe(access);
    }
    expect(mints).toBe(0);
  }
  const owner = new WasmAgentAuthority(capability);
  const observer = new AbortController();
  try {
    await expect(owner.issueScope("before-revoke", ["interaction:route"])).resolves.toBeDefined();
    // Register on the original owner, not a local notification channel.
    const changed = capability.trustChanged(observer.signal);
    host.revoke(capability.admission.scope);
    await changed;
    expect(capability.check(capability.admission.scope)).not.toBe("current");
    expect(access.policy(new Uint8Array(), {} as never, observer.signal)).toBe(false);
    expect(() => materializeGuestAdmission("platformAuthority", bindings)).toThrow();
    expect(() => materializeGuestAdmission("hostAccess", bindings)).toThrow();
    await expect(owner.issueScope("after-revoke", ["interaction:route"])).rejects.toBeDefined();
  } finally { observer.abort(); owner.free(); host.free(); }
});

test("a separate host issues exact sealed descendants and keeps current ownership", async () => {
  const host = fixtureAuthority("opaque-authority");
  let calls = 0;
  let aborts = 0;
  const capability = {
    ...host.capability,
    derive(request: unknown, signal: AbortSignal) {
      calls++;
      signal.addEventListener("abort", () => { aborts++; }, { once: true });
      return host.capability.derive(request, signal);
    },
  };
  const owner = new WasmAgentAuthority(capability);
  try {
    const scope = await owner.issueScope("exact-child", ["interaction:route"]);
    expect(scope.id).toBe("exact-child");
    expect(scope.capabilities).toEqual(["interaction:route"]);
    expect(calls).toBe(1);
    expect(aborts).toBe(0);
    await expect(owner.attenuate(scope, "widened", ["unadmitted"])).rejects.toBeDefined();
    expect(calls).toBe(1);
    host.revoke(scope);
    await expect(owner.attenuate(scope, "revoked-child", ["interaction:route"])).rejects.toBeDefined();
    expect(calls).toBe(1);
    // Targeted child revocation leaves the original admission live.
    await expect(owner.issueScope("unrelated-child", ["interaction:route"])).resolves.toBeDefined();
    expect(calls).toBe(2);
    host.close();
    expect(() => materializeGuestAdmission("platformAuthority", host.bindings)).toThrow("guest admission stopped");
    expect(() => materializeGuestAdmission("hostAccess", host.bindings)).toThrow("guest admission stopped");
    await expect(owner.issueScope("after-close", ["interaction:route"])).rejects.toBeDefined();
    expect(calls).toBe(2);
  } finally {
    owner.free();
    host.free();
  }
});

test("async-only validity and a changed audience fail before operation dispatch", () => {
  const host = fixtureAuthority("admission-refusals");
  try {
    expect(() => new WasmAgentAuthority({ ...host.capability, check: async () => "current" })).toThrow();
    expect(() => new WasmAgentAuthority({ ...host.capability, trustClosed: () => true })).toThrow();
    const admission = { ...host.capability.admission,
      audience: { ...host.capability.admission.audience, id: "another-task" } };
    expect(() => new WasmAgentAuthority({ ...host.capability, admission })).toThrow();
  } finally {
    host.free();
  }
});

test("original owner loss cancels one pending foreign operation without replay", async () => {
  const host = fixtureAuthority("pending-authority");
  let entered!: () => void;
  const dispatched = new Promise<void>(resolve => { entered = resolve; });
  let calls = 0;
  let aborted = false;
  const owner = new WasmAgentAuthority({
    ...host.capability,
    derive(_request: unknown, signal: AbortSignal) {
      calls++;
      return new Promise((_resolve, reject) => {
        signal.addEventListener("abort", () => { aborted = true; reject(signal.reason); }, { once: true });
        entered();
      });
    },
  });
  try {
    const pending = owner.issueScope("pending", ["interaction:route"]);
    await dispatched;
    host.close();
    await expect(pending).rejects.toBeDefined();
    expect(calls).toBe(1);
    expect(aborted).toBe(true);
    await expect(owner.issueScope("no-replay", ["interaction:route"])).rejects.toBeDefined();
    expect(calls).toBe(1);
  } finally {
    owner.free();
    host.free();
  }
});
