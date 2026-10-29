import { beforeAll, expect, test } from "bun:test";
import { WasmAgentAuthority } from "../generated/wasm/acyclic_harness_wasm.js";
import { ensureBrowserWasm } from "../src/wasm-runtime.js";
import { fixtureAuthority, initializeFixtureAuthority } from "../../../../rust/crates/conformance/cloudflare/fixture-authority.mjs";

beforeAll(async () => {
  await ensureBrowserWasm();
  await initializeFixtureAuthority();
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
