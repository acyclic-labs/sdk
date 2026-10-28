import { beforeAll, expect, test } from "bun:test";
import * as wrappers from "../generated/wasm/acyclic_harness_wasm.js";
import initWasm from "../generated/wasm/acyclic_harness_wasm.js";
import {
  assertBrowserWasmExports, assertClientWasmExports,
  ensureBrowserWasm, ensureClientWasm,
} from "../src/wasm-runtime.js";

let raw: Record<string, unknown>;
beforeAll(async () => {
  await ensureBrowserWasm();
  raw = await initWasm() as unknown as Record<string, unknown>;
});

const clientConstructors = [
  ["WasmStream", "wasmstream_new"],
  ["WasmWireSession", "wasmwiresession_new"],
  ["WasmAgentClient", "wasmagentclient_new"],
  ["WasmAgentCommand", "wasmagentcommand_new"],
  ["WasmAgentControls", "wasmagentcontrols_new"],
  ["WasmExplorer", "wasmexplorer_new"],
] as const;
const browserConstructors = [
  ["WasmAgentAuthority", "wasmagentauthority_new"],
  ["WasmAgentRegistry", "wasmagentregistry_new"],
] as const;
type Constructor = readonly [string, string];

function constructorRemoval(check: (value: unknown) => void, entries: readonly Constructor[]): void {
  // Exhaust every nonempty subset; a missing constructor must refuse before IO.
  for (let mask = 1; mask < 2 ** entries.length; mask++) {
    const rawCandidate: Record<string, unknown> = { ...raw };
    const wrapperCandidate: Record<string, unknown> = { ...wrappers };
    for (const [index, [name, symbol]] of entries.entries()) {
      if ((mask & (2 ** index)) === 0) continue;
      delete rawCandidate[symbol];
      delete wrapperCandidate[name];
    }
    expect(() => check(rawCandidate)).toThrow();
    expect(() => check(wrapperCandidate)).toThrow();
  }
}

for (const [name, check, entries] of [
  ["client", assertClientWasmExports, clientConstructors],
  ["browser host", assertBrowserWasmExports, browserConstructors],
] as const) {
  test(`${name} admission accepts the actual generated wrappers and raw ABI`, () => {
    expect(() => check(wrappers)).not.toThrow();
    expect(() => check(raw)).not.toThrow();
  });

  test(`${name} admission requires every constructed WASM class before dispatch`, () => {
    constructorRemoval(check, entries);
  });

  test(`${name} constructor-removal oracle rejects a method-only admission`, () => {
    const methodOnly = (value: unknown) => {
      const candidate = { ...(value as Record<string, unknown>) };
      for (const [, symbol] of entries) candidate[symbol] ??= raw[symbol];
      check(candidate);
    };
    expect(() => constructorRemoval(methodOnly, entries)).toThrow();
  });
}

test("browser and frontend initialization share the retained WASM ABI", async () => {
  await ensureBrowserWasm();
  await ensureClientWasm();
});

test("client admission requires both Rust JSON directions before dispatch", () => {
  for (const symbol of ["decodeClientJson", "encodeCanonicalJson"] as const) {
    for (const actual of [wrappers, raw]) {
      const missing = { ...actual } as Record<string, unknown>;
      delete missing[symbol];
      expect(() => assertClientWasmExports(missing)).toThrow();
    }
  }
});
