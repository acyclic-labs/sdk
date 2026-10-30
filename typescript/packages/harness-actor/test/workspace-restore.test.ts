import { expect, mock, test } from "bun:test";
import { readFile } from "node:fs/promises";
import { materializeActorState } from "@acyclic-labs/actors";
import * as browser from "@acyclic-labs/harness/browser";
import * as harness from "@acyclic-labs/harness";
import * as hosting from "@acyclic-labs/harness/hosting";
import { HARNESS_BINDINGS, MODEL_FETCH } from "../src/bindings.js";
import owner from "./owner.json";

// Restore order, observed at the handles the entry drives. Only exports no
// other test file imports are replaced, so a runner that shares module mocks
// across files changes nothing they use; everything else is the real module.
const calls: unknown[][] = [];
let refuse: "open" | "bind" | undefined;
const handle = { free() { calls.push(["free"]); } };
class Authority { constructor(readonly capability: unknown) {} }
mock.module("@acyclic-labs/harness/browser", () => ({
  ...browser,
  BrowserAgentAuthority: Authority,
  BrowserAgentRegistry: class {
    bindMachinesWorkspace(workspace: unknown) {
      calls.push(["bind", workspace]);
      if (refuse === "bind") throw new Error("bind refused");
    }
  },
  BrowserMachinesWorkspace: {
    open(authority: unknown, source: unknown) {
      calls.push(["open", authority, source]);
      if (refuse === "open") throw new Error("open refused");
      return handle;
    },
  },
}));
mock.module("@acyclic-labs/harness", () => ({
  ...harness,
  ModelRegistry: class {
    bindCredentials() {}
    register() {}
    resolve() { return { install() { calls.push(["install"]); }, free() {} }; }
    free() {}
  },
}));
mock.module("@acyclic-labs/harness/hosting", () => ({
  ...hosting,
  EventAgentHost: class { async create(name: string) { calls.push(["create", name]); } },
}));
await hosting.initializeHosting({ module_or_path: await readFile(new URL("../../harness/generated/wasm/acyclic_harness_wasm_bg.wasm", import.meta.url)) });
(globalThis as { HARNESS_ACTOR?: unknown }).HARNESS_ACTOR = { config: { options: owner.options, model: owner.model } };
const { actor } = await import("../src/harness-actor.js");

const source = { provider: new Uint8Array(), observation: new Uint8Array(), open: {} };
const capability = {};
const storage = materializeActorState({
  seed: { metadata: [], journalTail: "0" },
  async commit() {}, async readJournal() { return []; }, fence() {},
});
const env = { ...Object.fromEntries(Object.values(HARNESS_BINDINGS).map(name => [name, {}])),
  [HARNESS_BINDINGS.storage]: storage, [HARNESS_BINDINGS.streams]: { register() {}, dispatch() {}, open() {} },
  [HARNESS_BINDINGS.authority]: capability, [HARNESS_BINDINGS.workspace]: source, [MODEL_FETCH]: {} };
const restore = () => actor.restore(null, { env, actor: { id: "root" } } as never);

test("restore opens the admitted Machines source under the actor's authority, binds it and frees the handle", async () => {
  calls.length = 0; refuse = undefined;
  await restore();
  expect(calls.map(call => call[0])).toEqual(["install", "open", "bind", "free", "create"]);
  const [, authority, opened] = calls[1]!;
  expect(authority).toBeInstanceOf(Authority);
  expect((authority as Authority).capability).toBe(capability);
  expect(opened).toBe(source);
  expect(calls[2]![1]).toBe(handle);
});

test("a refused Machines workspace creates no agent, and a refused bind still frees the handle", async () => {
  calls.length = 0; refuse = "open";
  await expect(restore()).rejects.toThrow("open refused");
  expect(calls.map(call => call[0])).toEqual(["install", "open"]);
  calls.length = 0; refuse = "bind";
  await expect(restore()).rejects.toThrow("bind refused");
  expect(calls.map(call => call[0])).toEqual(["install", "open", "bind", "free"]);
});
