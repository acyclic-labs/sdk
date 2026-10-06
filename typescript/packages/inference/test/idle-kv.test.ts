import { expect, test } from "bun:test";
import { create } from "@bufbuild/protobuf";
import { Inference, InferenceClient, Warm, warmCommitment, contextRevision,
  RetainWarmRequestSchema, RenewWarmRequestSchema, WarmViewSchema, WarmState,
} from "../src/index.js";
import { validateRuntimeShape } from "../src/contract.js";

const bytes = (value: number, length = 32) => new Uint8Array(length).fill(value);
const identity = { clientInstance: bytes(1, 16), requestId: bytes(2, 16) };
const policy = { profile: bytes(6), idleTimeoutMs: 20n };
const view = () => create(WarmViewSchema, {
  commitment: bytes(3), context: bytes(4), modelProfile: bytes(5), expiresAtMs: 120n,
  state: WarmState.ACTIVE, evidenceDigest: bytes(7), admissionReceiptId: bytes(8), sequence: 1n,
  idleKv: { policy, retainedAtMs: 100n },
});

test("idle KV handles exact retry identities and renew from the retained baseline", async () => {
  const requests = [];
  const transport = {
    async retainWarm(request) { requests.push(request); return view(); },
    async inspectWarm() { return view(); },
    async renewWarm(request) {
      const response = view();
      response.idleKv!.policy!.idleTimeoutMs = request.idleTimeoutMs!;
      response.expiresAtMs = 100n + request.idleTimeoutMs!;
      return response;
    },
    async releaseWarm() { const response = view(); response.state = WarmState.RELEASED; return response; },
  } as never;
  const inference = new Inference(new InferenceClient(transport));
  const context = inference.context(contextRevision(bytes(4)));
  const warm = await context.retain({ idleKv: policy, identity });
  await context.retain({ idleKv: policy, identity });
  expect(requests[0]).toEqual(requests[1]);
  const recovered = new Warm(inference, warmCommitment(warm.id()));
  expect((await recovered.inspect()).idleKv?.lastUsedAtMs).toBeUndefined();
  const renewed = await recovered.renewIdle(30n, { identity });
  expect(renewed.expiresAtMs).toBe(130n);
  expect(renewed.idleKv?.retainedAtMs).toBe(100n);
  expect(renewed.idleKv?.lastUsedAtMs).toBeUndefined();
  expect((await recovered.release({ identity })).state).toBe(WarmState.RELEASED);
});
test("Rust validation rejects mixed policies, invented actual-use evidence and overflow", async () => {
  await expect(validateRuntimeShape(RetainWarmRequestSchema, create(RetainWarmRequestSchema, {
    identity, context: bytes(4), idleKv: policy, latencyProfile: bytes(6), expiresAtMs: 100n,
  }))).rejects.toThrow("mutually exclusive");
  await expect(validateRuntimeShape(RenewWarmRequestSchema, create(RenewWarmRequestSchema, {
    identity, commitment: bytes(3), idleTimeoutMs: 0n,
  }))).rejects.toThrow();
  const actualUse = view();
  actualUse.idleKv!.lastUsedAtMs = 110n;
  actualUse.expiresAtMs = 130n;
  await expect(validateRuntimeShape(WarmViewSchema, actualUse)).rejects.toThrow("evidence differs");
  actualUse.idleKv!.lastRunId = bytes(9, 16);
  await validateRuntimeShape(WarmViewSchema, actualUse);
  actualUse.idleKv!.lastUsedAtMs = 18446744073709551615n;
  actualUse.expiresAtMs = 19n;
  await expect(validateRuntimeShape(WarmViewSchema, actualUse)).rejects.toThrow("evidence differs");
});

test("legacy retain and renew reject idle responses while recovered inspect accepts them", async () => {
  const client = new InferenceClient({ retainWarm: async () => view(), renewWarm: async () => view(), inspectWarm: async () => view() } as never);
  await expect(client.retainWarm(create(RetainWarmRequestSchema, { identity, context: bytes(4), latencyProfile: bytes(6), expiresAtMs: 120n }))).rejects.toThrow("retention mode differs");
  await expect(client.renewWarm(create(RenewWarmRequestSchema, { identity, commitment: bytes(3), expiresAtMs: 120n }))).rejects.toThrow("retention mode differs");
  expect((await client.inspectWarm(bytes(3))).idleKv?.policy?.idleTimeoutMs).toBe(20n);
});

test("client rejects a service substituting a legacy or different idle policy", async () => {
  let substitute = view();
  const client = new InferenceClient({ retainWarm: async () => substitute } as never);
  const request = create(RetainWarmRequestSchema, { identity, context: bytes(4), idleKv: policy });
  substitute.idleKv!.policy!.profile = bytes(10);
  await expect(client.retainWarm(request)).rejects.toThrow("policy differs");
  substitute = view();
  substitute.idleKv = undefined;
  substitute.latencyProfile = bytes(6);
  await expect(client.retainWarm(request)).rejects.toThrow("response is absent");
});
