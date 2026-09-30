import { expect, test } from "bun:test";
import { create, fromJsonString, toJsonString } from "@bufbuild/protobuf";
import { Inference, InferenceClient, HttpInferenceTransport, Warm, warmCommitment, contextRevision,
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

test("idle KV handles encode exact retry identities and renew from the retained baseline", async () => {
  const requests: string[] = [];
  const fetcher: typeof fetch = async (url, init) => {
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer fixture");
    const body = String(init?.body);
    requests.push(body);
    const response = view();
    if (String(url).endsWith("/retain")) {
      const request = fromJsonString(RetainWarmRequestSchema, body);
      expect(request.identity).toEqual(create(RetainWarmRequestSchema, { identity }).identity);
      expect(request.idleKv?.profile).toEqual(policy.profile);
      expect(request.latencyProfile.byteLength).toBe(0);
      expect(request.expiresAtMs).toBe(0n);
    } else if (String(url).endsWith("/renew")) {
      const request = fromJsonString(RenewWarmRequestSchema, body);
      response.idleKv!.policy!.idleTimeoutMs = request.idleTimeoutMs!;
      response.expiresAtMs = 100n + request.idleTimeoutMs!;
    } else if (String(url).endsWith("/release")) response.state = WarmState.RELEASED;
    return new Response(toJsonString(WarmViewSchema, response));
  };
  const inference = new Inference(new InferenceClient(new HttpInferenceTransport("https://fixture.test", () => ({ authorization: "Bearer fixture" }), fetcher)));
  const context = inference.context(contextRevision(bytes(4)));
  const warm = await context.retain({ idleKv: policy, identity });
  await context.retain({ idleKv: policy, identity });
  expect(requests[0]).toBe(requests[1]);
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
