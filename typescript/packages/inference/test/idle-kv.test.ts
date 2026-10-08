import { expect, test } from "bun:test";
import { create, fromJsonString, toJsonString } from "@bufbuild/protobuf";
import { Inference, InferenceClient, HttpInferenceTransport, Warm, warmCommitment, contextRevision,
  RetainWarmRequestSchema, RenewWarmRequestSchema, WarmViewSchema, WarmState,
} from "../src/index.js";
import { validateRuntimeShape, validateContract } from "../src/contract.js";

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

test("latency retain and renew reject idle responses while recovered inspect accepts them", async () => {
  const client = new InferenceClient({ retainWarm: async () => view(), renewWarm: async () => view(), inspectWarm: async () => view() } as never);
  await expect(client.retainWarm(create(RetainWarmRequestSchema, { identity, context: bytes(4), latencyProfile: bytes(6), expiresAtMs: 120n }))).rejects.toThrow("retention policy differs");
  await expect(client.renewWarm(create(RenewWarmRequestSchema, { identity, commitment: bytes(3), expiresAtMs: 120n }))).rejects.toThrow("renewal policy differs");
  expect((await client.inspectWarm(bytes(3))).idleKv?.policy?.idleTimeoutMs).toBe(20n);
});

test("client rejects a service substituting a latency or different idle policy", async () => {
  let substitute = view();
  const client = new InferenceClient({ retainWarm: async () => substitute } as never);
  const request = create(RetainWarmRequestSchema, { identity, context: bytes(4), idleKv: policy });
  substitute.idleKv!.policy!.profile = bytes(10);
  await expect(client.retainWarm(request)).rejects.toThrow("policy differs");
  substitute = view();
  substitute.idleKv = undefined;
  substitute.latencyProfile = bytes(6);
  await expect(client.retainWarm(request)).rejects.toThrow("retention policy differs");
});


test("removed wire kinds reject valid idle and latency responses", async () => {
  for (const idle of [true, false]) {
    const response = view();
    if (!idle) { response.idleKv = undefined; response.latencyProfile = bytes(6); }
    await validateRuntimeShape(WarmViewSchema, response);
    for (const kind of ["legacy_warm_context", "legacy_warm_commitment", "idle_warm_context", "idle_warm_commitment"]) {
      await expect(validateContract(kind, WarmViewSchema, response, kind.endsWith("context") ? bytes(4) : bytes(3))).rejects.toThrow("unknown inference message kind");
    }
  }
});

test("invalid retention requests fail before transport for either mode", async () => {
  let calls = 0;
  const client = new InferenceClient({
    retainWarm: async () => { calls++; return view(); },
    renewWarm: async () => { calls++; return view(); },
  } as never);
  for (const request of [
    { identity, context: bytes(4), latencyProfile: bytes(6), expiresAtMs: 0n },
    { context: bytes(4), idleKv: policy },
    { identity, context: bytes(4), idleKv: policy, expiresAtMs: 120n },
  ]) await expect(client.retainWarm(create(RetainWarmRequestSchema, request))).rejects.toThrow();
  for (const request of [
    { identity, commitment: bytes(3), expiresAtMs: 0n },
    { commitment: bytes(3), idleTimeoutMs: 20n },
    { identity, commitment: bytes(3), idleTimeoutMs: 20n, expiresAtMs: 120n },
  ]) await expect(client.renewWarm(create(RenewWarmRequestSchema, request))).rejects.toThrow();
  expect(calls).toBe(0);
});

test("latency admission binds profile and expiry and renewal binds expiry", async () => {
  let response = view();
  response.idleKv = undefined;
  response.latencyProfile = bytes(6);
  const client = new InferenceClient({ retainWarm: async () => response, renewWarm: async () => response } as never);
  const retain = create(RetainWarmRequestSchema, { identity, context: bytes(4), latencyProfile: bytes(6), expiresAtMs: 120n });
  const renew = create(RenewWarmRequestSchema, { identity, commitment: bytes(3), expiresAtMs: 120n });
  expect(await client.retainWarm(retain)).toBe(response);
  expect(await client.renewWarm(renew)).toBe(response);
  response.latencyProfile = bytes(9);
  await expect(client.retainWarm(retain)).rejects.toThrow("retention policy differs");
  response.latencyProfile = bytes(6);
  response.expiresAtMs = 121n;
  await expect(client.retainWarm(retain)).rejects.toThrow("retention policy differs");
  await expect(client.renewWarm(renew)).rejects.toThrow("renewal policy differs");
  response.expiresAtMs = 120n;
  response.context = bytes(9);
  await expect(client.retainWarm(retain)).rejects.toThrow("shape differs");
  response.commitment = bytes(9);
  await expect(client.renewWarm(renew)).rejects.toThrow("shape differs");
});


test("warm validation keeps caller authority through concurrent request aliases", async () => {
  const retain = create(RetainWarmRequestSchema, { identity, context: bytes(4), idleKv: policy });
  const renew = create(RenewWarmRequestSchema, { identity, commitment: bytes(3), idleTimeoutMs: 20n });
  const client = new InferenceClient({
    retainWarm: async (sent: typeof retain) => {
      retain.context = bytes(9);
      expect(sent.context).toEqual(bytes(4));
      sent.context = bytes(10);
      const response = view(); response.context = sent.context;
      return response;
    },
    renewWarm: async (sent: typeof renew) => {
      renew.commitment = bytes(9);
      expect(sent.commitment).toEqual(bytes(3));
      sent.commitment = bytes(10);
      const response = view(); response.commitment = sent.commitment;
      return response;
    },
  } as never);
  await expect(client.retainWarm(retain)).rejects.toThrow("shape differs");
  await expect(client.renewWarm(renew)).rejects.toThrow("shape differs");
});


test("invalid authority width rejects before snapshot copies or transport", async () => {
  class CopyTrap extends Uint8Array {
    override slice(): Uint8Array { throw new Error("unexpected authority copy"); }
  }
  const client = new InferenceClient({} as never);
  const retain = create(RetainWarmRequestSchema, { identity, context: new CopyTrap(33), idleKv: policy });
  const renew = create(RenewWarmRequestSchema, { identity, commitment: new CopyTrap(33), idleTimeoutMs: 20n });
  await expect(client.retainWarm(retain)).rejects.toThrow("context revision must be exactly");
  await expect(client.renewWarm(renew)).rejects.toThrow("warm commitment must be exactly");
});
