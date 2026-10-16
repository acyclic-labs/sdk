import { expect, test } from "bun:test";
import fc from "fast-check";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { credentialProvider, validateCredential } from "../src/credential.js";
import { ActorsClient, type ActorsRustClient } from "../src/client.js";
import { ACTORS_OPERATION_NAMES } from "../src/generated/actors-service.js";

const actor = { actorId: "actor-a" as Parameters<ActorsClient["inspectActor"]>[0]["actorId"] };
const legacyClient = (): ActorsRustClient => Object.fromEntries(
  ACTORS_OPERATION_NAMES.map(name => [name, async () => new Uint8Array()]),
) as ActorsRustClient;

const token = fc.string({ minLength: 1, maxLength: 256 })
  .filter(value => value.trim().length > 0 && !/[\r\n\0]/.test(value));
const settings = { numRuns: 128 };

test("credential admission matches the byte bound and forbidden characters", () => {
  fc.assert(fc.property(fc.oneof(fc.string({ maxLength: 14000 }), token), value => {
    const admitted = value.trim().length > 0 &&
      new TextEncoder().encode(value).byteLength <= 12288 && !/[\r\n\0]/.test(value);
    if (admitted) expect(() => validateCredential(value)).not.toThrow();
    else expect(() => validateCredential(value)).toThrow(TypeError);
  }), settings);
  for (const value of ["é".repeat(6144), "é".repeat(6145), "a".repeat(12288), "a".repeat(12289)]) {
    if (new TextEncoder().encode(value).length <= 12288) validateCredential(value);
    else expect(() => validateCredential(value)).toThrow(TypeError);
  }
});

test("every request resolves the current provider without caching or fallback", async () => {
  await fc.assert(fc.asyncProperty(fc.array(token, { minLength: 1, maxLength: 16 }), async values => {
    let calls = 0;
    const options = { token: "stale", credential: () => values[calls++]! };
    const resolve = credentialProvider(options);
    options.credential = () => "mutated";
    for (const expected of values) expect(await resolve()).toBe(expected);
    expect(calls).toBe(values.length);
    await expect(resolve()).rejects.toThrow(TypeError);
  }), settings);
});

test("fixed credentials are snapshotted and failures never expose a fallback", async () => {
  await fc.assert(fc.asyncProperty(token, async value => {
    const options = { token: value };
    const resolve = credentialProvider(options);
    options.token = "mutated";
    expect(await resolve()).toBe(value);
    const failure = new Error("renewal unavailable");
    const failed = credentialProvider({ token: value, credential: () => { throw failure; } });
    await expect(failed()).rejects.toBe(failure);
    const invalid = credentialProvider({ token: value, credential: () => "\r\n" });
    await expect(invalid()).rejects.toThrow(TypeError);
  }), settings);
});

test("cancellation before and during renewal prevents credential admission", async () => {
  await fc.assert(fc.asyncProperty(token, async value => {
    const controller = new AbortController();
    const reason = new Error("request cancelled");
    let calls = 0;
    const resolve = credentialProvider({ credential: signal => {
      calls++;
      expect(signal).toBe(controller.signal);
      controller.abort(reason);
      return Promise.resolve(value);
    } });
    await expect(resolve(controller.signal)).rejects.toBe(reason);
    expect(calls).toBe(1);
    await expect(resolve(controller.signal)).rejects.toBe(reason);
    expect(calls).toBe(1);
    expect(() => credentialProvider({ token: value })(controller.signal)).toThrow(reason);
  }), settings);
});

test("Actors sends every renewed credential on one reusable connection", async () => {
  await fc.assert(fc.asyncProperty(fc.array(token, { minLength: 2, maxLength: 16 }), async values => {
    let calls = 0;
    const sent: string[] = [];
    const connected: string[] = [];
    const client = new ActorsClient({
      endpoint: "https://actors.example.test",
      token: "stale",
      credential: () => values[calls++]!,
      binding: { connect: async (_endpoint, token) => {
        connected.push(token);
        return { ...legacyClient(), withCredential: async (method, _request, credential) => {
          expect(method).toBe("InspectActor");
          sent.push(credential);
          return new Uint8Array();
        } };
      } },
    });
    for (const _value of values) {
      await client.inspectActor(actor);
      expect(await client.transport).toBeUndefined();
    }
    expect(connected).toEqual([values[0]!]);
    expect(sent).toEqual(values);
    expect(calls).toBe(values.length);
  }), settings);
});

test("Actors never falls back to a stale bridge credential", async () => {
  await fc.assert(fc.asyncProperty(token, async value => {
    let sent = 0;
    const client = new ActorsClient({
      endpoint: "https://actors.example.test",
      credential: () => value,
      binding: { connect: async () => ({ ...legacyClient(), inspectActor: async () => {
        sent++;
        return new Uint8Array();
      } }) },
    });
    await expect(client.inspectActor(actor)).rejects.toThrow("per-request credentials");
    expect(sent).toBe(0);
  }), settings);
});

test("Actors snapshots provider selection once even when configuration has getters", async () => {
  await fc.assert(fc.asyncProperty(fc.array(token, { minLength: 2, maxLength: 8 }), async values => {
    let calls = 0;
    let reads = 0;
    const sent: string[] = [];
    const options = {
      endpoint: "https://actors.example.test", token: "stale", credential: () => values[calls++]!,
      binding: { connect: async () => ({ ...legacyClient(), withCredential: async (_method: string, _request: Uint8Array, value: string) => {
        sent.push(value);
        return new Uint8Array();
      } }) },
    };
    const callback = options.credential;
    Object.defineProperty(options, "credential", { get: () => ++reads === 1 ? callback : undefined });
    const client = new ActorsClient(options);
    for (const _value of values) await client.inspectActor(actor);
    expect(reads).toBe(1);
    expect(sent).toEqual(values);
    expect(calls).toBe(values.length);
  }), settings);
});

test("Actors cancellation settles while a credential provider is still pending", async () => {
  await fc.assert(fc.asyncProperty(token, fc.boolean(), async (value, reject) => {
    let resolve!: (value: string) => void;
    let fail!: (error: Error) => void;
    const pending = new Promise<string>((yes, no) => { resolve = yes; fail = no; });
    const controller = new AbortController();
    let connected = 0;
    const client = new ActorsClient({
      endpoint: "https://actors.example.test", credential: () => pending,
      binding: { connect: async () => { connected++; return legacyClient(); } },
    });
    // Negative control: awaiting the provider alone ignores cancellation.
    let brokenSettled = false;
    const broken = pending.then(() => { brokenSettled = true; }, () => { brokenSettled = true; });
    const call = client.inspectActor(actor, { signal: controller.signal });
    controller.abort();
    try {
      await expect(call).rejects.toMatchObject({ code: "cancelled" });
      expect(brokenSettled).toBe(false);
      expect(connected).toBe(0);
    } finally {
      if (reject) fail(new Error("late credential failure")); else resolve(value);
      await broken;
    }
  }), { numRuns: 32 });
}, 1000);

test("shared renewal explores every cancellation and settlement ordering in Node", () => {
  const fixture = new URL("./credential-interleavings.mjs", import.meta.url);
  const run = (variant: string) => spawnSync("node", ["--unhandled-rejections=strict", fileURLToPath(fixture), variant], {
    encoding: "utf8", timeout: 2000, windowsHide: true,
  });
  const fixed = run("fixed");
  expect(fixed.error).toBeUndefined();
  expect(fixed.stderr).toBe("");
  expect(fixed.status).toBe(0);
  const broken = run("broken");
  expect(broken.error).toBeUndefined();
  expect(broken.status).toBe(1);
  expect(broken.stderr).toMatch(/pending|cancelled|AbortError/);
}, 5000);