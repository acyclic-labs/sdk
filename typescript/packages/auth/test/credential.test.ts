import { expect, test } from "bun:test";
import { CredentialCache, CredentialSnapshot, credentialProvider, credentialStream, resolveCredential } from "../src/credential.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

test("provider projects trusted binding and source changes without raw identity", async () => {
  let revision: string | undefined = "account/audience/1";
  const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${revision}`, revision!, 100),
    () => ({ nowUnixSeconds: 1, revision }));
  const provider = credentialProvider({ credential: cache.provider });
  const watch = provider.watch!();
  const closed = provider.watch!();
  closed.close();
  const old = await resolveCredential(provider);
  expect(old.binding).toEqual({ revision, expiresAtUnixSeconds: 100 });
  expect(watch.validity.revision).toBe(revision);
  revision = "other-account/audience/2";
  cache.invalidate();
  expect(watch.changed.aborted).toBe(true);
  expect(closed.changed.aborted).toBe(false);
  expect(() => old.check()).toThrow();
  const next = await resolveCredential(provider);
  const changed = provider.watch!();
  expect(next.binding?.revision).toBe(revision);
  expect(old.reject()).toBe(false);
  expect(changed.changed.aborted).toBe(false);
  expect(next.reject()).toBe(true);
  expect(changed.changed.aborted).toBe(true);
  revision = undefined;
  expect(provider.watch!().validity.revision).toBeUndefined();
  await expect(resolveCredential(provider)).rejects.toThrow();
  const raw = credentialProvider({ credential: () => "synthetic-raw" });
  expect(raw.watch).toBeUndefined();
  expect((await resolveCredential(raw)).binding).toBeUndefined();
});

test("cohort and warm requests share validity; expiry refreshes once", async () => {
  let now = 1;
  let calls = 0;
  let gate = deferred<CredentialSnapshot>();
  const started = deferred<void>();
  const cache = new CredentialCache(async () => { calls++; started.resolve(); return gate.promise; },
    () => ({ nowUnixSeconds: now, revision: "account/audience/1" }));
  const cohort = Array.from({ length: 100 }, () => cache.resolve());
  await started.promise;
  expect(calls).toBe(1);
  const first = new CredentialSnapshot("synthetic-first", "account/audience/1", 10);
  gate.resolve(first);
  expect((await Promise.all(cohort)).every(lease => lease === first)).toBe(true);
  await Promise.all(Array.from({ length: 100 }, () => cache.resolve()));
  expect(calls).toBe(1);
  now = 10;
  gate = deferred<CredentialSnapshot>();
  const expired = Array.from({ length: 100 }, () => cache.resolve());
  const next = new CredentialSnapshot("synthetic-next", "account/audience/1", 20);
  gate.resolve(next);
  expect((await Promise.all(expired)).every(lease => lease === next)).toBe(true);
  expect(calls).toBe(2);
});

test("late rejection and dispatch after revision change preserve exact invalidation", async () => {
  let revision = "account/audience/1";
  let generation = 0;
  const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${generation++}`, revision, 100),
    () => ({ nowUnixSeconds: 1, revision }));
  const provider = credentialProvider({ credential: cache.provider });
  const old = await resolveCredential(provider);
  expect(old.reject()).toBe(true);
  const next = await resolveCredential(provider);
  expect(old.reject()).toBe(false);
  next.check();
  revision = "other-account/audience/2";
  expect(() => next.check()).toThrow();
  const changed = await resolveCredential(provider);
  expect(next.reject()).toBe(false);
  changed.check();
});

test("last cancellation aborts and fences source; cancelling one waiter preserves cohort", async () => {
  const gate = deferred<CredentialSnapshot>();
  const started = deferred<AbortSignal>();
  let calls = 0;
  const cache = new CredentialCache(async (_rejected, signal) => {
    if (calls++ === 0) { started.resolve(signal); return gate.promise; }
    return new CredentialSnapshot("synthetic-replacement", "r", 100);
  },
    () => ({ nowUnixSeconds: 1, revision: "r" }));
  const a = new AbortController();
  const b = new AbortController();
  const first = cache.resolve(a.signal);
  const second = cache.resolve(b.signal);
  const signal = await started.promise;
  a.abort();
  await expect(first).rejects.toThrow();
  expect(signal.aborted).toBe(false);
  b.abort();
  await expect(second).rejects.toThrow();
  expect(signal.aborted).toBe(true);
  const abandoned = new CredentialSnapshot("synthetic-abandoned", "r", 100);
  gate.resolve(abandoned);
  // The abandoned result cannot publish even if its source ignores abort.
  const replacement = await cache.resolve();
  expect(replacement.token()).toBe("synthetic-replacement");
  expect(cache.reject(abandoned)).toBe(false);
  cache.check(replacement);
  expect(calls).toBe(2);
});

test("source failures and server rejected bearer cannot fall back", async () => {
  let unavailable = false;
  let failure = false;
  const cache = new CredentialCache(async () => {
    if (failure) throw new Error("source denied");
    return new CredentialSnapshot("synthetic-reused", "r", 100);
  }, () => unavailable ? { nowUnixSeconds: 1 } : { nowUnixSeconds: 1, revision: "r" });
  const old = await cache.resolve();
  cache.reject(old);
  await expect(cache.resolve()).rejects.toThrow("rejected");
  failure = true;
  await expect(cache.resolve()).rejects.toThrow("source denied");
  unavailable = true;
  await expect(cache.resolve()).rejects.toThrow("unavailable");
});

test("stream trailers reject only their exact lease; denial and cancellation preserve it", async () => {
  let generation = 0;
  const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${generation++}`, "r", 100),
    () => ({ nowUnixSeconds: 1, revision: "r" }));
  const old = await resolveCredential(cache.provider);
  async function* response(error: unknown) { yield 1; throw error; }
  const unauthenticated = new Error("unauthenticated");
  const rejected = (error: unknown) => error === unauthenticated;
  const stream = credentialStream(response(unauthenticated), old, rejected)[Symbol.asyncIterator]();
  expect((await stream.next()).value).toBe(1);
  old.reject();
  const current = await resolveCredential(cache.provider);
  await expect(stream.next()).rejects.toBe(unauthenticated);
  current.check();
  for (const failure of [new Error("policy denial"), new DOMException("cancelled", "AbortError")]) {
    await expect(Array.fromAsync(credentialStream(response(failure), current, rejected))).rejects.toBe(failure);
    current.check();
  }
  await expect(Array.fromAsync(credentialStream(response(unauthenticated), current, rejected))).rejects.toBe(unauthenticated);
  expect(() => current.check()).toThrow();
});
