import { expect, test } from "bun:test";
import { CredentialCache, CredentialSnapshot, credentialProvider, credentialStream, resolveCredential } from "../src/credential.js";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
  return { promise, resolve, reject };
}

test("observation cannot admit or republish synchronously invalidated custody", async () => {
  for (const change of ["invalidate", "reject"] as const) {
    let observeChange = () => {};
    let calls = 0;
    const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${++calls}`, "r", 100),
      () => { const change = observeChange; observeChange = () => {}; change(); return { nowUnixSeconds: 1, revision: "r" }; });
    const old = await cache.resolve();
    observeChange = () => { if (change === "invalidate") cache.invalidate(); else cache.reject(old); };
    expect(() => cache.check(old)).toThrow();
    const next = await cache.resolve();
    expect(next).not.toBe(old);
    expect(calls).toBe(2);
    cache.check(next);
  }
  for (const cancel of [false, true]) {
    let observeChange = () => {};
    let abandoned: CredentialSnapshot | undefined;
    let calls = 0;
    const controller = new AbortController();
    const cache = new CredentialCache(async () => {
      const snapshot = new CredentialSnapshot(`synthetic-${++calls}`, "r", 100);
      if (calls === 1) {
        abandoned = snapshot;
        observeChange = () => { if (cancel) controller.abort(); else cache.invalidate(); };
      }
      return snapshot;
    }, () => { const change = observeChange; observeChange = () => {}; change(); return { nowUnixSeconds: 1, revision: "r" }; });
    await expect(cache.resolve(controller.signal)).rejects.toThrow();
    expect(() => cache.check(abandoned!)).toThrow();
    const current = await cache.resolve();
    expect(current).not.toBe(abandoned);
    expect(calls).toBe(2);
    cache.check(current);
  }
});

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

test("source observation can invalidate a registered watch and recover from refusal", () => {
  let action = "unchanged";
  const cache = new CredentialCache(async () => new CredentialSnapshot("synthetic", "r", 100), () => {
    if (action === "change") cache.invalidate();
    if (action === "refuse") throw new Error("source unavailable");
    return { nowUnixSeconds: 1, revision: "r" };
  });
  const unchanged = cache.watch();
  expect(unchanged.changed.aborted).toBe(false);
  action = "change";
  const reentrant = cache.watch();
  expect(unchanged.changed.aborted).toBe(true);
  expect(reentrant.changed.aborted).toBe(true);
  action = "refuse";
  expect(() => cache.watch()).toThrow("source unavailable");
  action = "unchanged";
  const next = cache.watch();
  expect(next.changed.aborted).toBe(false);
  next.close();
  cache.invalidate();
  expect(next.changed.aborted).toBe(false);
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

test("request cancellation preserves shared source custody and exact rejection", async () => {
  let calls = 0;
  const cache = new CredentialCache(async () => new CredentialSnapshot(`synthetic-${++calls}`, "r", 100),
    () => ({ nowUnixSeconds: 1, revision: "r" }));
  const cancelled = new AbortController();
  const old = await resolveCredential(cache.provider, cancelled.signal);
  const peer = await resolveCredential(cache.provider);
  cancelled.abort();
  expect(() => old.check()).toThrow(DOMException);
  peer.check();
  expect(calls).toBe(1);
  expect(peer.reject()).toBe(true);
  const next = await resolveCredential(cache.provider);
  expect(old.reject()).toBe(false);
  next.check();
  expect(calls).toBe(2);
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

test("source invalidation settles the whole cohort even when renewal ignores abort", async () => {
  const entered = deferred<void>();
  const held = deferred<CredentialSnapshot>();
  let revision = "first";
  let calls = 0;
  const cache = new CredentialCache(async () => {
    if (++calls === 1) { entered.resolve(); return held.promise; }
    return new CredentialSnapshot("synthetic-current", revision, 100);
  }, () => ({ nowUnixSeconds: 1, revision }));
  const cohort = [cache.resolve(), cache.resolve()];
  await entered.promise;
  revision = "second";
  cache.invalidate();
  const results = await Promise.allSettled(cohort);
  expect(results.every(result => result.status === "rejected")).toBe(true);
  const current = await cache.resolve();
  held.resolve(new CredentialSnapshot("synthetic-abandoned", "first", 100));
  expect(await cache.resolve()).toBe(current);
  expect(calls).toBe(2);
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
  const lateRejection = stream.next().then(() => undefined, error => error);
  old.reject();
  const current = await resolveCredential(cache.provider);
  expect(await lateRejection).toBe(unauthenticated);
  current.check();
  for (const failure of [new Error("policy denial"), new DOMException("cancelled", "AbortError")]) {
    await expect(Array.fromAsync(credentialStream(response(failure), current, rejected))).rejects.toBe(failure);
    current.check();
  }
  await expect(Array.fromAsync(credentialStream(response(unauthenticated), current, rejected))).rejects.toBe(unauthenticated);
  expect(() => current.check()).toThrow();
});

test("stream custody refuses changed records and completion and retires its iterator", async () => {
  for (const paused of [false, true]) {
    let revision = "first";
    let pulls = 0;
    const cache = new CredentialCache(async () => new CredentialSnapshot("synthetic", revision, 100),
      () => ({ nowUnixSeconds: 1, revision }));
    const lease = await resolveCredential(cache.provider);
    async function* records() { pulls++; yield 1; pulls++; yield 2; }
    const stream = credentialStream(records(), lease, () => false)[Symbol.asyncIterator]();
    if (paused) expect((await stream.next()).value).toBe(1);
    revision = "second";
    await expect(stream.next()).rejects.toThrow("source changed or expired");
    // Refusal must precede the source's first pull or next side effect.
    expect(pulls).toBe(paused ? 1 : 0);
  }
  for (const completion of [false, true]) {
    let revision = "first";
    let retired = false;
    const cache = new CredentialCache(async () => new CredentialSnapshot("synthetic", revision, 100),
      () => ({ nowUnixSeconds: 1, revision }));
    const lease = await resolveCredential(cache.provider);
    async function* records() {
      try {
        yield 1;
        revision = "second";
        if (!completion) yield 2;
      } finally { retired = true; }
    }
    const stream = credentialStream(records(), lease, () => false)[Symbol.asyncIterator]();
    expect((await stream.next()).value).toBe(1);
    await expect(stream.next()).rejects.toThrow("source changed or expired");
    expect(retired).toBe(true);
    // A current source remains usable after the old iterator is refused.
    const current = await resolveCredential(cache.provider);
    current.check();
  }
});
