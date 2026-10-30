import { expect, test } from "bun:test";
import { ActorActivation } from "../src/activation.js";

test("ready delivery avoids a queue, while an exclusive owner still gates and fences it", async () => {
  const activation = new ActorActivation(() => 42);
  await activation.deliver(state => state);
  let entered = false;
  const ready = activation.deliver(state => { entered = true; return state; });
  // Negative control: an empty exclusive admission defers this ready handler.
  expect(entered).toBe(true);
  expect(await ready).toBe(42);

  const exclusive = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
  const owner = activation.blockConcurrencyWhile(async () => { exclusive.resolve(); await release.promise; });
  await exclusive.promise;
  entered = false;
  const queued = activation.deliver(() => { entered = true; });
  const settled = Promise.allSettled([owner, queued]);
  expect(entered).toBe(false);
  activation.fence();
  // The original owner may never produce a result after host loss.
  try {
    for (const result of await settled) {
      expect(result.status).toBe("rejected");
      if (result.status === "rejected") expect(result.reason).toMatchObject({ code: "unavailable" });
    }
    expect(entered).toBe(false);
  } finally { release.resolve(); }
});

test("ready delivery registers constant completion work independent of dormant activations", async () => {
  const activation = new ActorActivation(() => 42);
  await activation.deliver(state => state);
  async function registrations(forceExclusive: boolean): Promise<number> {
    const then = Promise.prototype.then;
    let count = 0;
    Promise.prototype.then = function (...args) { count++; return then.apply(this, args); };
    try {
      if (forceExclusive) void activation.blockConcurrencyWhile(state => state);
      expect(await activation.deliver(state => state)).toBe(42);
      return count;
    } finally { Promise.prototype.then = then; }
  }
  const ready = await registrations(false);
  const dormant = Array.from({ length: 100_000 }, () => new ActorActivation(() => 0));
  expect(await registrations(false)).toBe(ready);
  // Restore the removed empty section: the work oracle must detect it.
  expect(await registrations(true)).toBeGreaterThan(ready);
  expect(await dormant[99_999]!.deliver(state => state)).toBe(0);
});

test("initialization and exclusive sections gate every handler; failed or fenced heaps never revive", async () => {
  let release!: () => void; const held = new Promise<void>(resolve => { release = resolve; });
  let initializations = 0, deliveries = 0;
  const activation = new ActorActivation(async () => { initializations++; await held; return 42; });
  const a = activation.deliver(state => { deliveries++; return state; });
  const b = activation.deliver(state => { deliveries++; return state; });
  expect(deliveries).toBe(0); release(); expect(await Promise.all([a,b])).toEqual([42,42]);
  expect(initializations).toBe(1);
  await expect(activation.blockConcurrencyWhile(() => { throw new Error("initialization failed"); })).rejects.toThrow();
  await expect(activation.deliver(() => { deliveries++; })).rejects.toThrow(); expect(deliveries).toBe(2);
  let finish!: () => void; const gate = new Promise<void>(resolve => { finish = resolve; });
  const fenced = new ActorActivation(async () => { await gate; return 1; });
  const pending = fenced.deliver(() => { deliveries++; }); fenced.fence(); finish();
  await expect(pending).rejects.toThrow(); await expect(fenced.deliver(() => { deliveries++; })).rejects.toThrow();
  expect(deliveries).toBe(2);
});

test("capture closes admission, drains reentrant handlers, then acquires exclusive state", async () => {
  let reentered = 0, successorCalls = 0;
  const entered = Promise.withResolvers<void>(), resume = Promise.withResolvers<void>();
  const attempting = Promise.withResolvers<void>();
  const capturing = Promise.withResolvers<void>(), publish = Promise.withResolvers<void>();
  const activation = new ActorActivation(() => ({ value: 0 }));
  const delivery = activation.deliver(async () => {
    entered.resolve();
    await resume.promise;
    attempting.resolve();
    await activation.blockConcurrencyWhile(state => { reentered++; state.value++; });
  });
  await entered.promise;
  const capture = activation.quiesce(async state => {
    capturing.resolve();
    await publish.promise;
    return state.value;
  });
  const successor = activation.deliver(state => { successorCalls++; return ++state.value; });
  try {
    resume.resolve();
    await attempting.promise;
    // Hand the queue to the already admitted exclusive section once. Capture
    // must not own that section while waiting for this handler to finish.
    await new Promise<void>(resolve => queueMicrotask(resolve));
    expect(reentered).toBe(1);
    await delivery;
    await capturing.promise;
    expect(successorCalls).toBe(0);
    publish.resolve();
    expect(await capture).toBe(1);
    expect(await successor).toBe(2);
  } finally {
    activation.fence(); resume.resolve(); publish.resolve();
    await Promise.allSettled([delivery, capture, successor]);
  }
});

test("queued admission belongs to its capture cut; host loss settles every current and late observer", async () => {
  const initialize = Promise.withResolvers<number>();
  const activation = new ActorActivation(() => initialize.promise);
  const order: string[] = [];
  const a = activation.deliver(() => { order.push("first"); });
  const b = activation.quiesce(() => { order.push("capture"); });
  const c = activation.deliver(() => { order.push("second"); });
  const d = activation.quiesce(() => { order.push("capture-second"); });
  initialize.resolve(1);
  await Promise.all([a, b, c, d]);
  expect(order).toEqual(["first", "capture", "second", "capture-second"]);

  const held = Promise.withResolvers<number>(), entered = Promise.withResolvers<void>();
  const lost = new ActorActivation(() => { entered.resolve(); return held.promise; });
  let effects = 0;
  const pending = [lost.deliver(() => { effects++; }), lost.quiesce(() => { effects++; }), lost.deliver(() => { effects++; })];
  const settled = Promise.allSettled(pending);
  const failures: unknown[] = [];
  for (const operation of pending) void operation.catch(error => { failures.push(error); });
  await entered.promise;
  try {
    lost.fence();
    // Ownership loss synchronously rejects observers; observe that notification
    // on the next microtask, before releasing the original blocked producer.
    await Promise.resolve();
    expect(failures).toHaveLength(3);
    for (const result of await settled) {
      expect(result.status).toBe("rejected");
      if (result.status === "rejected") expect(result.reason.code).toBe("unavailable");
    }
    await expect(lost.deliver(() => { effects++; })).rejects.toMatchObject({ code: "unavailable" });
    await expect(lost.quiesce(() => { effects++; })).rejects.toMatchObject({ code: "unavailable" });
    expect(effects).toBe(0);
  } finally {
    held.resolve(1);
    await settled;
  }
});

test("capture registers constant completion work across admitted populations", async () => {
  async function registrations(population: number): Promise<number> {
    const activation = new ActorActivation(() => 1);
    const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<void>();
    let admitted = 0;
    const deliveries = Array.from({ length: population }, () => activation.deliver(async () => {
      if (++admitted === population) entered.resolve();
      await release.promise;
    }));
    await entered.promise;
    const then = Promise.prototype.then;
    let count = 0;
    Promise.prototype.then = function (...args) { count++; return then.apply(this, args); };
    try {
      const capture = activation.quiesce(state => state);
      // Execute the already queued capture preparation before delivering the
      // original handlers' completion event. This is one handoff, not a probe.
      await new Promise<void>(resolve => queueMicrotask(resolve));
      release.resolve();
      expect(await capture).toBe(1);
      return count;
    } finally {
      Promise.prototype.then = then;
      release.resolve();
      await Promise.all(deliveries);
    }
  }
  const one = await registrations(1);
  expect(await registrations(128)).toBe(one);
  expect(await registrations(4096)).toBe(one);
});

test("fencing between admission and delivery prevents every handler effect", async () => {
  const activation = new ActorActivation(() => 1);
  const then = Promise.prototype.then;
  let observed = false, effects = 0;
  let delivery: Promise<void>;
  Promise.prototype.then = function (...args) {
    const operation = then.apply(this, args);
    if (!observed) {
      observed = true;
      // Observe the actual admission completion before its handler continuation.
      void then.call(operation, () => activation.fence(), () => undefined);
    }
    return operation;
  };
  try { delivery = activation.deliver(() => { effects++; }); }
  finally { Promise.prototype.then = then; }
  await expect(delivery!).rejects.toThrow("fenced");
  expect(effects).toBe(0);
  await expect(activation.deliver(() => { effects++; })).rejects.toThrow();
  expect(effects).toBe(0);
});

// Each public call owns its internal sections' error, independently of peers.
test.each(["initialization", "capture"])("%s failure preserves its initiating error and fences peers", async origin => {
  const original = new Error(`${origin} failed`);
  const entered = Promise.withResolvers<void>(), release = Promise.withResolvers<number>();
  const activation = new ActorActivation(() => {
    if (origin === "initialization") { entered.resolve(); return release.promise; }
    return 1;
  });
  let effects = 0;
  const initiating = origin === "initialization"
    ? activation.deliver(() => { effects++; })
    : activation.quiesce(() => { entered.resolve(); return release.promise; });
  const peer = activation.deliver(() => { effects++; });
  const settled = Promise.allSettled([initiating, peer]);
  await entered.promise;
  release.reject(original);
  const [first, second] = await settled;
  expect(first!.status).toBe("rejected");
  if (first!.status === "rejected") expect(first!.reason).toBe(original);
  expect(second!.status).toBe("rejected");
  if (second!.status === "rejected") expect(second!.reason).toMatchObject({ code: "unavailable", cause: original });
  await expect(activation.deliver(() => { effects++; })).rejects.toMatchObject({ code: "unavailable", cause: original });
  expect(effects).toBe(0);
});
