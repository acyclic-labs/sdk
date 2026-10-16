import assert from "node:assert/strict";
import { plugin } from "bun";

const fault = process.env.STREAM_CREDENTIAL_FAULT;
if (fault) plugin({ name: "native-credential-negative-control", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]native-credentials\.ts$/ }, async args => {
    let source = await Bun.file(args.path).text();
    const condition = "if (active.size === 0) {";
    assert(source.includes(condition));
    if (fault === "no-abandon") {
      assert(source.includes("abandon();"));
      source = source.replace("abandon();", "void 0;");
    } else if (fault === "live-abort-set") {
      assert(source.includes("[...pending]"));
      source = source.replace("[...pending]", "pending");
    } else {
      source = source.replace(condition, fault === "any-release" ? "if (true) {" : "if (false) {");
    }
    return { contents: source, loader: "ts" };
  });
} });
const { nativeCredentials } = await import("../src/native-credentials.js");
type Event = "openA" | "openB" | "closeA" | "closeB" | "resolve" | "settle";
function* schedules(prefix: Event[] = []): Generator<Event[]> {
  const candidates: Event[] = ["openA", "openB", "closeA", "closeB", "resolve", "settle"];
  if (prefix.length === candidates.length) { yield prefix; return; }
  for (const event of candidates) {
    if (prefix.includes(event)) continue;
    if (event === "closeA" && !prefix.includes("openA")) continue;
    if (event === "closeB" && !prefix.includes("openB")) continue;
    if (event === "settle" && !prefix.includes("resolve")) continue;
    yield* schedules([...prefix, event]);
  }
}
let explored = 0;
for (const events of schedules()) for (let aborts = 0; aborts < 4; aborts++) {
  const active = new Set<string>();
  const controllers = { A: new AbortController(), B: new AbortController() };
  const release: Partial<Record<"A" | "B", () => void>> = {};
  let signal: AbortSignal | undefined;
  let finish: ((value: string) => void) | undefined;
  let outcome: Promise<string> | undefined;
  let expected = "cancelled";
  let settled = false;
  let called = 0;
  let fenced = 0, resolvedEpoch = 0;
  const credentials = nativeCredentials(observed => {
    resolvedEpoch = fenced;
    signal = observed;
    called++;
    assert(observed);
    return new Promise<string>((resolve, reject) => {
      const abort = () => reject(observed.reason);
      observed.addEventListener("abort", abort, { once: true });
      finish = value => { observed.removeEventListener("abort", abort); resolve(value); };
    });
  }, () => { fenced++; });
  for (const event of events) {
    const id = event.endsWith("A") ? "A" : "B";
    if (event.startsWith("open")) {
      active.add(id);
      release[id] = credentials.retain(controllers[id].signal);
    } else if (event.startsWith("close")) {
      active.delete(id);
      const alreadyCancelled = signal?.aborted;
      if (aborts & (id === "A" ? 1 : 2)) controllers[id].abort();
      else release[id]?.();
      release[id]?.();
      if (signal && !settled && !alreadyCancelled) {
        assert.equal(signal.aborted, active.size === 0,
          active.size === 0 ? "last request leaked credentials" : "healthy request cancelled");
        if (active.size === 0) assert(fenced > resolvedEpoch, "abandoned credential must be fenced before abort");
      }
    } else if (event === "resolve") {
      outcome = credentials.resolve().then(value => value, () => "cancelled");
      expected = active.size === 0 ? "cancelled" : "fixture";
    } else {
      if (signal?.aborted) expected = "cancelled";
      finish?.("fixture");
      assert.equal(await outcome, expected);
      settled = true;
    }
  }
  assert(active.size === 0);
  const previous = called;
  await assert.rejects(credentials.resolve(), error => error instanceof DOMException && error.name === "AbortError");
  assert.equal(called, previous, "no credential work after the last cursor closes");
  explored++;
}
assert.equal(explored, 360);
// An abort callback can synchronously open a new cursor and request credentials.
// Cancellation must affect the captured requests, without cancelling the new one.
let secondSignal: AbortSignal | undefined;
let secondFinish: ((value: string) => void) | undefined;
let secondRelease: (() => void) | undefined;
let secondOutcome: Promise<string> | undefined;
let calls = 0;
const reentrant = nativeCredentials(signal => {
  assert(signal);
  if (++calls === 1) return new Promise<string>((_resolve, reject) => {
    signal.addEventListener("abort", () => {
      secondRelease = reentrant.retain();
      secondOutcome = reentrant.resolve().catch(() => "cancelled");
      reject(signal.reason);
    }, { once: true });
  });
  secondSignal = signal;
  return new Promise<string>((resolve, reject) => {
    secondFinish = resolve;
    signal.addEventListener("abort", () => reject(signal.reason), { once: true });
  });
});
const firstRelease = reentrant.retain();
const firstOutcome = reentrant.resolve().catch(() => "cancelled");
firstRelease();
assert.equal(await firstOutcome, "cancelled");
assert.equal(secondSignal?.aborted, false, "reentrant healthy request cancelled");
secondFinish?.("fixture");
assert.equal(await secondOutcome, "fixture");
secondRelease?.();
console.log("native credential schedules", explored);
