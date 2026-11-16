import { describe, expect, test } from "bun:test";
import { DependencyUnavailableError, GuestStepError, dependencyBinding, readinessStep, requireBinding } from "../src/guest.js";

const wire = (error: DependencyUnavailableError) => JSON.stringify({ type: error.type, detail: error.detail });

describe("guest dependency failures", () => {
  test("an absent binding is the GuestError wire form naming it", () => {
    const present = { objects: { open() {} } };
    expect(requireBinding(present, "objects")).toBe(present.objects);
    let thrown: unknown;
    try { requireBinding({ objects: null }, "objects"); } catch (error) { thrown = error; }
    expect(thrown).toBeInstanceOf(DependencyUnavailableError);
    // The exact canonical Rust serde form of DependencyUnavailable.
    expect(wire(thrown as DependencyUnavailableError)).toBe('{"type":"dependencyUnavailable","detail":{"dependency":"objects"}}');
  });

  test("a dependency name is never prose", () => {
    for (const name of ["", "objects is down", "1objects", "a".repeat(64), "objects\n"])
      expect(() => new DependencyUnavailableError(name)).toThrow(TypeError);
    expect(() => dependencyBinding("machines workspace", {})).toThrow(TypeError);
    expect(new DependencyUnavailableError("_".padEnd(63, "a")).detail.dependency).toHaveLength(63);
  });

  test("every failure of a wrapped binding names it and keeps its cause", async () => {
    const original = new Error("connection reset");
    const workspace = dependencyBinding("machinesWorkspace", {
      admit: async (request: bigint) => { if (request === 0n) throw original; return { request }; },
      check(): never { throw original; },
    });
    expect(await workspace.admit(1n)).toEqual({ request: 1n });
    const rejected = await workspace.admit(0n).then(() => undefined, (error: unknown) => error);
    expect(rejected).toBeInstanceOf(DependencyUnavailableError);
    expect((rejected as DependencyUnavailableError).detail.dependency).toBe("machinesWorkspace");
    expect((rejected as Error).cause).toBe(original);
    expect(() => workspace.check()).toThrow(DependencyUnavailableError);
    const resolve = dependencyBinding("modelFetch", (fail: boolean) => { if (fail) throw original; return 7; });
    expect(resolve(false)).toBe(7);
    expect(() => resolve(true)).toThrow(DependencyUnavailableError);
  });

  test("a frozen binding is wrapped like any other", async () => {
    // A host freezes its admitted bindings; proxying the frozen value itself
    // made every member read a TypeError, so readiness refused the binding.
    const original = new Error("connection reset");
    // Negative control: a get trap on the frozen value itself cannot return a wrapper.
    const wrapped = new Proxy(Object.freeze({ admit() {} }), { get: () => () => undefined });
    expect(() => wrapped.admit).toThrow(TypeError);
    const workspace = dependencyBinding("machinesWorkspace", Object.freeze({
      admit: async (request: bigint) => { if (request === 0n) throw original; return { request }; },
      limit: 3,
    }));
    expect(typeof workspace.admit).toBe("function");
    expect(workspace.limit).toBe(3);
    expect(await workspace.admit(1n)).toEqual({ request: 1n });
    const rejected = await workspace.admit(0n).then(() => undefined, (error: unknown) => error);
    expect(rejected).toBeInstanceOf(DependencyUnavailableError);
    expect((rejected as Error).cause).toBe(original);
    const resolve = dependencyBinding("modelFetch", Object.freeze((fail: boolean) => { if (fail) throw original; return 7; }));
    expect(typeof resolve).toBe("function");
    expect(resolve(false)).toBe(7);
    expect(() => resolve(true)).toThrow(DependencyUnavailableError);
  });

  test("a readiness step names a rejected thenable that is not a native Promise", async () => {
    const original = new Error("connection reset");
    const foreign: PromiseLike<never> = { then: (_resolve, reject) => { reject?.(original); return foreign as never; } };
    const error = await Promise.resolve(readinessStep(cause => new GuestStepError("harness.open", { cause }), () => foreign))
      .then(() => undefined, (error: unknown) => error) as GuestStepError;
    expect(error).toBeInstanceOf(GuestStepError);
    expect(error.detail.step).toBe("harness.open");
    expect(error.cause).toBe(original);
  });

  test("cancellation is not a dependency failure", async () => {
    const abort = new DOMException("stopped", "AbortError");
    const workspace = dependencyBinding("machinesWorkspace", { admit: async () => { throw abort; } });
    expect(await workspace.admit().then(() => undefined, (error: unknown) => error)).toBe(abort);
  });
});
