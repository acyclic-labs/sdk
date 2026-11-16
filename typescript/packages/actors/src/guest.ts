/** Typed guest authoring only; execution and publication belong to the actor host. */
import type { ActorDefinition } from "./generated/guest.js";
export * from "./generated/guest.js";

/** Preserve state and binding inference without installing an execution runtime. */
export function defineActor<State, Bindings>(
  definition: ActorDefinition<State, Bindings>,
): ActorDefinition<State, Bindings> {
  return definition;
}

/** The environment name grammar the manifest validator admits for bindings and
 * host endpoints (Rust `domain::valid_environment_name`). */
const DEPENDENCY_NAME = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
function dependencyName(name: string): string {
  if (!DEPENDENCY_NAME.test(name)) throw new TypeError("a guest dependency is a binding or endpoint name");
  return name;
}

/** A manifest binding or host endpoint the guest required is absent or failed.
 * Its `type` and `detail` are the `GuestError` wire form, so the host commits it
 * as `DependencyUnavailable { dependency }`; the message is never read. */
export class DependencyUnavailableError extends Error {
  readonly type = "dependencyUnavailable" as const;
  readonly detail: { readonly dependency: string };
  constructor(dependency: string, options?: { readonly cause?: unknown }) {
    super("guest dependency is unavailable", options);
    this.name = "DependencyUnavailableError";
    this.detail = Object.freeze({ dependency: dependencyName(dependency) });
  }
}

/** Every readiness step an SDK guest runtime names that consumes no binding.
 * A closed set: a host admits a step only from this list (its own copy, kept
 * equal by its conformance), so a step name is never guest prose. */
export const GUEST_STEPS = Object.freeze([
  "harness.hosting", "harness.model", "harness.providers", "harness.open", "harness.manifest",
] as const);
export type GuestStep = (typeof GUEST_STEPS)[number];

/** A guest runtime's own readiness step failed, named only by its
 * {@link GUEST_STEPS} entry. The message and `cause` stay in the guest. */
export class GuestStepError extends Error {
  readonly type = "guestStep" as const;
  readonly detail: { readonly step: GuestStep };
  constructor(step: GuestStep, options?: { readonly cause?: unknown }) {
    super("guest readiness step failed", options);
    if (!(GUEST_STEPS as readonly string[]).includes(step)) throw new TypeError("a guest step is a declared GUEST_STEPS entry");
    this.name = "GuestStepError";
    this.detail = Object.freeze({ step });
  }
}

const typed = (error: unknown) => error instanceof DependencyUnavailableError || error instanceof GuestStepError ||
  typeof error === "object" && error !== null && ["AbortError", "TimeoutError"].includes((error as { name?: unknown }).name as string);

/** A promise of any realm, or any other object or function with a callable `then`. */
const thenable = (value: unknown): value is PromiseLike<unknown> =>
  (typeof value === "object" || typeof value === "function") && value !== null &&
  typeof (value as PromiseLike<unknown>).then === "function";

/** `run()`'s result, or its throw or rejection raised as `fail(cause)`: the
 * typed failure of the one readiness step it is. Cancellation and an already
 * typed failure pass unchanged, so the innermost step names the failure. */
export function readinessStep<T>(fail: (cause: unknown) => Error, run: () => T): T {
  const raise = (error: unknown): never => { throw typed(error) ? error : fail(error); };
  let result: T;
  try { result = run(); } catch (error) { return raise(error); }
  return thenable(result) ? Promise.resolve(result).catch(raise) as T : result;
}

/** The binding `name` of `env`, or a {@link DependencyUnavailableError} naming it. */
export function requireBinding<T>(env: object, name: string): NonNullable<T> {
  const value: unknown = (env as Record<string, unknown>)[name];
  if (value === undefined || value === null) throw new DependencyUnavailableError(name);
  return value as NonNullable<T>;
}

/** `value` whose own calls, and its methods' calls, raise every synchronous throw
 * or rejection as a {@link DependencyUnavailableError} naming `name`, with the
 * original as `cause`. Cancellation and an already typed failure pass unchanged.
 * Use it only for a binding whose every failure means the dependency failed. */
export function dependencyBinding<T extends object>(name: string, value: T): T {
  dependencyName(name);
  const fail = (error: unknown): never => {
    throw typed(error) ? error : new DependencyUnavailableError(name, { cause: error });
  };
  const call = (member: (...args: unknown[]) => unknown, self: unknown, args: unknown[]): unknown => {
    let result: unknown;
    try { result = Reflect.apply(member, self, args); } catch (error) { return fail(error); }
    return thenable(result) ? Promise.resolve(result).catch(fail) : result;
  };
  // The Proxy shadows a fresh shell, never `value`: a frozen binding's
  // non-configurable members would forbid the get trap returning wrapped ones.
  const shell = (typeof value === "function" ? () => undefined : {}) as T;
  return new Proxy(shell, {
    apply: (_shell, self, args) => call(value as unknown as (...args: unknown[]) => unknown, self, args),
    get(_shell, key) {
      const member: unknown = Reflect.get(value, key, value);
      return typeof member === "function"
        ? (...args: unknown[]) => call(member as (...args: unknown[]) => unknown, value, args)
        : member;
    },
  });
}
