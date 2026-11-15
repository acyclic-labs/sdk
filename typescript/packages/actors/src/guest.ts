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

/** The binding `name` of `env`, or a {@link DependencyUnavailableError} naming it. */
export function requireBinding<T>(env: object, name: string): NonNullable<T> {
  const value: unknown = (env as Record<string, unknown>)[name];
  if (value === undefined || value === null) throw new DependencyUnavailableError(name);
  return value as NonNullable<T>;
}

const passes = (error: unknown) => error instanceof DependencyUnavailableError ||
  typeof error === "object" && error !== null && ["AbortError", "TimeoutError"].includes((error as { name?: unknown }).name as string);

/** `value` whose own calls, and its methods' calls, raise every synchronous throw
 * or rejection as a {@link DependencyUnavailableError} naming `name`, with the
 * original as `cause`. Cancellation and an already typed failure pass unchanged.
 * Use it only for a binding whose every failure means the dependency failed. */
export function dependencyBinding<T extends object>(name: string, value: T): T {
  dependencyName(name);
  const fail = (error: unknown): never => {
    throw passes(error) ? error : new DependencyUnavailableError(name, { cause: error });
  };
  const call = (member: (...args: unknown[]) => unknown, self: unknown, args: unknown[]): unknown => {
    let result: unknown;
    try { result = Reflect.apply(member, self, args); } catch (error) { return fail(error); }
    return result !== null && typeof result === "object" && typeof (result as PromiseLike<unknown>).then === "function"
      ? Promise.resolve(result).catch(fail) : result;
  };
  return new Proxy(value, {
    apply: (target, self, args) => call(target as (...args: unknown[]) => unknown, self, args),
    get(target, key) {
      const member: unknown = Reflect.get(target, key, target);
      return typeof member === "function"
        ? (...args: unknown[]) => call(member as (...args: unknown[]) => unknown, target, args)
        : member;
    },
  });
}
