/** Typed guest authoring only; execution and publication belong to the actor host. */
import type { ActorDefinition } from "./generated/guest.js";
export * from "./generated/guest.js";

/** Preserve state and binding inference without installing an execution runtime. */
export function defineActor<State, Bindings>(
  definition: ActorDefinition<State, Bindings>,
): ActorDefinition<State, Bindings> {
  return definition;
}
