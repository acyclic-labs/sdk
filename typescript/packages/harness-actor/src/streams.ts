import type { ImmutableObjects, StreamLedger } from "@acyclic-labs/harness/hosting";
import { HARNESS_BINDINGS, HarnessActorError } from "./bindings.js";

/** Bind the original shared ledger and separately admitted Objects capability.
 * The consistency-domain owner retains change publication, placement and wakes;
 * the executing Actor owns only its EventAgentHost metadata. */
export function harnessStreamLedger(streams: Pick<StreamLedger, "register" | "dispatch" | "open">,
  objects: ImmutableObjects): StreamLedger {
  if (typeof streams?.register !== "function" || typeof streams.dispatch !== "function" || typeof streams.open !== "function") {
    throw new HarnessActorError("failed_precondition", HARNESS_BINDINGS.streams);
  }
  return {
    register: (actor, paths) => streams.register(actor, paths),
    dispatch: (actor, operation, input) => streams.dispatch(actor, operation, input),
    open: (operation, input, preceding) => streams.open(operation, input, preceding),
    objectLength: key => objects.length(key),
    objectRead: (key, maximum) => objects.read(key, maximum),
    objectPut: (keys, values) => objects.put(keys, values),
  };
}
