/** The one place the application-selected environment names are spelled.
 * Each names an existing public SDK interface; none is a new binding kind. */
export const HARNESS_BINDINGS = {
  storage: "eventStorage",
  objects: "immutableObjects",
  streams: "sharedStreams",
  access: "hostAccess",
  authority: "platformAuthority",
  workspace: "machinesWorkspace",
} as const;

/** The entry's protected model egress handle, declared as `entry.fetch.name`. */
export const MODEL_FETCH = "modelFetch";

/** The entry's host-only Machines endpoint, declared as `entry.hostFetch[0].name`. */
export const MACHINES_ENDPOINT = "machinesEndpoint";

/** The bundled module file and its logical manifest module identifier. */
export const HARNESS_MODULE = { file: "harness-actor.mjs", id: "harness_actor" } as const;

/** Typed refusal: a required platform binding or primitive is not available. */
export class HarnessActorError extends Error {
  constructor(readonly code: "failed_precondition", readonly subject: string) {
    super(`STEP harness-actor failed: ${subject} is not available; recovery: materialize every selected binding from its original admitted producer`);
  }
}
