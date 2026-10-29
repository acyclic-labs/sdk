/** The one place the application-selected environment names are spelled.
 * Each names an existing public SDK interface; none is a new binding kind. */
export const HARNESS_BINDINGS = {
  storage: "eventStorage",
  objects: "immutableObjects",
  journal: "streamJournal",
  access: "hostAccess",
  authority: "platformAuthority",
  workspace: "machinesWorkspace",
} as const;

/** The entry's protected model egress handle, declared as `entry.fetch.name`. */
export const MODEL_FETCH = "modelFetch";

/** The bundled module file and its logical manifest module identifier. */
export const HARNESS_MODULE = { file: "harness-actor.mjs", id: "harness_actor" } as const;
