// Canonical generation inputs and their published copies live here.
// Every family that negotiates a protocol ships the shared handshake schema.
/** @type {[string, string[], string?][]} */
export const packagedTypeScriptBindings = [
  ["filesystem/v2/filesystem_pb", ["filesystem"]],
  ["protocol/v1/protocol_pb", ["filesystem"]],
  // Service bindings import the proto2 custom options descriptor transitively.
  ["validation/v1/options_pb", ["auth", "inference", "actors", "filesystem", "machines", "objects", "stream"], "compatibility"],
  ["auth/v1/auth_pb", ["auth"]],
  ["inference/v1/inference_pb", ["inference"]],
  ["machines/v1/machines_pb", ["machines"]],
  ["harness/v2/resources_pb", ["machines", "actors"], "compatibility"],
  ["objects/v1/objects_pb", ["objects"]],
  ["stream/v1/stream_pb", ["stream"], "rust/crates/stream-wire"],
  ["stream/v1/session_pb", ["stream"], "rust/crates/stream-wire"],
  ["actors/v1/actors_pb", ["actors"]],
];

// These packages intentionally own no protobuf outputs after protocol retirement.
export const retiredTypeScriptBindingPackages = ["harness"];

/** @param {ReadonlySet<string>} packages
 * @returns {[string, string[], string?][]} */
export function selectTypeScriptBindings(packages) {
  /** @type {[string, string[], string?][]} */
  const bindings = [];
  for (const [stem, names, sourceRoot] of packagedTypeScriptBindings) {
    const selected = names.filter(name => packages.has(name));
    if (selected.length) bindings.push([stem, selected, sourceRoot]);
  }
  return bindings;
}
