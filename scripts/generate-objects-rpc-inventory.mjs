export const rust = [["acyclic-objects", "bin", "objects-rpc-inventory"]];
export function render([stdout]) {
  return { "compatibility/rpc-implementations/objects.json": JSON.stringify(JSON.parse(stdout), null, 2) + "\n" };
}
