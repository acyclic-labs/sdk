export const rust = [["acyclic-actors", "bin", "actors-rpc-inventory"]];
export function render([stdout]) {
  return { "compatibility/rpc-implementations/actors.json": JSON.stringify(JSON.parse(stdout), null, 2) + "\n" };
}
