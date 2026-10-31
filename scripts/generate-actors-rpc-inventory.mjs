export const rust = [
  ["acyclic-actors", "bin", "actors-rpc-inventory"],
  ["acyclic-actors-wasm", "bin", "actors-wasm-rpc-inventory"],
];
export function render([stdout, wasm]) {
  return { "compatibility/rpc-implementations/actors.json": JSON.stringify({ ...JSON.parse(stdout), wasm: JSON.parse(wasm) }, null, 2) + "\n" };
}
