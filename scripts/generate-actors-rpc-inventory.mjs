export const rust = [
  "//rust/crates/actors:actors-rpc-inventory",
  "//rust/crates/actors-wasm:actors-wasm-rpc-inventory",
];
export function render([stdout, wasm]) {
  return { "compatibility/rpc-implementations/actors.json": JSON.stringify({ ...JSON.parse(stdout), wasm: JSON.parse(wasm) }, null, 2) + "\n" };
}
