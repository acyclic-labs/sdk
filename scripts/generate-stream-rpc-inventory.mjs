export const rust = [["acyclic-stream", "bin", "stream-rpc-inventory"]];
export function render([stdout]) {
  return { "compatibility/rpc-implementations/stream.json": JSON.stringify(JSON.parse(stdout), null, 2) + "\n" };
}
