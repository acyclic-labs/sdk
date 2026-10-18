import { checkGraph } from "./test-support/license-graphs.mjs";

// Byte-oriented fuzz entry point. Every input defines a bounded package graph;
// graph edges may alias, cycle, or use scoped and non-ASCII package paths.
export async function fuzz(data) {
  await checkGraph(new Uint8Array(data));
}
