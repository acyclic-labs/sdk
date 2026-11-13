import { plugin } from "bun";
import assert from "node:assert/strict";

// Run the real provider/contract with only its closed-binding admission removed.
plugin({ name: "closed-provider-negative-control", setup(build) {
  build.onLoad({ filter: /[\\/]src[\\/]stream-provider\.ts$/ }, async args => {
    const source = await Bun.file(args.path).text();
    const guard = '          if (closed) return { code: "unavailable" };';
    assert(source.includes(guard), "closed-binding guard mutation did not apply");
    return { contents: source.replace(guard, ""), loader: "ts" };
  });
} });
const { initializeClientQueries } = await import("../src/client-query.js");
await initializeClientQueries();
const { WasmStream } = await import("../generated/wasm/acyclic_harness_wasm.js");
const { streamProviderPort } = await import("../src/stream-provider.js");
const { sharedProviderTree } = await import("./support/stream-provider-contract.js");
await sharedProviderTree(streamProviderPort(new WasmStream()));