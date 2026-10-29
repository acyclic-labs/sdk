import { beforeAll, test } from "bun:test";
import "fake-indexeddb/auto";
import { WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { initializeClientQueries } from "../src/client-query.js";
import { streamProviderPort } from "../src/event-host.js";
import { sharedProviderTree } from "./support/stream-provider-contract.js";

beforeAll(initializeClientQueries);

test("hosted provider bindings share the original fork and activation cut", async () => {
  await sharedProviderTree(streamProviderPort(new WasmStream()));
  await sharedProviderTree(streamProviderPort(await WasmStream.openBrowser(
    `shared-provider-${crypto.randomUUID()}`, 64, 1048576n)));
});
