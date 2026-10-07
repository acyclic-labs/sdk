import { buildProviderWasm } from "./build-provider-wasm.mjs";

const [outputArgument, ...unexpected] = process.argv.slice(2);
if (unexpected.length > 0) throw new Error("usage: build-actors-wasm.mjs [OUTPUT_DIR]");
await buildProviderWasm("actors", outputArgument);
