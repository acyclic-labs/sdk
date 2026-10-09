import init from "../generated/wasm/acyclic_harness_wasm.js";
import { createBrowserMcpHttpProvider } from "../dist/mcp-http.js";
import { exerciseMcpHttp } from "./mcp-http-consumer.mjs";

onmessage = async event => {
  try {
    await init();
    const { origin, client } = event.data;
    postMessage({ observed: await exerciseMcpHttp(origin, client, "sse", createBrowserMcpHttpProvider) });
  } catch (error) { postMessage({ error: String(error.stack ?? error) }); }
};
