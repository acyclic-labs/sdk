import initialize, { WasmStream } from "/stream/generated/wasm/acyclic_stream_wasm.js";
await initialize();
const name = new URL(location.href).searchParams.get("database");
const stream = await WasmStream.openBrowser(name, 65_536, 256n * 1024n * 1024n);
postMessage({ ready: true });
onmessage = async ({ data }) => {
  try { postMessage({ id: data.id, value: await stream.dispatch(data.operation, data.input) }); }
  catch (error) { postMessage({ id: data.id, error: String(error.stack ?? error) }); }
};
