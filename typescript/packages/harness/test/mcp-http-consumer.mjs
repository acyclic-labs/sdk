// Shared Node/browser consumer: protocol and catalog semantics remain in Rust.
import { WasmMcpHttpTransport, validateMcpCatalog, mcpModelDefinitions, searchMcpCatalog }
  from "../generated/wasm/acyclic_harness_wasm.js";

const decode = bytes => new TextDecoder().decode(bytes);
const check = (condition, message) => { if (!condition) throw new Error(message); };

async function retain(exchange, maximum) {
  const chunks = [];
  let length = 0;
  try {
    const head = await exchange.response;
    for (;;) {
      const bytes = await exchange.read();
      if (bytes === null) break;
      length += bytes.length;
      check(length <= maximum, "retained fixture response overflow");
      chunks.push(bytes.slice());
    }
    const body = new Uint8Array(length);
    let offset = 0;
    for (const chunk of chunks) { body.set(chunk, offset); offset += chunk.length; }
    return { head, body };
  } finally { exchange.cancel(); }
}

export async function peerStats(origin, client) {
  const response = await fetch(`${origin}/stats?client=${encodeURIComponent(client)}`, { credentials: "omit" });
  check(response.ok, "fixture counters unavailable");
  return response.json();
}

export async function exerciseMcpHttp(origin, client, mode, createProvider) {
  const endpoint = `${origin}/mcp?client=${encodeURIComponent(client)}&mode=${mode}`;
  const provider = createProvider();
  const setup = new WasmMcpHttpTransport(provider, endpoint, undefined, 8192, 5000);
  let accepted;
  let transport;
  try {
    const initialization = crypto.randomUUID();
    const request = setup.initializationRequest(initialization, "qualified-host", "1");
    check((await peerStats(origin, client)).requests === 0, "construction dispatched");
    const { head, body } = await retain(provider(initialization, request), request.maximum_response_bytes);
    accepted = await setup.acceptInitialization(initialization, head, body);
    check(JSON.parse(decode(accepted.resultJson())).protocolVersion === "2025-11-25", "negotiation changed");
    const ready = accepted.initializedRequest();
    const session = ready.headers.get("mcp-session-id");
    const notification = await retain(provider(crypto.randomUUID(), ready), ready.maximum_response_bytes);
    check(notification.head.status === 202 && notification.body.length === 0, "readiness failed");
    transport = accepted.intoTransport(); accepted = undefined;
    const first = JSON.parse(decode(await transport.listToolsJson(crypto.randomUUID(), undefined)));
    check(first.nextCursor === "next", "first discovery page changed");
    const last = JSON.parse(decode(await transport.listToolsJson(crypto.randomUUID(), first.nextCursor)));
    check(last.nextCursor === undefined, "catalog is incomplete");
    const catalog = { server: "fixture", revision: "1", schema_exposure: { kind: "selected", names: ["echo"] },
      discovery: "search", tools: [...first.tools, ...last.tools] };
    validateMcpCatalog(catalog, 2, 8192);
    check(mcpModelDefinitions(catalog, 2, 8192).map(tool => tool.name).join() === "mcp.fixture.echo", "schema exposure changed");
    check(searchMcpCatalog(catalog, "find", undefined, 2, 2, 8192).length === 2, "hidden schema discovery failed");
    let disabled = false;
    try { searchMcpCatalog({ ...catalog, discovery: "disabled" }, "find", undefined, 2, 2, 8192); }
    catch { disabled = true; }
    check(disabled, "disabled discovery was accepted");
    const value = decode(await transport.callJson(crypto.randomUUID(), "echo", {}));
    check(value.includes("18446744073709551615") && JSON.parse(value).content[0].text === "héllo 🦀", "result bytes changed");
    const lostOperation = crypto.randomUUID();
    let lost = false;
    try { await transport.callJson(lostOperation, "echo", { loseResponse: true }); }
    catch { lost = true; }
    check(lost, "incomplete observation accepted");
    check(await transport.reconcileJson(lostOperation) === undefined, "invented remote receipt");
    const stats = await peerStats(origin, client);
    check(stats.requests === 6 && stats.calls === 2 && stats.session === session, "physical request count changed");
    return { endpoint, client, session, lostOperation, requests: stats.requests, calls: stats.calls };
  } finally { accepted?.free(); transport?.free(); setup.free(); }
}

export async function verifyReopenedMcpHttp(origin, observed, createProvider) {
  const transport = new WasmMcpHttpTransport(createProvider(), observed.endpoint, observed.session, 8192, 5000);
  try {
    const before = await peerStats(origin, observed.client);
    check(await transport.reconcileJson(observed.lostOperation) === undefined, "reopen invented remote receipt");
    const after = await peerStats(origin, observed.client);
    check(JSON.stringify(before) === JSON.stringify(after) && after.requests === observed.requests
      && after.calls === observed.calls, "reopen reposted a request");
  } finally { transport.free(); }
}
