import { WasmStream, initializeClientQueries } from "../src/client-query.js";
import { exerciseSwarm } from "./swarm-client.scenario.js";
export async function test(endpoint: string) {
  await initializeClientQueries();
  const stream = WasmStream.connect(endpoint, () => "browser-test");
  try {
    const result = await exerciseSwarm(stream, async () => {
      const before = await stream.nextConnection(0n);
      await fetch(`${endpoint}/fixture/recover`, { method: "POST" });
      if (await stream.nextConnection(before) <= before) throw new Error("recovery did not push its generation");
    });
    const connections = await (await fetch(`${endpoint}/fixture/connections`)).text();
    if (connections !== "2") throw new Error("swarm did not share one session across reconnect");
    return { ...result, sessions: 1, reconnects: 1 };
  } finally { stream.free(); }
}
