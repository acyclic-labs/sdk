import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { connect } from "node:net";
import test from "node:test";
import { listenReportPipe } from "./report-pipe.mjs";

const windows = { skip: process.platform !== "win32" };
const generation = () => randomBytes(16).toString("hex");

async function read(socket) {
  const chunks = socket.iterator({ destroyOnReturn: false });
  try {
    const next = await chunks.next();
    if (next.done) throw Object.assign(new Error(
      "step=report-pipe-read; recovery=node --test scripts/graphcoder-e2e/report-pipe.test.mjs",
    ), { code: "REPORT_PIPE_EOF" });
    return next.value;
  } finally {
    await chunks.return();
  }
}

test("the admitted Windows pipe carries both directions and joins its original channel", windows, async t => {
  const endpoint = await listenReportPipe(generation());
  let client;
  t.after(async () => { client?.destroy(); await endpoint.close(); });
  client = connect(endpoint.environment.ACYCLIC_REPORT_PIPE);
  const connecting = once(client, "connect");
  const [channel] = await Promise.all([endpoint.connected, connecting]);
  const exchange = Promise.all([
    read(channel).then(bytes => {
      assert.equal(bytes.toString(), "selected request\n");
      channel.write("selected acknowledgement\n");
    }),
    read(client).then(bytes => assert.equal(bytes.toString(), "selected acknowledgement\n")),
  ]);
  client.write("selected request\n");
  await exchange;
  const buffered = once(client, "readable");
  channel.write("retained acknowledgement\n");
  await buffered;
  assert.equal((await read(client)).toString(), "retained acknowledgement\n");
  const ended = assert.rejects(read(client), { code: "REPORT_PIPE_EOF" });
  const closed = endpoint.close();
  assert.equal(endpoint.close(), closed, "cleanup retains one physical close join");
  await Promise.all([ended, closed]);
  await assert.rejects(read(client), { code: "REPORT_PIPE_EOF" });
});

test("withdrawing the Windows endpoint ends the same pending connection wait", windows, async t => {
  const endpoint = await listenReportPipe(generation());
  t.after(() => endpoint.close());
  const refused = assert.rejects(endpoint.connected, { code: "REPORT_PIPE_CLOSED" });
  await endpoint.close();
  await refused;
});

test("a second listener cannot replace an admitted Windows report endpoint", windows, async t => {
  const selected = generation();
  const endpoint = await listenReportPipe(selected);
  let client;
  t.after(async () => { client?.destroy(); await endpoint.close(); });
  await assert.rejects(listenReportPipe(selected), { code: "EADDRINUSE" });
  client = connect(endpoint.environment.ACYCLIC_REPORT_PIPE);
  const [channel] = await Promise.all([endpoint.connected, once(client, "connect")]);
  const delivered = read(client);
  channel.write("original owner\n");
  assert.equal((await delivered).toString(), "original owner\n");
});
