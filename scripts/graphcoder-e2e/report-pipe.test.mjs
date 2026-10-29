import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { connect } from "node:net";
import test from "node:test";
import { listenReportPipe } from "./report-pipe.mjs";

const windows = { skip: process.platform !== "win32" };
const generation = () => randomBytes(16).toString("hex");

test("the admitted Windows pipe carries both directions and joins its original channel", windows, async () => {
  const endpoint = await listenReportPipe(generation());
  const client = connect(endpoint.environment.ACYCLIC_REPORT_PIPE);
  try {
    const channel = await endpoint.connected;
    const request = once(channel, "data");
    client.write("selected request\n");
    assert.equal((await request)[0].toString(), "selected request\n");
    const acknowledgement = once(client, "data");
    channel.write("selected acknowledgement\n");
    assert.equal((await acknowledgement)[0].toString(), "selected acknowledgement\n");
    const ended = once(client, "end");
    const closed = endpoint.close();
    assert.equal(endpoint.close(), closed, "cleanup retains one physical close join");
    await Promise.all([ended, closed]);
  } finally {
    client.destroy();
    await endpoint.close();
  }
});

test("withdrawing the Windows endpoint ends the same pending connection wait", windows, async () => {
  const endpoint = await listenReportPipe(generation());
  const refused = assert.rejects(endpoint.connected, { code: "REPORT_PIPE_CLOSED" });
  await endpoint.close();
  await refused;
});

test("a second listener cannot replace an admitted Windows report endpoint", windows, async () => {
  const selected = generation();
  const endpoint = await listenReportPipe(selected);
  try {
    await assert.rejects(listenReportPipe(selected), { code: "EADDRINUSE" });
    const client = connect(endpoint.environment.ACYCLIC_REPORT_PIPE);
    try {
      const channel = await endpoint.connected;
      const delivered = once(client, "data");
      channel.write("original owner\n");
      assert.equal((await delivered)[0].toString(), "original owner\n");
    } finally { client.destroy(); }
  } finally { await endpoint.close(); }
});
