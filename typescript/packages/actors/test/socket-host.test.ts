import { expect, test } from "bun:test";
import { Buffer } from "node:buffer";
import { loadWasmModule } from "../src/client.js";
import { SocketHostClient } from "../src/socket-host.js";
import type { SocketCommand } from "../src/generated/guest.js";

test("socket effects retain selected caller bytes before the WASM await", async () => {
  const wasm = await loadWasmModule();
  for (const buffer of [false, true]) for (const attachment of [false, true]) {
    const backing = buffer ? Buffer.alloc(32) : new Uint8Array(32);
    const payload = backing.subarray(7, 10);
    payload.set([7, 11, 13]);
    if (!buffer) Object.defineProperty(payload, "slice", {value() {throw new Error("caller slice invoked");}});
    const refusal = new Error("original host refused");
    let captured: Uint8Array | undefined;
    const host = new SocketHostClient(async packet => {captured = packet; throw refusal;});
    const command: SocketCommand = attachment ? {type: "setAttachment", connection: "socket"} :
      {type: "send", connection: "socket", binary: true};
    const pending = attachment ? host.setAttachment("socket", payload) : host.send("socket", payload);
    backing.fill(0);
    await expect(pending).rejects.toBe(refusal);
    expect(captured).toEqual(wasm.encodeSocketRequest(command, Uint8Array.of(7, 11, 13)));
  }
  const refusal = new Error("original host refused");
  let captured: Uint8Array | undefined;
  const host = new SocketHostClient(async packet => {captured = packet; throw refusal;});
  const value = {request: "ping", response: "pong"};
  const pending = host.setAutoResponse(value);
  value.request = value.response = "reused";
  await expect(pending).rejects.toBe(refusal);
  expect(captured).toEqual(wasm.encodeSocketRequest({type: "setAutoResponse", value: {request: "ping", response: "pong"}}, new Uint8Array()));
});
