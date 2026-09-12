import { expect, test } from "bun:test";
import { Harness } from "@acyclic-labs/harness";
import { HandshakeRequestSchema } from "@acyclic-labs/harness/proto";

test("installed package resolves its protobuf and default WASM artifacts", async () => {
  expect(HandshakeRequestSchema.typeName).toBe("acyclic.harness.v1.HandshakeRequest");
  const harness = await Harness.create({
    authority: { kind: "conversation", id: "installed-package" },
    issuerId: "installed-package",
    issuerKey: new Uint8Array(32).fill(7),
  });
  expect(harness.issueScope("root", ["event:append"]).capabilities).toEqual(["event:append"]);
  harness.free();
});
