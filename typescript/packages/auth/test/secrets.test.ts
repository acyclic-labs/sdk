import { expect, test } from "bun:test";
import { SecretsClient } from "../src/secrets.js";
import { SecretsClient as WireClient } from "../src/wasm.js";
import { SecretKind, SecretListRequestSchema } from "../generated/proto/auth/v1/auth_pb.js";
import { create, toBinary } from "@bufbuild/protobuf";

// The embedding has no original invocation ingress. Its former successful
// request path is unavailable until that prerequisite is supplied; shared Rust
// conformance retains CAS, visibility, permission and response-redaction cases.
test("WASM/TS secrets refuse missing invocation ownership before credentials and HTTP", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  let calls = 0;
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", fetch() {
    calls++;
    return new Response(planted, { status: 403 });
  }});
  let credentials = 0;
  const credential = () => { credentials++; return "allowed-fixture"; };
  const client = new SecretsClient(server.url.origin, credential);
  const wire = new WireClient(server.url.origin, credential);
  async function refused(pending: Promise<unknown>) {
    const outcome = await pending.then(
      () => ({ code: "unexpected_success", message: "request admitted" }),
      (error: unknown) => error,
    );
    expect(outcome).toMatchObject({ code: "missing_operation_context" });
    expect(String(outcome)).not.toContain(planted);
  }
  try {
    await refused(client.list({ scope: "fixture" }));
    await refused(client.put({ scope: "fixture", name: "key", kind: SecretKind.PROTECTED, value: planted, expectedGeneration: 0n }));
    await refused(client.delete({ scope: "fixture", name: "key", expectedGeneration: 0n }));
    const canonical = toBinary(SecretListRequestSchema, create(SecretListRequestSchema, { scope: "fixture" }));
    await refused(wire.list(canonical));
    // create() alone silently drops account: both original admission boundaries
    // must still reject unknown selectors without invoking the credential owner.
    const illegal = { scope: "fixture", account: planted };
    expect(create(SecretListRequestSchema, illegal)).not.toHaveProperty("account");
    await expect(client.list(illegal)).rejects.toThrow("invalid request");
    const unknown = new Uint8Array([...canonical, 0x7a, 3, 98, 97, 100]);
    await expect(wire.list(unknown)).rejects.toThrow("invalid request");
    expect(credentials).toBe(0);
    expect(calls).toBe(0);
  } finally { client.free(); wire.free(); await server.stop(true); }
});
