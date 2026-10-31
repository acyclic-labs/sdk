import { expect, test } from "bun:test";
import { SecretsClient } from "../src/secrets.js";
import { SecretsClient as WireClient } from "../src/wasm.js";
import { SecretKind } from "../generated/proto/auth/v1/auth_pb.js";
import { create, toBinary } from "@bufbuild/protobuf";
import { SecretListRequestSchema } from "../generated/proto/auth/v1/auth_pb.js";

// Disposable normative HTTP provider, not a captured Cloud/tool response.
test("WASM/TS secrets preserve CAS and visibility, deny each operation and redact provider errors", async () => {
  const planted = "PLANTED_SECRET_DO_NOT_LOG_6547";
  let generation = 0;
  let calls = 0;
  let fault = false;
  let row: { name: string; version: number; kind: string; value: string } | undefined;
  const server = Bun.serve({ port: 0, hostname: "127.0.0.1", async fetch(request) {
    calls++;
    expect(request.method).toBe("POST");
    expect(request.headers.get("content-type")).toBe("application/json");
    if (request.headers.get("authorization") !== "Bearer allowed-fixture") return new Response(planted, { status: 403 });
    if (fault) return Response.json({ generation, variables: [{ name: "key", version: 1, kind: "protected", value: planted }] });
    const body = await request.json() as { scope: string; name?: string; kind?: string; value?: string; expectedGeneration?: number; cursor?: { generation: number; after: string } | null };
    expect(body.scope).toBe("fixture");
    const operation = new URL(request.url).pathname.split("/").at(-1);
    if (operation === "list") {
      // Generation fence: a cursor from an older generation ends its listing.
      if (body.cursor && body.cursor.generation !== generation) return new Response(planted, { status: 409 });
      const variables = !row ? [] : row.kind === "plain" ? [row] : [{ name: row.name, version: row.version, kind: row.kind }];
      return Response.json({ generation, variables });
    }
    if (body.expectedGeneration !== generation) return new Response(planted, { status: 409 });
    generation++;
    if (operation === "put") {
      row = { name: body.name!, version: generation, kind: body.kind!, value: body.value! };
      return Response.json({ generation, name: row.name, version: row.version, kind: row.kind });
    }
    row = undefined;
    return Response.json({ generation, name: body.name, version: generation, kind: "deleted" });
  }});
  let credentials = 0;
  const client = new SecretsClient(server.url.origin, () => { credentials++; return "allowed-fixture"; });
  const denied = new SecretsClient(server.url.origin, () => "denied-fixture");
  try {
    expect((await client.list({ scope: "fixture" })).variables).toEqual([]);
    const put = { scope: "fixture", name: "key", kind: SecretKind.PROTECTED, value: planted, expectedGeneration: 0n };
    expect((await client.put(put)).generation).toBe(1n);
    const listed = await client.list({ scope: "fixture" });
    expect(JSON.stringify(listed, (_, value) => typeof value === "bigint" ? String(value) : value)).not.toContain(planted);
    expect(listed.variables[0]?.kind.case).toBe("protected");
    await expect(client.put(put)).rejects.toThrow("generation conflict");
    await expect(client.delete({ scope: "fixture", name: "key", expectedGeneration: 0n })).rejects.toThrow("generation conflict");
    for (const pending of [denied.put(put), denied.list({ scope: "fixture" }), denied.delete({ scope: "fixture", name: "key", expectedGeneration: 1n })]) {
      try { await pending; throw new Error("denied request admitted"); }
      catch (error) { expect(String(error)).toContain("permission denied"); expect(String(error)).not.toContain(planted); }
    }
    fault = true;
    await expect(client.list({ scope: "fixture" })).rejects.toThrow("invalid response");
    fault = false;
    expect((await client.put({ ...put, kind: SecretKind.PLAIN, value: "public", expectedGeneration: 1n })).generation).toBe(2n);
    const plain = (await client.list({ scope: "fixture" })).variables[0]?.kind;
    expect(plain?.case === "plain" && plain.value.value).toBe("public");
    expect((await client.delete({ scope: "fixture", name: "key", expectedGeneration: 2n })).kind).toBe(SecretKind.DELETED);
    await expect(client.list({ scope: "fixture", cursor: { generation: 2n, after: "key" }, pageSize: 1 })).rejects.toThrow("listing changed");
    expect(credentials + 3).toBe(calls);
    await client.list(create(SecretListRequestSchema, { scope: "fixture" }));
    const before = calls;
    // create() alone is the negative control: it silently drops account.
    const illegal = { scope: "fixture", account: planted };
    expect(create(SecretListRequestSchema, illegal)).not.toHaveProperty("account");
    await expect(client.list(illegal)).rejects.toThrow("invalid request");
    const wire = new WireClient(server.url.origin, () => "allowed-fixture");
    try {
      const canonical = toBinary(SecretListRequestSchema, create(SecretListRequestSchema, { scope: "fixture" }));
      const unknown = new Uint8Array([...canonical, 0x7a, 3, 98, 97, 100]);
      await expect(wire.list(unknown)).rejects.toThrow("invalid request");
    } finally { wire.free(); }
    expect(calls).toBe(before);
  } finally { client.free(); denied.free(); await server.stop(true); }
});
