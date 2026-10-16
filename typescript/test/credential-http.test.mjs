import { expect, test } from "bun:test";
import fc from "fast-check";
import { create, toBinary } from "@bufbuild/protobuf";
import { HttpStreamProvider } from "../packages/stream/src/http.js";
import { HttpObjectsV1 } from "../packages/objects/src/v1-http.js";
import { HeadBucketRequestSchema } from "../packages/objects/generated/proto/objects/v1/objects_pb.js";
import { HttpMachinesProvider } from "../packages/machines/src/http.js";
import { machineId } from "../packages/machines/src/index.js";
import { openHostedFs } from "../packages/filesystem/src/hosted.js";
import { HandshakeResponseSchema, FilesystemProfile } from "../packages/filesystem/generated/proto/filesystem/v2/filesystem_pb.js";
import { FILESYSTEM_DESCRIPTOR_DIGEST } from "../packages/filesystem/generated/descriptor-digest.js";
import { DEFAULT_HOSTED_OPTIONS } from "../packages/filesystem/generated/defaults.js";

function handshake() {
  const bytes = toBinary(HandshakeResponseSchema, create(HandshakeResponseSchema, {
    protocol: {
      protocol: { version: DEFAULT_HOSTED_OPTIONS.protocolVersion, descriptorDigest: FILESYSTEM_DESCRIPTOR_DIGEST },
      supported: { capabilities: [{ name: "filesystem", version: DEFAULT_HOSTED_OPTIONS.protocolVersion }] },
    },
    capabilities: {
      contractVersion: DEFAULT_HOSTED_OPTIONS.protocolVersion, profiles: [FilesystemProfile.PORTABLE],
      maximumRequestBytes: 8388608n, maximumResponseBytes: BigInt(DEFAULT_HOSTED_OPTIONS.maximumResponseBytes),
      maximumTransactionMutations: 64, maximumPageItems: 64,
    },
  }));
  const trailers = new TextEncoder().encode("grpc-status: 0\r\n");
  const body = new Uint8Array(bytes.length + trailers.length + 10);
  new DataView(body.buffer).setUint32(1, bytes.length);
  body.set(bytes, 5);
  body[bytes.length + 5] = 128;
  new DataView(body.buffer).setUint32(bytes.length + 6, trailers.length);
  body.set(trailers, bytes.length + 10);
  return new Response(body, { headers: { "content-type": "application/grpc-web+proto" } });
}

test("HTTP clients renew for each request and never fall back to fixed credentials", async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 8 }), async values => {
    for (const product of ["streams", "fs", "objects", "machines"]) {
      let calls = 0;
      const sent = [];
      const fetcher = async (input, init) => {
        const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
        sent.push(headers.get("authorization"));
        if (product === "fs" && sent.length === 1) return handshake();
        throw new Error("fixture stopped after observing request");
      };
      const options = { endpoint: "https://service.example.test", token: "stale", bearerToken: "stale", credential: async () => values[calls++], fetcher, fetch: fetcher };
      let invoke;
      let release;
      switch (product) {
        case "streams": { const client = new HttpStreamProvider(options); invoke = () => client.tail("credential/test"); break; }
        case "fs": { const client = await openHostedFs(options); invoke = () => client.openWorkspace("credential-test"); release = () => client.close(); break; }
        case "objects": { const client = new HttpObjectsV1(options); invoke = () => client.headBucket(create(HeadBucketRequestSchema, { bucket: { name: "credential-test" } })); break; }
        case "machines": { const client = new HttpMachinesProvider(options); invoke = () => client.inspectMachine(machineId("00000000-0000-0000-0000-000000000003")); break; }
      }
      options.credential = async () => "mutated";
      try {
        const initial = product === "fs" ? 1 : 0;
        for (let index = initial; index < values.length; index++) await expect(invoke()).rejects.toThrow();
        expect(sent).toEqual(values.map(value => `Bearer ${value}`));
        expect(calls).toBe(values.length);
        await expect(invoke()).rejects.toThrow();
        expect(sent.length).toBe(values.length);
      } finally { release?.(); }
    }
  }), { numRuns: 32 });
});
