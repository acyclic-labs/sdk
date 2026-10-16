import { expect, test } from "bun:test";
import fc from "fast-check";
import { credentialTlsFixture } from "./credential-tls.js";
import { StreamService } from "../packages/stream/generated/proto/stream/v1/stream_pb.js";
import { FilesystemService } from "../packages/filesystem/generated/proto/filesystem/v2/filesystem_pb.js";
import { BucketsService } from "../packages/objects/generated/proto/objects/v1/objects_pb.js";
import { MachinesService } from "../packages/machines/generated/proto/machines/v1/machines_pb.js";
import { createStreamGrpcClient } from "../packages/stream/src/grpc.js";
import { createFilesystemGrpcClient } from "../packages/filesystem/src/grpc.js";
import { createObjectsV1GrpcClients } from "../packages/objects/src/v1-grpc.js";
import { createMachinesGrpcClient } from "../packages/machines/src/grpc.js";

test("every product gRPC client resolves current credentials and stops on failure", async () => {
  const sent: string[] = [];
  const observe = (context: { requestHeader: Headers }) => { sent.push(context.requestHeader.get("authorization") ?? ""); return {}; };
  const { endpoint, certificate, close } = await credentialTlsFixture(router => {
    router.service(StreamService, { tail(_request, context) { observe(context); return { tail: 0n }; } });
    router.service(FilesystemService, { handshake(_request, context) { return observe(context); } });
    router.service(BucketsService, { headBucket(_request, context) { return observe(context); } });
    router.service(MachinesService, { listMachines(_request, context) { return observe(context); } });
  });
  try {
    await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 8 }), async values => {
      for (const product of ["streams", "fs", "objects", "machines"] as const) {
        let calls = 0;
        const options = { endpoint, caCertificate: certificate, token: "stale", credential: async () => values[calls++]! };
        let invoke: () => Promise<unknown>;
        let release: (() => void) | undefined;
        switch (product) {
          case "streams": { const client = createStreamGrpcClient(options); invoke = () => client.tail({ path: "credential/test" }); break; }
          case "fs": { const client = createFilesystemGrpcClient(options); invoke = () => client.filesystem.handshake({}); release = client.close; break; }
          case "objects": { const client = createObjectsV1GrpcClients(options); invoke = () => client.buckets.headBucket({}); release = client.close; break; }
          case "machines": { const client = createMachinesGrpcClient(options); invoke = () => client.machines.listMachines({}); release = client.close; break; }
        }
        options.credential = async () => "mutated";
        const start = sent.length;
        try {
          for (const _value of values) await invoke();
          expect(sent.slice(start)).toEqual(values.map(value => `Bearer ${value}`));
          expect(calls).toBe(values.length);
          await expect(invoke()).rejects.toThrow();
          expect(sent.length).toBe(start + values.length);
        } finally { release?.(); }
      }
    }), { numRuns: 16 });
  } finally { await close(); }
});
