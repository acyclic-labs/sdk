import { expect, test } from "bun:test";
import fc from "fast-check";
import { credentialGetter, credentialGetterKinds } from "./credential-getters.js";
import { WorkersDeploymentClient } from "../packages/workers/src/deployment.js";
import { create, toBinary } from "@bufbuild/protobuf";
import { HttpStreamProvider } from "../packages/stream/src/http.js";
import { HttpObjectsV1 } from "../packages/objects/src/v1-http.js";
import { HeadBucketRequestSchema } from "../packages/objects/generated/proto/objects/v1/objects_pb.js";
import { HttpMachinesProvider } from "../packages/machines/src/http.js";
import { machineId } from "../packages/machines/src/index.js";
import { HttpInferenceTransport } from "../packages/inference/src/index.js";
import { Inference } from "../packages/inference/src/handles.js";
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
  await fc.assert(fc.asyncProperty(fc.array(fc.stringMatching(/^[a-zA-Z0-9_-]{1,32}$/), { minLength: 2, maxLength: 8 }), fc.constantFrom(...credentialGetterKinds), async (values, kind) => {
    for (const product of ["streams", "fs", "objects", "machines", "inference"]) {
      let calls = 0;
      const sent = [];
      const fetcher = async (input, init) => {
        const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
        sent.push(headers.get("authorization"));
        if (product === "fs" && sent.length === 1) return handshake();
        throw new Error("fixture stopped after observing request");
      };
      const options = credentialGetter({ endpoint: "https://service.example.test", token: "stale", bearerToken: "stale", fetcher, fetch: fetcher }, async () => values[calls++], kind);
      let invoke;
      let release;
      switch (product) {
        case "streams": { const client = new HttpStreamProvider(options); invoke = () => client.tail("credential/test"); break; }
        case "fs": { const client = await openHostedFs(options); invoke = () => client.openWorkspace("credential-test"); release = () => client.close(); break; }
        case "objects": { const client = new HttpObjectsV1(options); invoke = () => client.headBucket(create(HeadBucketRequestSchema, { bucket: { name: "credential-test" } })); break; }
        case "machines": { const client = new HttpMachinesProvider(options); invoke = () => client.inspectMachine(machineId("00000000-0000-0000-0000-000000000003")); break; }
        case "inference": { const client = new HttpInferenceTransport(options.endpoint, options, fetcher); invoke = () => client.listModels(); break; }
      }
      try {
        const initial = product === "fs" ? 1 : 0;
        for (let index = initial; index < values.length; index++) await expect(invoke()).rejects.toThrow();
        expect(sent).toEqual(values.map(value => `Bearer ${value}`));
        expect(calls).toBe(values.length);
        await expect(invoke()).rejects.toThrow();
        expect(sent.length).toBe(values.length);
        expect(options.credentialReads()).toBe(1);
      } finally { release?.(); }
    }
  }), { numRuns: 32, examples: credentialGetterKinds.map(kind => [["fresh-a", "fresh-b"], kind]) });
});

test("environment constructors read every credential getter shape once", async () => {
  const originalFetch = globalThis.fetch;
  try {
    for (const product of ["machines", "inference"]) {
      for (const kind of credentialGetterKinds) {
      let calls = 0;
      const sent = [];
      globalThis.fetch = async (_input, init) => {
        sent.push(new Headers(init.headers).get("authorization"));
        throw new Error("fixture stopped after header");
      };
      const callback = () => `fresh-${++calls}`;
      const options = credentialGetter({ endpoint: "https://service.example.test", token: "stale" }, callback, kind);
      let invoke;
      if (product === "machines") {
        const client = HttpMachinesProvider.fromEnv(options);
        invoke = () => client.inspectMachine(machineId("00000000-0000-0000-0000-000000000003"));
      } else {
        const client = await Inference.fromEnv({ ACYCLIC_INFERENCE_ENDPOINT: options.endpoint }, options);
        invoke = () => client.client.listModels();
      }
      for (let index = 0; index < 3; index++) await expect(invoke()).rejects.toThrow();
      expect(options.credentialReads()).toBe(1);
      expect(sent).toEqual(["Bearer fresh-1", "Bearer fresh-2", "Bearer fresh-3"]);
      }
    }
  } finally { globalThis.fetch = originalFetch; }
});

test("Workers preserves every getter shape through Actors delegation", async () => {
  for (const kind of credentialGetterKinds) {
    const sent = [];
    let calls = 0;
    const options = credentialGetter({ endpoint: "https://service.example.test", token: "stale",
      binding: { connect: async () => ({ withCredential: async (_method, _bytes, token) => {
        sent.push(token); return new Uint8Array();
      } }) },
    }, () => `fresh-${++calls}`, kind);
    const client = new WorkersDeploymentClient(options, { put: async () => {} });
    for (let i = 0; i < 3; i++) await client.delete({ actorId: "actor-a" });
    expect(options.credentialReads()).toBe(1);
    expect(sent).toEqual(["fresh-1", "fresh-2", "fresh-3"]);
  }
});
