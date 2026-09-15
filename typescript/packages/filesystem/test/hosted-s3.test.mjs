import { expect, test } from "bun:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

import { HostedFsError, openHostedFs } from "../dist/hosted.js";
import {
  ApplyTransactionRequestSchema,
  CapabilitiesSchema,
  CredentialRequestSchema,
  CredentialResponseSchema,
  GenerationRefSchema,
  GenerationResponseSchema,
  HandshakeResponseSchema,
  FilesystemProfile,
  MutationResponseSchema,
  MutationStatus,
  S3CredentialSchema,
  SourceInvalidationReason,
  SourceResponseSchema,
  SourceState,
  WorkspaceRefSchema,
  WorkspaceResponseSchema,
  WorkspaceSchema,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";
import {
  CapabilitySchema,
  CapabilitySetSchema,
  HandshakeResponseSchema as HarnessHandshakeResponseSchema,
  ProtocolIdentitySchema,
} from "../generated/proto/harness/v1/harness_pb.js";

const descriptorDigest = "371d83258cb3ff55f97e01011ded1b0222e586df09c229c4f42dfb9a37952d4e";

const workspaceRef = create(WorkspaceRefSchema, {
  workspaceId: new Uint8Array(16).fill(1),
  name: "hosted-s3",
});
const generationRef = create(GenerationRefSchema, {
  workspace: workspaceRef,
  generationId: new Uint8Array(32).fill(2),
});

function grpcWebResponse(schema, value) {
  const message = frame(0, toBinary(schema, value));
  const trailers = frame(0x80, new TextEncoder().encode("grpc-status: 0\r\n"));
  const body = new Uint8Array(message.length + trailers.length);
  body.set(message);
  body.set(trailers, message.length);
  return new Response(body, {
    status: 200,
    headers: { "content-type": "application/grpc-web+proto" },
  });
}

function frame(flag, payload) {
  const result = new Uint8Array(payload.length + 5);
  result[0] = flag;
  new DataView(result.buffer).setUint32(1, payload.length, false);
  result.set(payload, 5);
  return result;
}

function validHandshake(sourceReconciliation = false) {
  return create(HandshakeResponseSchema, {
    harness: create(HarnessHandshakeResponseSchema, {
      protocol: create(ProtocolIdentitySchema, { version: "1", descriptorDigest }),
      supported: create(CapabilitySetSchema, {
        capabilities: [create(CapabilitySchema, { name: "filesystem", version: "1" })],
      }),
    }),
    capabilities: create(CapabilitiesSchema, {
      contractVersion: "1",
      profiles: [FilesystemProfile.PORTABLE],
      maximumRequestBytes: 8_388_608n,
      maximumResponseBytes: 16_777_216n,
      maximumTransactionMutations: 64,
      maximumPageItems: 64,
      s3Credentials: true,
      sourceReconciliation,
    }),
  });
}

function fixtureFetch(
  requests,
  credentialCase = "s3",
  expiresAtUnixSeconds = BigInt(Math.floor(Date.now() / 1_000) + 60),
  handshake = validHandshake(),
  methods = [],
  sourceResponse,
) {
  return async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const method = new URL(request.url).pathname.split("/").at(-1);
    methods.push(method);
    if (method === "Handshake") {
      return grpcWebResponse(HandshakeResponseSchema, handshake);
    }
    if (method === "CreateWorkspace") {
      return grpcWebResponse(WorkspaceResponseSchema, create(WorkspaceResponseSchema, {
        workspace: create(WorkspaceSchema, {
          workspace: workspaceRef,
          name: workspaceRef.name,
          head: generationRef,
        }),
      }));
    }
    if (method === "GetHead") {
      return grpcWebResponse(GenerationResponseSchema, create(GenerationResponseSchema, {
        generation: generationRef,
      }));
    }
    if (method === "ApplyTransaction") {
      const framed = new Uint8Array(await request.arrayBuffer());
      requests.push(fromBinary(ApplyTransactionRequestSchema, framed.subarray(5)));
      return grpcWebResponse(MutationResponseSchema, create(MutationResponseSchema, {
        status: MutationStatus.COMMITTED,
        generation: generationRef,
      }));
    }
    if (["GetSourceState", "ReconcileSource", "RescanSource", "SealSource"].includes(method)) {
      if (sourceResponse !== undefined) {
        return grpcWebResponse(SourceResponseSchema, sourceResponse);
      }
      const sealed = method === "SealSource";
      const pending = method === "GetSourceState";
      return grpcWebResponse(SourceResponseSchema, create(SourceResponseSchema, {
        state: sealed ? SourceState.SEALED : pending ? SourceState.NEEDS_RESCAN : SourceState.CLEAN,
        reason: pending ? SourceInvalidationReason.QUEUE_OVERFLOW : SourceInvalidationReason.UNSPECIFIED,
        ...(pending ? {} : { generation: generationRef }),
      }));
    }
    if (method === "IssueS3Credential") {
      const framed = new Uint8Array(await request.arrayBuffer());
      requests.push(fromBinary(CredentialRequestSchema, framed.subarray(5)));
      const credential = credentialCase !== "bearerToken"
        ? {
            case: "s3",
            value: create(S3CredentialSchema, {
              bucket: credentialCase === "malformed" ? "" : "bucket-1",
              region: "eu-west-2",
              accessKeyId: "access",
              secretAccessKey: "secret",
              sessionToken: "session",
            }),
          }
        : { case: "bearerToken", value: "wrong-kind" };
      return grpcWebResponse(CredentialResponseSchema, create(CredentialResponseSchema, {
        endpoint: "https://s3.example.test",
        expiresAtUnixSeconds,
        credential,
      }));
    }
    return new Response("missing fixture", { status: 500 });
  };
}

test("hosted S3 access preserves scope, expiry, idempotency, and response fields", async () => {
  const requests = [];
  const expiresAtUnixSeconds = BigInt(Math.floor(Date.now() / 1_000) + 60);
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch(requests, "s3", expiresAtUnixSeconds),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  const idempotencyKey = new Uint8Array(16).fill(3);
  const access = await workspace.s3Access(true, 60n, idempotencyKey);

  expect(access).toEqual({
    endpoint: "https://s3.example.test",
    expiresAtUnixSeconds,
    bucket: "bucket-1",
    region: "eu-west-2",
    accessKeyId: "access",
    secretAccessKey: "secret",
    sessionToken: "session",
  });
  expect(requests).toHaveLength(1);
  expect(fs.capabilities).toMatchObject({
    version: "1",
    profiles: ["portable"],
    maximumRequestBytes: 8_388_608n,
    maximumResponseBytes: 16_777_216n,
    maximumTransactionMutations: 64,
    maximumPageItems: 64,
    s3Credentials: true,
    sourceReconciliation: false,
  });
  expect(requests[0].workspace?.workspaceId).toEqual(workspaceRef.workspaceId);
  expect(requests[0].generation?.generationId).toEqual(generationRef.generationId);
  expect(requests[0].writable).toBe(true);
  expect(requests[0].expiresAfterSeconds).toBe(60n);
  expect(requests[0].operation?.idempotencyKey).toEqual(idempotencyKey);
  fs.close();
});

test("hosted source lifecycle rejects inconsistent state and reason pairs", async () => {
  const malformed = [
    create(SourceResponseSchema, {
      state: SourceState.CLEAN,
      reason: SourceInvalidationReason.QUEUE_OVERFLOW,
      generation: generationRef,
    }),
    create(SourceResponseSchema, {
      state: SourceState.NEEDS_RESCAN,
      reason: SourceInvalidationReason.UNSPECIFIED,
    }),
  ];
  for (const sourceResponse of malformed) {
    const fs = await openHostedFs({
      endpoint: "https://filesystem.example.test",
      bearerToken: "token",
      fetch: fixtureFetch([], "s3", undefined, validHandshake(true), [], sourceResponse),
    });
    const workspace = await fs.createWorkspace("hosted-s3");
    await expect(workspace.sourceState()).rejects.toMatchObject({ code: "invalid_response" });
    fs.close();
  }
});

test("hosted source lifecycle follows the advertised provider capability", async () => {
  const methods = [];
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", undefined, validHandshake(true), methods),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  expect(fs.capabilities.sourceReconciliation).toBe(true);
  expect(await workspace.sourceState()).toEqual({
    status: "needs-rescan",
    reason: "queue-overflow",
    generationId: undefined,
  });
  expect((await workspace.reconcileSource(new Uint8Array(16).fill(3))).status).toBe("clean");
  expect((await workspace.rescanSource(new Uint8Array(16).fill(4))).generationId).toEqual(generationRef.generationId);
  expect((await workspace.seal(new Uint8Array(16).fill(5))).id).toEqual(generationRef.generationId);
  expect(methods).toEqual([
    "Handshake", "CreateWorkspace", "GetSourceState", "ReconcileSource", "RescanSource", "SealSource",
  ]);
  fs.close();
});

test("hosted S3 access rejects a credential of the wrong wire kind", async () => {
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "bearerToken"),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  await expect(workspace.s3Access(false, 60n)).rejects.toEqual(
    new HostedFsError("protocol", "missing S3 credential"),
  );
  fs.close();
});

test("hosted S3 access rejects credentials that have already expired", async () => {
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", 1n),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  await expect(workspace.s3Access(false, 60n)).rejects.toEqual(
    new HostedFsError("invalid_response", "S3 credential expiry must be in the future"),
  );
  fs.close();
});

test("hosted S3 access rejects unsupported and malformed grants before exposure", async () => {
  const unsupportedHandshake = validHandshake();
  unsupportedHandshake.capabilities.s3Credentials = false;
  const unsupportedRequests = [];
  const unsupported = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch(unsupportedRequests, "s3", undefined, unsupportedHandshake),
  });
  const unsupportedWorkspace = await unsupported.createWorkspace("hosted-s3");
  await expect(unsupportedWorkspace.s3Access(false, 60n)).rejects.toEqual(
    new HostedFsError("unsupported", "hosted filesystem does not issue S3 credentials"),
  );
  expect(unsupportedRequests).toHaveLength(0);
  unsupported.close();

  const malformed = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "malformed"),
  });
  const malformedWorkspace = await malformed.createWorkspace("hosted-s3");
  await expect(malformedWorkspace.s3Access(false, 60n)).rejects.toThrow("name must be non-empty");
  malformed.close();
});

test("hosted transaction commits respect the negotiated conflict limit", async () => {
  const handshake = validHandshake();
  handshake.capabilities.maximumPageItems = 1;
  const requests = [];
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch(requests, "s3", undefined, handshake),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  const transaction = await workspace.beginTransaction(new Uint8Array(16).fill(4));
  await transaction.createDirectory("/bounded");
  expect((await transaction.commit()).status).toBe("committed");
  expect(requests).toHaveLength(1);
  expect(requests[0].maximumConflicts).toBe(1);
  fs.close();
});

test("hosted discovery rejects an incomplete or incompatible handshake", async () => {
  const missingHarness = create(HandshakeResponseSchema, {
    capabilities: validHandshake().capabilities,
  });
  await expect(openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", undefined, missingHarness),
  })).rejects.toEqual(new HostedFsError("invalid_response", "handshake response is absent"));

  const wrongDigest = validHandshake();
  wrongDigest.harness.protocol.descriptorDigest = "substituted";
  await expect(openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", undefined, wrongDigest),
  })).rejects.toEqual(new HostedFsError("protocol", "filesystem descriptor digest does not match"));
});

test("hosted requests fail locally when negotiated byte or page limits are exceeded", async () => {
  const tinyRequestHandshake = validHandshake();
  tinyRequestHandshake.capabilities.maximumRequestBytes = 1n;
  const byteLimitedMethods = [];
  const byteLimited = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", undefined, tinyRequestHandshake, byteLimitedMethods),
  });
  await expect(byteLimited.createWorkspace("too-large")).rejects.toThrow(
    "hosted filesystem request exceeds the negotiated byte limit",
  );
  expect(byteLimitedMethods).toEqual(["Handshake"]);
  byteLimited.close();

  const pageLimitedMethods = [];
  const pageLimited = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "s3", undefined, validHandshake(), pageLimitedMethods),
  });
  const workspace = await pageLimited.createWorkspace("hosted-s3");
  const before = pageLimitedMethods.slice();
  await expect(workspace.diff({}, {}, 65)).rejects.toThrow(
    "maximum changes exceeds the negotiated page limit",
  );
  expect(pageLimitedMethods).toEqual(before);
  pageLimited.close();
});
