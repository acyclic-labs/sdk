import { expect, test } from "bun:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";

import { HostedFsError, openHostedFs } from "../dist/hosted.js";
import {
  CapabilitiesSchema,
  CredentialRequestSchema,
  CredentialResponseSchema,
  GenerationRefSchema,
  GenerationResponseSchema,
  HandshakeResponseSchema,
  S3CredentialSchema,
  WorkspaceRefSchema,
  WorkspaceResponseSchema,
  WorkspaceSchema,
} from "../generated/proto/filesystem/v2/filesystem_pb.js";

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

function fixtureFetch(requests, credentialCase = "s3") {
  return async (input, init) => {
    const request = input instanceof Request ? input : new Request(input, init);
    const method = new URL(request.url).pathname.split("/").at(-1);
    if (method === "Handshake") {
      return grpcWebResponse(HandshakeResponseSchema, create(HandshakeResponseSchema, {
        capabilities: create(CapabilitiesSchema, {
          contractVersion: "filesystem.v2",
          maximumTransactionMutations: 64,
          maximumPageItems: 64,
        }),
      }));
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
    if (method === "IssueS3Credential") {
      const framed = new Uint8Array(await request.arrayBuffer());
      requests.push(fromBinary(CredentialRequestSchema, framed.subarray(5)));
      const credential = credentialCase === "s3"
        ? {
            case: "s3",
            value: create(S3CredentialSchema, {
              bucket: "bucket-1",
              region: "eu-west-2",
              accessKeyId: "access",
              secretAccessKey: "secret",
              sessionToken: "session",
            }),
          }
        : { case: "bearerToken", value: "wrong-kind" };
      return grpcWebResponse(CredentialResponseSchema, create(CredentialResponseSchema, {
        endpoint: "https://s3.example.test",
        expiresAtUnixSeconds: 1234n,
        credential,
      }));
    }
    return new Response("missing fixture", { status: 500 });
  };
}

test("hosted S3 access preserves scope, expiry, idempotency, and response fields", async () => {
  const requests = [];
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch(requests),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  const idempotencyKey = new Uint8Array(16).fill(3);
  const access = await workspace.s3Access(true, 60n, idempotencyKey);

  expect(access).toEqual({
    endpoint: "https://s3.example.test",
    expiresAtUnixSeconds: 1234n,
    bucket: "bucket-1",
    region: "eu-west-2",
    accessKeyId: "access",
    secretAccessKey: "secret",
    sessionToken: "session",
  });
  expect(requests).toHaveLength(1);
  expect(requests[0].workspace?.workspaceId).toEqual(workspaceRef.workspaceId);
  expect(requests[0].generation?.generationId).toEqual(generationRef.generationId);
  expect(requests[0].writable).toBe(true);
  expect(requests[0].expiresAfterSeconds).toBe(60n);
  expect(requests[0].operation?.idempotencyKey).toEqual(idempotencyKey);
  fs.close();
});

test("hosted S3 access rejects a credential of the wrong wire kind", async () => {
  const fs = await openHostedFs({
    endpoint: "https://filesystem.example.test",
    bearerToken: "token",
    fetch: fixtureFetch([], "bearerToken"),
  });
  const workspace = await fs.createWorkspace("hosted-s3");
  expect(workspace.s3Access(false, 60n)).rejects.toEqual(
    new HostedFsError("protocol", "missing S3 credential"),
  );
  fs.close();
});
