import { type HostedFsOptions, type HostedFsEngine, type HostedFsCapabilities } from "./contracts.js";
import { secureServiceEndpoint } from "./endpoint.js";
import { DEFAULT_HOSTED_OPTIONS } from "../generated/defaults.js";
import { positiveSafeInteger, HostedFsError, boundedFetch, call, required, type HostedClient, assertOpen, requireName, operation } from "./hosted-rpc.js";
import { type Interceptor, createClient } from "@connectrpc/connect";
import { toBinary } from "@bufbuild/protobuf";
import { FilesystemService, FilesystemProfile } from "../generated/proto/filesystem/v2/filesystem_pb.js";
import { createGrpcWebTransport } from "@connectrpc/connect-web";
import { observeInterceptors, resolveObserver } from "@acyclic-labs/actors/observe";
import { FILESYSTEM_DESCRIPTOR_DIGEST } from "../generated/descriptor-digest.js";
import { profileFromWire } from "./hosted-values.js";
import { workspace } from "./hosted-workspace.js";

export type * from "./public-types.js";
export { DEFAULT_OBJECT_CACHE_OPTIONS, DEFAULT_VOLUME_LIMITS } from "./contracts.js";
export { performanceObserver } from "@acyclic-labs/actors/observe";
export { HostedFsError, type HostedFsErrorCode } from "./hosted-rpc.js";

export const MAX_BEARER_TOKEN_BYTES = 12 * 1024;

export async function openHostedFs(options: HostedFsOptions): Promise<HostedFsEngine> {
  const endpoint = secureServiceEndpoint(options.endpoint, message => new RangeError(`hosted filesystem ${message}`));
  if (!options.bearerToken.trim() || new TextEncoder().encode(options.bearerToken).byteLength > MAX_BEARER_TOKEN_BYTES || /[\r\n\0]/.test(options.bearerToken)) {
    throw new RangeError("bearer token must be non-empty, at most 12 KiB, and free of CR, LF, or NUL");
  }
  const maximumResponseBytes = options.maximumResponseBytes ?? DEFAULT_HOSTED_OPTIONS.maximumResponseBytes;
  positiveSafeInteger(maximumResponseBytes, "maximum response bytes");
  if (maximumResponseBytes < DEFAULT_HOSTED_OPTIONS.minimumHandshakeResponseBytes) {
    throw new RangeError(
      `maximum response bytes must be at least ${DEFAULT_HOSTED_OPTIONS.minimumHandshakeResponseBytes}`,
    );
  }
  const maximumPayloadResponseBytes = maximumResponseBytes - DEFAULT_HOSTED_OPTIONS.maximumByteResponseEnvelopeBytes;
  const send = options.fetch ?? globalThis.fetch;
  if (send === undefined) throw new TypeError("this runtime does not provide fetch");
  let negotiatedMaximumRequestBytes: bigint | undefined;
  const authorize: Interceptor = (next) => async (request) => {
    request.header.set("authorization", `Bearer ${options.bearerToken}`);
    return next(request);
  };
  const boundRequest: Interceptor = (next) => async (request) => {
    if (!request.stream && negotiatedMaximumRequestBytes !== undefined) {
      const bytes = toBinary(request.method.input, request.message).byteLength;
      if (BigInt(bytes) > negotiatedMaximumRequestBytes) {
        throw new HostedFsError("limit", "hosted filesystem request exceeds the negotiated byte limit");
      }
    }
    return next(request);
  };
  const rpcClient = createClient(FilesystemService, createGrpcWebTransport({
    baseUrl: endpoint.href.replace(/\/$/, ""),
    interceptors: observeInterceptors([authorize, boundRequest], resolveObserver(options.observer), "fs"),
    fetch: boundedFetch(send, maximumResponseBytes),
  }));
  const handshake = await call(rpcClient.handshake({
    protocol: {
      protocol: { version: DEFAULT_HOSTED_OPTIONS.protocolVersion, descriptorDigest: FILESYSTEM_DESCRIPTOR_DIGEST },
      required: { capabilities: [{ name: "filesystem", version: DEFAULT_HOSTED_OPTIONS.protocolVersion }] },
    },
  }));
  const negotiated = required(handshake.protocol, "handshake response");
  const protocol = required(negotiated.protocol, "handshake protocol");
  if (protocol.version !== DEFAULT_HOSTED_OPTIONS.protocolVersion) throw new HostedFsError("protocol", "filesystem protocol version is unsupported");
  if (protocol.descriptorDigest !== FILESYSTEM_DESCRIPTOR_DIGEST) throw new HostedFsError("protocol", "filesystem descriptor digest does not match");
  const supported = required(negotiated.supported, "supported capabilities");
  if (!supported.capabilities.some(capability => capability.name === "filesystem" && capability.version === DEFAULT_HOSTED_OPTIONS.protocolVersion)) {
    throw new HostedFsError("protocol", "filesystem capability version is unsupported");
  }
  const advertised = required(handshake.capabilities, "filesystem capabilities");
  if (advertised.contractVersion !== DEFAULT_HOSTED_OPTIONS.protocolVersion) throw new HostedFsError("protocol", "filesystem contract version is unsupported");
  if (advertised.maximumRequestBytes <= 0n || advertised.maximumResponseBytes <= 0n) {
    throw new HostedFsError("protocol", "filesystem capabilities contain an unbounded byte limit");
  }
  negotiatedMaximumRequestBytes = advertised.maximumRequestBytes;
  positiveSafeInteger(advertised.maximumTransactionMutations, "maximum transaction mutations");
  positiveSafeInteger(advertised.maximumPageItems, "maximum page items");
  const profiles = advertised.profiles.map(profileFromWire);
  const negotiatedResponseBytes = advertised.maximumResponseBytes < BigInt(maximumPayloadResponseBytes)
    ? advertised.maximumResponseBytes
    : BigInt(maximumPayloadResponseBytes);
  const client: HostedClient = {
    rpc: rpcClient,
    maximumResponseBytes: Number(negotiatedResponseBytes),
    maximumTransactionMutations: advertised.maximumTransactionMutations,
    maximumPageItems: advertised.maximumPageItems,
    s3Credentials: advertised.s3Credentials,
    sourceReconciliation: advertised.sourceReconciliation,
    closed: false,
  };
  const capabilities: HostedFsCapabilities = {
    version: advertised.contractVersion,
    platform: "hosted",
    architecture: "service",
    authority: "remote",
    immutableObjects: "remote",
    nativeMount: "none",
    writableNativeMount: false,
    nativeWatch: false,
    nativeWatchBackend: "none",
    nativeWatchPersistentRestart: false,
    nativeWatchRootIdentityFencing: false,
    providerProcessIoObservable: false,
    profiles,
    maximumRequestBytes: advertised.maximumRequestBytes,
    maximumResponseBytes: negotiatedResponseBytes,
    maximumTransactionMutations: advertised.maximumTransactionMutations,
    maximumPageItems: advertised.maximumPageItems,
    nativeMountCredentials: advertised.nativeMountCredentials,
    s3Credentials: advertised.s3Credentials,
    sourceReconciliation: advertised.sourceReconciliation,
  };
  return {
    capabilities,
    async createWorkspace(name) {
      assertOpen(client);
      requireName(name);
      const response = await call(client.rpc.createWorkspace({
        name,
        profile: FilesystemProfile.PORTABLE,
        operation: operation(),
      }));
      return workspace(client, required(response.workspace, "created workspace"));
    },
    async openWorkspace(name) {
      assertOpen(client);
      requireName(name);
      const response = await call(client.rpc.openWorkspace({
        selector: { case: "name", value: name },
      }));
      return workspace(client, required(response.workspace, "opened workspace"));
    },
    close() { client.closed = true; },
  };
}
