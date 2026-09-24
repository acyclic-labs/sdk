import { fromJson, toJsonString } from "@bufbuild/protobuf";
import {
  ErrorCode,
  HandshakeRequestSchema,
  HandshakeResponseSchema,
  TransportKind,
  type HandshakeRequest,
  type HandshakeResponse,
  type ResumeRequest,
} from "../generated/proto/harness/v1/harness_pb.js";
import {
  GrpcWireTransport,
  HttpSseWireTransport,
  WebSocketWireTransport,
  WireError,
  httpWireError,
  validateHandshake,
  type WebSocketFactory,
  type WireConnection,
  type WireTransport,
} from "./wire-transport.js";

export type TransportKindName = "grpc" | "grpc-web" | "websocket" | "http-sse";

export interface GrpcBridge {
  handshake(request: HandshakeRequest): Promise<HandshakeResponse>;
  connect: WireTransport["connect"];
}

export interface ConnectOptions {
  negotiation: HandshakeRequest;
  /**
   * Transport preference. Defaults to the ACYCLIC_TRANSPORT environment
   * variable when defined, otherwise "auto". An explicit option always wins.
   */
  transport?: TransportKindName | "auto";
  fetcher?: typeof fetch;
  webSocketFactory?: WebSocketFactory;
  /** Without it the grpc and grpc-web kinds are ineligible. */
  grpc?: (binding: { kind: "grpc" | "grpc-web"; url: string }) => GrpcBridge;
}

interface Candidate {
  kind: TransportKindName;
  url: string;
}

const PREFERENCE: readonly TransportKindName[] = ["grpc", "grpc-web", "websocket", "http-sse"];

/**
 * Resolves the wire transport for one endpoint. `connect()` discovers the
 * transports a server advertises on every call, so reconnections re-evaluate
 * the choice; a live connection never switches transport.
 */
export function connectHarness(endpoint: string, options: ConnectOptions): NegotiatedWireTransport {
  return new NegotiatedWireTransport(endpoint, options);
}

export class NegotiatedWireTransport implements WireTransport {
  selected?: TransportKindName;

  constructor(
    readonly endpoint: string,
    readonly options: ConnectOptions,
  ) {}

  async connect(resume: ResumeRequest, signal?: AbortSignal): Promise<WireConnection> {
    const transport = await this.#resolve(signal);
    return transport.connect(resume, signal);
  }

  async #resolve(signal?: AbortSignal): Promise<WireTransport> {
    const { endpoint, options } = this;
    const scheme = endpoint.slice(0, endpoint.indexOf(":")).toLowerCase();
    switch (scheme) {
      case "ws":
      case "wss": {
        const factory = this.#webSocketFactory();
        if (factory === undefined) {
          throw new WireError(ErrorCode.UNSUPPORTED, "transport websocket is not eligible");
        }
        this.selected = "websocket";
        return new WebSocketWireTransport(endpoint, options.negotiation, factory);
      }
      case "grpc":
      case "grpcs": {
        if (options.grpc === undefined) {
          throw new WireError(ErrorCode.UNSUPPORTED, "transport grpc is not eligible");
        }
        const url = `${scheme === "grpcs" ? "https" : "http"}${endpoint.slice(scheme.length)}`;
        return this.#grpcTransport("grpc", url);
      }
      case "http":
      case "https":
        break;
      default:
        throw new WireError(ErrorCode.INVALID, `unsupported endpoint scheme: ${endpoint}`);
    }

    const fetcher = options.fetcher ?? fetch;
    const response = await fetcher(
      new URL("v1/harness/handshake", withSlash(endpoint)),
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: toJsonString(HandshakeRequestSchema, options.negotiation),
        ...(signal === undefined ? {} : { signal }),
      },
    );
    if (!response.ok) throw await httpWireError(response, "handshake");
    const handshake = fromJson(HandshakeResponseSchema, await response.json());
    validateHandshake(options.negotiation, handshake);

    const candidates = handshake.transports
      .map(binding => ({ kind: kindName(binding.kind), url: binding.url }))
      .filter((candidate): candidate is Candidate => candidate.kind !== undefined);
    // HTTP/SSE is always reachable on the discovery endpoint itself.
    if (!candidates.some(candidate => candidate.kind === "http-sse")) {
      candidates.push({ kind: "http-sse", url: endpoint });
    }

    const preference = options.transport ?? envTransport() ?? "auto";
    const eligible = (candidate: Candidate) =>
      candidate.kind === "http-sse" ||
      (candidate.kind === "websocket" && this.#webSocketFactory() !== undefined) ||
      ((candidate.kind === "grpc" || candidate.kind === "grpc-web") && options.grpc !== undefined);

    let chosen: Candidate | undefined;
    if (preference === "auto") {
      chosen = PREFERENCE.map(kind => candidates.find(c => c.kind === kind && eligible(c)))
        .find(candidate => candidate !== undefined);
    } else {
      chosen = candidates.find(c => c.kind === preference && eligible(c));
      if (chosen === undefined) {
        throw new WireError(
          ErrorCode.UNSUPPORTED,
          `transport ${preference} is not offered or eligible`,
        );
      }
    }
    if (chosen === undefined) {
      throw new WireError(ErrorCode.UNSUPPORTED, "no advertised transport is eligible");
    }
    switch (chosen.kind) {
      case "websocket":
        this.selected = "websocket";
        return new WebSocketWireTransport(chosen.url, options.negotiation, this.#webSocketFactory());
      case "grpc":
      case "grpc-web":
        return this.#grpcTransport(chosen.kind, chosen.url);
      default:
        this.selected = "http-sse";
        return new HttpSseWireTransport(chosen.url, options.negotiation, fetcher);
    }
  }

  #grpcTransport(kind: "grpc" | "grpc-web", url: string): WireTransport {
    const bridge = this.options.grpc!({ kind, url });
    this.selected = kind;
    return new GrpcWireTransport(this.options.negotiation, bridge.handshake, bridge.connect);
  }

  #webSocketFactory(): WebSocketFactory | undefined {
    return this.options.webSocketFactory ??
      (typeof WebSocket === "function" ? (url, protocols) => new WebSocket(url, protocols) : undefined);
  }
}

function kindName(kind: TransportKind): TransportKindName | undefined {
  switch (kind) {
    case TransportKind.GRPC: return "grpc";
    case TransportKind.GRPC_WEB: return "grpc-web";
    case TransportKind.WEBSOCKET: return "websocket";
    case TransportKind.HTTP_SSE: return "http-sse";
    default: return undefined;
  }
}

function envTransport(): TransportKindName | "auto" | undefined {
  const env =
    (globalThis as { process?: { env?: Record<string, string | undefined> } }).process?.env ??
    (globalThis as { Bun?: { env?: Record<string, string | undefined> } }).Bun?.env;
  const value = env?.ACYCLIC_TRANSPORT;
  return value === "grpc" || value === "grpc-web" || value === "websocket" ||
      value === "http-sse" || value === "auto"
    ? value
    : undefined;
}

function withSlash(value: string): string {
  return value.endsWith("/") ? value : `${value}/`;
}
