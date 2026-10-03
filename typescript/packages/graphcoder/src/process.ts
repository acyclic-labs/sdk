import { spawn, type ChildProcessWithoutNullStreams, type SpawnOptions } from "node:child_process";
import { randomUUID } from "node:crypto";
import { GraphCoderError } from "./api.js";
import { checkedRequestId, type GraphCoderBridge, type GraphCoderWireRequest, type GraphCoderWireResponse } from "./bridge.js";

export interface GraphCoderProcessBridgeOptions {
  readonly executable: string;
  readonly args?: readonly string[];
  readonly cwd?: string;
  /** Environment is explicit; omitted means an empty environment. */
  readonly env?: NodeJS.ProcessEnv;
  readonly maximumLineBytes?: number;
  /** Maximum number of requests awaiting a response before backpressure. */
  readonly maximumPendingRequests?: number;
  /** Optional host-defined wire cancellation control for a pending request. */
  readonly cancelMessage?: (requestId: string) => GraphCoderWireRequest | undefined;
  /** Host-only operator credential; generated when omitted and never model-visible. */
  readonly operatorToken?: string;
  readonly onDiagnostic?: (event: GraphCoderProcessDiagnostic) => void;
}

export type GraphCoderProcessDiagnostic =
  | { readonly kind: "stderr"; readonly text: string }
  | { readonly kind: "malformed_line"; readonly text: string }
  | { readonly kind: "unmatched_response"; readonly requestId: string }
  | { readonly kind: "cancelled_response"; readonly requestId: string }
  | { readonly kind: "cancel_control_failed"; readonly requestId: string; readonly message: string }
  | { readonly kind: "exit"; readonly code: number | null; readonly signal: NodeJS.Signals | null };

interface PendingRequest {
  readonly resolve: (response: GraphCoderWireResponse) => void;
  readonly reject: (error: GraphCoderError) => void;
}

export const DEFAULT_MAXIMUM_PROCESS_LINE_BYTES = 16 * 1024 * 1024;
export const DEFAULT_MAXIMUM_PENDING_PROCESS_REQUESTS = 64;

/**
 * JSON-lines bridge for a host-owned local runtime executable.
 *
 * The executable is an explicit host choice. This adapter does not discover,
 * start, or configure models, filesystems, credentials, or worker policies.
 */
export class JsonLineGraphCoderBridge implements GraphCoderBridge {
  readonly #child: ChildProcessWithoutNullStreams;
  readonly #pending = new Map<string, PendingRequest>();
  readonly #onDiagnostic: (event: GraphCoderProcessDiagnostic) => void;
  readonly #maximumLineBytes: number;
  readonly #maximumPendingRequests: number;
  readonly #cancelMessage: ((requestId: string) => GraphCoderWireRequest | undefined) | undefined;
  readonly #operatorToken: string;
  readonly #cancelled = new Set<string>();
  readonly #cancelControls = new Map<string, string>();
  #stdoutBuffer = Buffer.alloc(0);
  #closed = false;

  constructor(options: GraphCoderProcessBridgeOptions) {
    this.#maximumLineBytes = options.maximumLineBytes ?? DEFAULT_MAXIMUM_PROCESS_LINE_BYTES;
    if (!Number.isSafeInteger(this.#maximumLineBytes) || this.#maximumLineBytes < 1) {
      throw new GraphCoderError("invalid_input", "maximum process line bytes must be positive");
    }
    this.#maximumPendingRequests = options.maximumPendingRequests ?? DEFAULT_MAXIMUM_PENDING_PROCESS_REQUESTS;
    if (!Number.isSafeInteger(this.#maximumPendingRequests) || this.#maximumPendingRequests < 1) {
      throw new GraphCoderError("invalid_input", "maximum pending process requests must be positive");
    }
    this.#onDiagnostic = options.onDiagnostic ?? (() => undefined);
    this.#cancelMessage = options.cancelMessage;
    this.#operatorToken = options.operatorToken ?? randomUUID();
    if (this.#operatorToken.trim() === "") throw new GraphCoderError("invalid_input", "operator token must be nonempty");
    const spawnOptions: SpawnOptions = {
      cwd: options.cwd,
      env: { ...(options.env ?? {}), GRAPHCODER_OPERATOR_TOKEN: this.#operatorToken },
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    };
    const child = spawn(options.executable, [...(options.args ?? [])], spawnOptions);
    if (child.stdin === null || child.stdout === null || child.stderr === null) throw new GraphCoderError("transport", "bridge process did not expose piped stdio");
    this.#child = child as ChildProcessWithoutNullStreams;
    this.#child.stdout.on("data", chunk => this.#consumeStdout(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    this.#child.stderr.on("data", chunk => this.#onDiagnostic({ kind: "stderr", text: Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk) }));
    this.#child.on("error", error => this.#finish(new GraphCoderError("transport", `bridge process error: ${error.message}`)));
    this.#child.on("close", (code, signal) => {
      this.#onDiagnostic({ kind: "exit", code, signal });
      this.#finish(new GraphCoderError("transport", code === 0 ? "bridge process closed before replying" : `bridge process exited with code ${code ?? "unknown"}`));
    });
  }

  request(request: GraphCoderWireRequest): Promise<GraphCoderWireResponse> {
    if (this.#closed) return Promise.reject(new GraphCoderError("transport", "bridge process is closed"));
    try { checkedRequestId(request.request_id); }
    catch (error) { return Promise.reject(error instanceof GraphCoderError ? error : new GraphCoderError("invalid_input", String(error))); }
    if (this.#cancelled.has(request.request_id)) return Promise.reject(new GraphCoderError("invalid_input", `request id ${request.request_id} is retired after cancellation`));
    if (this.#cancelControls.has(request.request_id)) return Promise.reject(new GraphCoderError("invalid_input", `request id ${request.request_id} is reserved for a cancellation control`));
    if (this.#pending.has(request.request_id)) return Promise.reject(new GraphCoderError("invalid_input", `duplicate bridge request id ${request.request_id}`));
    if (this.#pending.size >= this.#maximumPendingRequests) return Promise.reject(new GraphCoderError("transport", "bridge pending request limit reached"));
    let line: string;
    try {
      line = `${JSON.stringify(request)}\n`;
    } catch (error) {
      return Promise.reject(new GraphCoderError("invalid_input", `bridge request is not serializable: ${error instanceof Error ? error.message : String(error)}`));
    }
    const bytes = Buffer.byteLength(line, "utf8");
    if (bytes > this.#maximumLineBytes) return Promise.reject(new GraphCoderError("invalid_input", "bridge request exceeds the configured line size"));
    return new Promise<GraphCoderWireResponse>((resolve, reject) => {
      this.#pending.set(request.request_id, { resolve, reject });
      try {
        this.#child.stdin.write(line, error => {
          if (error != null) {
            this.#pending.delete(request.request_id);
            reject(new GraphCoderError("transport", `bridge request write failed: ${error.message}`));
          }
        });
      } catch (error) {
        this.#pending.delete(request.request_id);
        reject(new GraphCoderError("transport", `bridge request write failed: ${error instanceof Error ? error.message : String(error)}`));
      }
    });
  }

  /**
   * Sends the authenticated host decision on the private operator channel.
   * The public resolve request follows only after this control response.
   */
  async operatorApprove(input: { readonly approvalId: string; readonly approved: boolean; readonly sessionId: string }): Promise<void> {
    const request = {
      request_id: `operator:${randomUUID()}`,
      method: "operator_approve",
      params: {
        session_id: input.sessionId,
        approval_id: input.approvalId,
        approved: input.approved,
        operator_token: this.#operatorToken,
      },
    } as unknown as GraphCoderWireRequest;
    const response = await this.request(request);
    if (!response.ok) throw new GraphCoderError(response.error.code, response.error.message);
  }

  /** Rejects one request while leaving the process available for later work. */
  cancel(requestId: string, reason = "bridge request cancelled"): boolean {
    const pending = this.#pending.get(requestId);
    if (pending === undefined) return false;
    this.#pending.delete(requestId);
    this.#cancelled.add(requestId);
    try {
      const control = this.#cancelMessage?.(requestId);
      if (control !== undefined) this.#writeCancelControl(requestId, control);
    } catch (error) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
    }
    pending.reject(new GraphCoderError("transport", reason));
    return true;
  }

  /** Closes the host process and rejects every request still awaiting a reply. */
  close(reason = "bridge process closed by host"): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#rejectPending(new GraphCoderError("transport", reason));
    this.#child.kill();
  }

  #consumeStdout(chunk: Buffer): void {
    if (this.#closed) return;
    this.#stdoutBuffer = Buffer.concat([this.#stdoutBuffer, chunk]);
    if (this.#stdoutBuffer.length > this.#maximumLineBytes && this.#stdoutBuffer.indexOf(0x0a) < 0) {
      this.#onDiagnostic({ kind: "malformed_line", text: "bridge response line exceeds the configured size" });
      this.#finish(new GraphCoderError("transport", "bridge response line exceeds the configured size"));
      return;
    }
    for (;;) {
      const newline = this.#stdoutBuffer.indexOf(0x0a);
      if (newline < 0) return;
      const line = this.#stdoutBuffer.subarray(0, newline);
      this.#stdoutBuffer = this.#stdoutBuffer.subarray(newline + 1);
      if (line.length > this.#maximumLineBytes) {
        this.#onDiagnostic({ kind: "malformed_line", text: "bridge response line exceeds the configured size" });
        this.#finish(new GraphCoderError("transport", "bridge response line exceeds the configured size"));
        return;
      }
      let text: string;
      try { text = new TextDecoder("utf-8", { fatal: true }).decode(line); }
      catch {
        this.#onDiagnostic({ kind: "malformed_line", text: "<invalid utf-8>" });
        this.#finish(new GraphCoderError("transport", "bridge emitted invalid UTF-8"));
        return;
      }
      this.#consumeLine(text);
      if (this.#closed) return;
    }
  }

  #consumeLine(line: string): void {
    if (line.endsWith("\r")) line = line.slice(0, -1);
    if (line.trim() === "") return;
    let value: unknown;
    try { value = JSON.parse(line); }
    catch {
      this.#onDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", "bridge emitted malformed JSON"));
      return;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value) || typeof (value as { request_id?: unknown }).request_id !== "string") {
      this.#onDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", "bridge emitted an invalid response envelope"));
      return;
    }
    let requestId: string;
    try { requestId = checkedRequestId((value as { request_id: unknown }).request_id, "bridge response id"); }
    catch (error) {
      this.#onDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", error instanceof Error ? error.message : String(error)));
      return;
    }
    const pending = this.#pending.get(requestId);
    if (pending === undefined) {
      const cancelledRequestId = this.#cancelControls.get(requestId);
      if (cancelledRequestId !== undefined) {
        this.#onDiagnostic({ kind: "cancelled_response", requestId: cancelledRequestId });
        return;
      }
      if (this.#cancelled.delete(requestId)) {
        this.#onDiagnostic({ kind: "cancelled_response", requestId });
        return;
      }
      this.#onDiagnostic({ kind: "unmatched_response", requestId });
      this.#finish(new GraphCoderError("transport", `bridge emitted an unmatched response id ${requestId}`));
      return;
    }
    this.#pending.delete(requestId);
    pending.resolve(value as GraphCoderWireResponse);
  }

  #finish(error: GraphCoderError): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#rejectPending(error);
    this.#child.kill();
  }

  #rejectPending(error: GraphCoderError): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }

  #writeCancelControl(requestId: string, request: GraphCoderWireRequest): void {
    try { checkedRequestId(request.request_id); }
    catch (error) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
      return;
    }
    if (request.request_id === requestId || this.#pending.has(request.request_id) || this.#cancelled.has(request.request_id) || this.#cancelControls.has(request.request_id)) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: "cancel control request id collides with an active or retired request" });
      return;
    }
    this.#cancelControls.set(request.request_id, requestId);
    let line: string;
    try { line = `${JSON.stringify(request)}\n`; }
    catch (error) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
      return;
    }
    if (Buffer.byteLength(line, "utf8") > this.#maximumLineBytes) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: "cancel control exceeds the configured line size" });
      return;
    }
    try {
      this.#child.stdin.write(line, error => {
        if (error != null) this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: error.message });
      });
    } catch (error) {
      this.#onDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
    }
  }
}
