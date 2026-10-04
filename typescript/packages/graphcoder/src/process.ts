import type { ChildProcess, ChildProcessWithoutNullStreams, SpawnOptions } from "node:child_process";
import { randomUUID } from "node:crypto";
import { GraphCoderError } from "./api.js";
import { checkedRequestId, type GraphCoderBridge, type GraphCoderWireRequest, type GraphCoderWireResponse } from "./bridge.js";
import { defaultOwnedProcessOwner, getOwnedProcessRecovery, type OwnedProcessOwner, type OwnedProcessTermination } from "./owned-process.js";

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
  /** Optional native process owner. Omit to use the bounded Node fallback. */
  readonly processOwner?: OwnedProcessOwner;
  readonly onDiagnostic?: (event: GraphCoderProcessDiagnostic) => void;
}

export type GraphCoderProcessDiagnostic =
  | { readonly kind: "stderr"; readonly text: string }
  | { readonly kind: "malformed_line"; readonly text: string }
  | { readonly kind: "unmatched_response"; readonly requestId: string }
  | { readonly kind: "cancelled_response"; readonly requestId: string }
  | { readonly kind: "cancel_control_failed"; readonly requestId: string; readonly message: string }
  | { readonly kind: "termination"; readonly outcome: OwnedProcessTermination }
  | { readonly kind: "exit"; readonly code: number | null; readonly signal: NodeJS.Signals | null };

/** The terminal state observed from the owned child process. */
export interface GraphCoderProcessExit {
  readonly kind: "closed";
  readonly code: number | null;
  readonly signal: NodeJS.Signals | null;
}

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
  readonly #processOwner: OwnedProcessOwner;
  readonly #cancelled = new Set<string>();
  readonly #cancelControls = new Map<string, string>();
  readonly #exitWaiters = new Set<{
    readonly resolve: (exit: GraphCoderProcessExit) => void;
    readonly reject: (error: GraphCoderError) => void;
  }>();
  #stdoutBuffer = Buffer.alloc(0);
  #closed = false;
  #exit: GraphCoderProcessExit | undefined;
  #termination: Promise<OwnedProcessTermination> | undefined;
  #terminationDone = false;

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
    this.#processOwner = options.processOwner ?? defaultOwnedProcessOwner;
    this.#operatorToken = options.operatorToken ?? randomUUID();
    if (this.#operatorToken.trim() === "") throw new GraphCoderError("invalid_input", "operator token must be nonempty");
    const spawnOptions: SpawnOptions = {
      cwd: options.cwd,
      env: { ...(options.env ?? {}), GRAPHCODER_OPERATOR_TOKEN: this.#operatorToken },
      stdio: ["pipe", "pipe", "pipe"],
      windowsHide: true,
    };
    let child: ChildProcess;
    try {
      child = this.#processOwner.spawn(options.executable, options.args ?? [], spawnOptions);
    } catch (error) {
      const recovery = getOwnedProcessRecovery(error);
      if (recovery !== undefined && this.#processOwner.recoverLaunch !== undefined) {
        void this.#processOwner.recoverLaunch(recovery).catch(() => undefined);
      }
      throw error instanceof GraphCoderError
        ? error
        : new GraphCoderError("transport", error instanceof Error ? error.message : String(error));
    }
    if (child.stdin === null || child.stdout === null || child.stderr === null) {
      void this.#processOwner.terminate(child).catch(() => undefined);
      throw new GraphCoderError("transport", "bridge process did not expose piped stdio");
    }
    this.#child = child as ChildProcessWithoutNullStreams;
    this.#child.stdout.on("data", chunk => this.#consumeStdout(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)));
    this.#child.stderr.on("data", chunk => this.#emitDiagnostic({ kind: "stderr", text: Buffer.isBuffer(chunk) ? chunk.toString("utf8") : String(chunk) }));
    this.#child.on("error", error => this.#finish(new GraphCoderError("transport", `bridge process error: ${error.message}`)));
    this.#child.on("exit", () => this.#requestTermination());
    this.#child.on("close", (code, signal) => {
      this.#emitDiagnostic({ kind: "exit", code, signal });
      this.#finish(new GraphCoderError("transport", code === 0 ? "bridge process closed before replying" : `bridge process exited with code ${code ?? "unknown"}`));
      this.#recordExit({ kind: "closed", code, signal });
    });
  }

  /**
   * Waits until the child has emitted its final close event. Calling close()
   * requests termination; this method observes completion of that request so
   * hosts can release their fixture or session resources deterministically.
   */
  waitForExit(timeoutMs?: number): Promise<GraphCoderProcessExit> {
    if (timeoutMs !== undefined && (!Number.isSafeInteger(timeoutMs) || timeoutMs < 0)) {
      return Promise.reject(new GraphCoderError("invalid_input", "exit wait timeout must be a nonnegative safe integer"));
    }
    if (this.#exit !== undefined) {
      const observed = this.#termination === undefined || this.#terminationDone
        ? Promise.resolve(this.#exit)
        : this.#termination.then(() => this.#exit!);
      return timeoutMs === undefined ? observed : waitWithTimeout(observed, timeoutMs);
    }
    return new Promise<GraphCoderProcessExit>((resolve, reject) => {
      let settled = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let waiter: {
        readonly resolve: (exit: GraphCoderProcessExit) => void;
        readonly reject: (error: GraphCoderError) => void;
      };
      waiter = {
        resolve: (exit: GraphCoderProcessExit): void => {
          if (settled) return;
          settled = true;
          if (timer !== undefined) clearTimeout(timer);
          this.#exitWaiters.delete(waiter);
          resolve(exit);
        },
        reject: (error: GraphCoderError): void => {
          if (settled) return;
          settled = true;
          if (timer !== undefined) clearTimeout(timer);
          this.#exitWaiters.delete(waiter);
          reject(error);
        },
      };
      this.#exitWaiters.add(waiter);
      if (timeoutMs !== undefined) {
        timer = setTimeout(() => {
          if (settled) return;
          settled = true;
          this.#exitWaiters.delete(waiter);
          reject(new GraphCoderError("transport", "timed out waiting for bridge process exit"));
        }, timeoutMs);
      }
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
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
    }
    pending.reject(new GraphCoderError("transport", reason));
    return true;
  }

  /** Closes the host process and rejects every request still awaiting a reply. */
  close(reason = "bridge process closed by host"): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#rejectPending(new GraphCoderError("transport", reason));
    this.#requestTermination();
  }

  #consumeStdout(chunk: Buffer): void {
    if (this.#closed) return;
    this.#stdoutBuffer = Buffer.concat([this.#stdoutBuffer, chunk]);
    if (this.#stdoutBuffer.length > this.#maximumLineBytes && this.#stdoutBuffer.indexOf(0x0a) < 0) {
      this.#emitDiagnostic({ kind: "malformed_line", text: "bridge response line exceeds the configured size" });
      this.#finish(new GraphCoderError("transport", "bridge response line exceeds the configured size"));
      return;
    }
    for (;;) {
      const newline = this.#stdoutBuffer.indexOf(0x0a);
      if (newline < 0) return;
      const line = this.#stdoutBuffer.subarray(0, newline);
      this.#stdoutBuffer = this.#stdoutBuffer.subarray(newline + 1);
      if (line.length > this.#maximumLineBytes) {
        this.#emitDiagnostic({ kind: "malformed_line", text: "bridge response line exceeds the configured size" });
        this.#finish(new GraphCoderError("transport", "bridge response line exceeds the configured size"));
        return;
      }
      let text: string;
      try { text = new TextDecoder("utf-8", { fatal: true }).decode(line); }
      catch {
        this.#emitDiagnostic({ kind: "malformed_line", text: "<invalid utf-8>" });
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
      this.#emitDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", "bridge emitted malformed JSON"));
      return;
    }
    if (typeof value !== "object" || value === null || Array.isArray(value) || typeof (value as { request_id?: unknown }).request_id !== "string") {
      this.#emitDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", "bridge emitted an invalid response envelope"));
      return;
    }
    let requestId: string;
    try { requestId = checkedRequestId((value as { request_id: unknown }).request_id, "bridge response id"); }
    catch (error) {
      this.#emitDiagnostic({ kind: "malformed_line", text: line });
      this.#finish(new GraphCoderError("transport", error instanceof Error ? error.message : String(error)));
      return;
    }
    const pending = this.#pending.get(requestId);
    if (pending === undefined) {
      const cancelledRequestId = this.#cancelControls.get(requestId);
      if (cancelledRequestId !== undefined) {
        this.#emitDiagnostic({ kind: "cancelled_response", requestId: cancelledRequestId });
        return;
      }
      if (this.#cancelled.delete(requestId)) {
        this.#emitDiagnostic({ kind: "cancelled_response", requestId });
        return;
      }
      this.#emitDiagnostic({ kind: "unmatched_response", requestId });
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
    this.#requestTermination();
  }

  #requestTermination(): void {
    if (this.#termination !== undefined) return;
    this.#terminationDone = false;
    this.#termination = Promise.resolve().then(() => this.#processOwner.terminate(this.#child)).catch(error => {
      return {
        kind: "unknown",
        pid: this.#child.pid ?? -1,
        reason: error instanceof Error ? error.message : String(error),
      } satisfies OwnedProcessTermination;
    });
    void this.#termination.then(outcome => {
      this.#terminationDone = true;
      // The helper reports an explicit typed outcome through the diagnostic
      // channel so hosts can retain ownership when cleanup is uncertain.
      this.#emitDiagnostic({ kind: "termination", outcome });
      // A proven owner termination is a terminal observation even if the
      // runtime has not delivered Node's final `close` event yet. This keeps
      // waiters tied to the ownership boundary rather than event-loop timing.
      if (outcome.kind === "terminated" && this.#exit === undefined) {
        this.#recordExit({
          kind: "closed",
          code: this.#child.exitCode,
          signal: this.#child.signalCode,
        });
      }
      if (outcome.kind !== "terminated" && this.#exit === undefined) {
        this.#rejectExitWaiters(new GraphCoderError("transport", `bridge cleanup is uncertain: ${outcome.kind}`));
        return;
      }
      this.#resolveExitWaiters();
    });
  }

  #recordExit(exit: GraphCoderProcessExit): void {
    if (this.#exit !== undefined) return;
    this.#exit = exit;
    this.#resolveExitWaiters();
  }

  #resolveExitWaiters(): void {
    if (this.#exit === undefined || (this.#termination !== undefined && !this.#terminationDone)) {
      return;
    }
    for (const waiter of this.#exitWaiters) waiter.resolve(this.#exit);
    this.#exitWaiters.clear();
  }

  #rejectExitWaiters(error: GraphCoderError): void {
    for (const waiter of this.#exitWaiters) waiter.reject(error);
    this.#exitWaiters.clear();
  }

  #emitDiagnostic(event: GraphCoderProcessDiagnostic): void {
    try { this.#onDiagnostic(event); }
    catch { /* Diagnostics must never prevent request rejection or exit cleanup. */ }
  }

  #rejectPending(error: GraphCoderError): void {
    for (const pending of this.#pending.values()) pending.reject(error);
    this.#pending.clear();
  }

  #writeCancelControl(requestId: string, request: GraphCoderWireRequest): void {
    try { checkedRequestId(request.request_id); }
    catch (error) {
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
      return;
    }
    if (request.request_id === requestId || this.#pending.has(request.request_id) || this.#cancelled.has(request.request_id) || this.#cancelControls.has(request.request_id)) {
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: "cancel control request id collides with an active or retired request" });
      return;
    }
    this.#cancelControls.set(request.request_id, requestId);
    let line: string;
    try { line = `${JSON.stringify(request)}\n`; }
    catch (error) {
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
      return;
    }
    if (Buffer.byteLength(line, "utf8") > this.#maximumLineBytes) {
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: "cancel control exceeds the configured line size" });
      return;
    }
    try {
      this.#child.stdin.write(line, error => {
        if (error != null) this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: error.message });
      });
    } catch (error) {
      this.#emitDiagnostic({ kind: "cancel_control_failed", requestId, message: error instanceof Error ? error.message : String(error) });
    }
  }
}

function waitWithTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new GraphCoderError("transport", "timed out waiting for bridge process exit")), timeoutMs);
    promise.then(value => {
      clearTimeout(timer);
      resolve(value);
    }, error => {
      clearTimeout(timer);
      reject(error);
    });
  });
}
