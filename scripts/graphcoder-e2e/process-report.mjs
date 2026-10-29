import { randomBytes } from "node:crypto";
import { StringDecoder } from "node:string_decoder";

const identity = value => typeof value === "string" && /^[0-9a-f]{32}$/.test(value);
const outcomes = new Set(["report", "empty", "write-failed", "exited", "unsupported"]);
const wait = value => value && typeof value.primitive === "string" && typeof value.key === "string"
  && Number.isFinite(value.since) && Number.isFinite(value.elapsed) && value.elapsed >= 0;

// One selected launch, one request, one terminal acknowledgement. The caller's
// original drain deadline bounds this exchange; it owns no timer or process.
export class ProcessReport {
  generation = randomBytes(16).toString("hex");
  requestId = randomBytes(16).toString("hex");
  #request;
  #ready = false;
  #readyResolve;
  #readyReject;
  ready = new Promise((resolve, reject) => { this.#readyResolve = resolve; this.#readyReject = reject; });
  #terminal;
  #resolve;
  #completion = new Promise(resolve => { this.#resolve = resolve; });
  #channel;
  #ended = false;
  #ownerExited = false;
  #write;
  #sent = false;
  constructor() { this.ready.catch(() => {}); }
  attach(child) {
    const channel = this.#channel = child.stdio[3];
    const decoder = new StringDecoder("utf8");
    let partial = "";
    channel.on("data", chunk => {
      const lines = (partial + decoder.write(chunk)).split("\n");
      partial = lines.pop();
      for (const line of lines) {
        try { this.#receive(JSON.parse(line)); }
        catch { this.#finish("write-failed", "invalid-acknowledgement"); }
      }
    });
    channel.on("error", error => this.#finish("write-failed", error.code ?? "transport-error"));
    const ended = () => {
      this.#ended = true;
      this.#endReady("write-failed", "control-eof-before-ready");
      if (this.#ownerExited && this.#request) this.#finish("exited");
    };
    channel.on("end", ended);
    channel.on("close", ended);
    child.once("exit", () => {
      this.#ownerExited = true;
      // Exit can precede delivery of bytes already written to the control
      // pipe. Drain that same pipe before classifying a missing reply.
      if (this.#ended && this.#request) this.#finish("exited");
    });
    this.#write = () => {
      if (!this.#request || !this.#ready || this.#terminal || this.#sent) return;
      this.#sent = true;
      channel.write(JSON.stringify(this.#request) + "\n", error => {
        if (error) this.#finish("write-failed", error.code ?? "transport-error");
      });
    };
  }
  #receive(message) {
    if (this.#terminal) return;
    if (message.ready === true && message.generation === this.generation && !this.#ready) {
      this.#ready = true;
      this.#readyResolve();
      this.#write?.();
      return;
    }
    const request = this.#request;
    if (!request || !identity(message.request_id) || message.request_id !== request.request_id
      || message.generation !== request.generation || !outcomes.has(message.outcome)
      || !Array.isArray(message.waits) || !message.waits.every(wait)
      || (message.outcome === "report" ? message.waits.length === 0 : message.waits.length !== 0)) {
      this.#finish("write-failed", "foreign-or-invalid-acknowledgement");
      return;
    }
    this.#finish(message.outcome, undefined, message.waits, true);
  }
  #finish(outcome, cause, waits = [], acknowledged = false) {
    if (this.#terminal) return;
    this.#terminal = { outcome, cause, waits, acknowledged };
    this.#endReady(outcome, cause);
    this.#resolve();
  }
  #endReady(outcome, cause) {
    if (this.#ready) return;
    const error = new Error(`report capability ended: ${cause ?? outcome}`);
    error.code = "REPORT_CONTROL_ENDED";
    error.outcome = outcome;
    this.#readyReject(error);
  }
  request(deadline) {
    if (!this.#request) {
      this.#request = { request_id: this.requestId, generation: this.generation, deadline_unix_ms: deadline };
      if (this.#ownerExited && this.#ended) this.#finish("exited");
      else if (this.#ended) this.#finish("write-failed", "control-eof-before-request");
      this.#write?.();
    }
    return this.#completion;
  }
  expire() {
    if (this.#ownerExited) { this.#finish("exited"); return; }
    this.#finish(this.#ended ? "write-failed" : "unacknowledged",
      this.#ended ? "control-eof-before-acknowledgement" : "original-drain-deadline");
  }
  get termination() {
    if (!this.#request) return undefined;
    const terminal = this.#terminal ?? { outcome: "unacknowledged", waits: [] };
    return {
      report_requested: true,
      request_id: this.#request.request_id,
      generation: this.generation,
      report_outcome: terminal.outcome,
      report_cause: terminal.cause,
      report_acknowledged: terminal.acknowledged === true,
      stuck_on: terminal.waits,
      missing_wait_report: terminal.outcome !== "report" && terminal.outcome !== "empty",
    };
  }
}
