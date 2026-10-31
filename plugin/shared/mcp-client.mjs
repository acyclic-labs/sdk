import { createInterface } from "node:readline";

// One initialized JSON-RPC connection over the host's original process pipes.
// Queue entries retain logical messages. Only the writable frontier is encoded;
// its full UTF-8 frame remains resident until sent, independently of pipe credit.
export class McpClient {
  #input;
  #pending = new Map();
  #head;
  #tail;
  #gate;
  #failure;
  #sequence = 0;
  #blocked = false;
  #scheduled;
  #pumping = false;
  #recovery;
  #onInitialized;
  #onFailure;

  constructor(child, { name, version, recovery, onInitialized, onFailure }) {
    this.#input = child.stdin;
    this.#recovery = recovery;
    this.#onInitialized = onInitialized;
    this.#onFailure = onFailure;
    this.#input.on("drain", () => { this.#blocked = false; this.#pump(); });
    this.#input.on("error", error => this.#stop(error));
    this.#input.on("close", () => this.#stop(new Error("native coordination input closed")));
    child.on("error", error => this.#stop(error));
    child.on("close", () => this.#stop(new Error("native coordination process closed")));
    child.stdout.on("error", error => this.#stop(error));
    createInterface({ input: child.stdout })
      .on("line", line => {
        try { this.#receive(JSON.parse(line)); }
        catch (error) { this.#stop(error); }
      })
      .on("error", error => this.#stop(error))
      .on("close", () => this.#stop(new Error("native coordination output closed")));
    this.#append({ message: { jsonrpc: "2.0", id: 0, method: "initialize", params: {
      protocolVersion: "2025-06-18", capabilities: {}, clientInfo: { name, version },
    } } });
    // This original response is the sole producer that opens the queue. It
    // becomes the initialized notification, ahead of every tools/call frame.
    this.#gate = { gated: true };
    this.#append(this.#gate);
    this.#pump();
  }

  request(name, args, signal) {
    if (this.#failure) return Promise.reject(this.#failure);
    if (signal?.aborted) return Promise.reject(this.#cancelled());
    return new Promise((yes, no) => {
      const entry = { id: ++this.#sequence, yes, no, signal,
        message: { jsonrpc: "2.0", id: this.#sequence, method: "tools/call", params: { name, arguments: args } } };
      entry.abort = () => {
        if (!this.#pending.delete(entry.id)) return;
        signal.removeEventListener("abort", entry.abort);
        // Cancellation settles without waiting for write credit. A partially
        // sent frame keeps custody until its newline; never truncate JSON.
        if (!entry.offset && !entry.sent) this.#unlink(entry);
        else if (!entry.sent) entry.cancelled = true;
        else this.#prioritize(this.#cancellation(entry.id));
        no(this.#cancelled());
        this.#pump();
      };
      this.#pending.set(entry.id, entry);
      this.#append(entry);
      signal?.addEventListener("abort", entry.abort, { once: true });
      this.#pump();
    });
  }

  #cancelled() {
    return new Error(`step=dispatch native coordination: cancelled; recovery=${this.#recovery}`);
  }

  #cancellation(id) {
    return { message: { jsonrpc: "2.0", method: "notifications/cancelled", params: { requestId: id } } };
  }

  #append(entry) {
    entry.previous = this.#tail;
    if (this.#tail) this.#tail.next = entry;
    else this.#head = entry;
    this.#tail = entry;
  }

  #unlink(entry) {
    if (entry.previous) entry.previous.next = entry.next;
    else this.#head = entry.next;
    if (entry.next) entry.next.previous = entry.previous;
    else this.#tail = entry.previous;
    entry.previous = entry.next = undefined;
  }

  #prioritize(entry) {
    // A cancellation may pass queued calls, but never a frame already in flight.
    const before = this.#head?.offset ? this.#head : undefined;
    entry.previous = before;
    entry.next = before ? before.next : this.#head;
    if (entry.next) entry.next.previous = entry;
    else this.#tail = entry;
    if (before) before.next = entry;
    else this.#head = entry;
  }

  #pump() {
    if (this.#failure || this.#blocked || this.#pumping || this.#scheduled) return;
    this.#pumping = true;
    try {
      // Per-turn work follows the existing writable's byte credit, not the
      // number of queued callers. No logical request-size limit is imposed.
      let budget = this.#input.writableHighWaterMark - this.#input.writableLength;
      if (budget <= 0) { this.#blocked = true; return; }
      while (budget > 0 && this.#head && !this.#head.gated && !this.#blocked) {
        const entry = this.#head;
        entry.bytes ??= Buffer.from(JSON.stringify(entry.message) + "\n");
        entry.offset ??= 0;
        const length = Math.min(budget, entry.bytes.length - entry.offset);
        const chunk = entry.bytes.subarray(entry.offset, entry.offset + length);
        this.#blocked = !this.#input.write(chunk);
        if (this.#failure) return;
        entry.offset += length;
        budget -= length;
        if (entry.offset === entry.bytes.length) {
          this.#unlink(entry);
          entry.sent = true;
          entry.bytes = entry.message = undefined;
          if (entry.cancelled) this.#prioritize(this.#cancellation(entry.id));
        }
      }
      if (!this.#blocked && this.#head && !this.#head.gated) {
        // Continue already-ready work fairly; drain owns blocked readiness.
        this.#scheduled = setImmediate(() => { this.#scheduled = undefined; this.#pump(); });
      }
    } catch (error) { this.#stop(error); }
    finally { this.#pumping = false; }
  }

  #receive(message) {
    if (this.#failure) return;
    if (!message || message.jsonrpc !== "2.0") throw new Error("invalid native JSON-RPC response");
    if (message.error && message.id == null) throw new Error(JSON.stringify(message.error));
    if (message.id === 0) {
      if (!this.#gate.gated || message.error || message.result?.protocolVersion !== "2025-06-18")
        throw new Error("invalid native initialize response");
      this.#gate.gated = false;
      this.#gate.message = { jsonrpc: "2.0", method: "notifications/initialized" };
      this.#pump();
      this.#onInitialized?.(message.result);
      return;
    }
    const entry = this.#pending.get(message.id);
    if (!entry) return;
    if (!entry.sent) throw new Error("native response precedes its request");
    this.#pending.delete(message.id);
    entry.signal?.removeEventListener("abort", entry.abort);
    if (message.error) entry.no(new Error(`step=complete native coordination: ${JSON.stringify(message.error)}; recovery=${this.#recovery}`));
    else entry.yes(message.result);
  }

  #stop(error) {
    if (this.#failure) return;
    this.#failure = new Error(`step=serve native coordination: ${error instanceof Error ? error.message : String(error)}; recovery=${this.#recovery}`);
    clearImmediate(this.#scheduled);
    this.#scheduled = undefined;
    for (const entry of this.#pending.values()) {
      entry.signal?.removeEventListener("abort", entry.abort);
      entry.no(this.#failure);
    }
    this.#pending.clear();
    this.#head = this.#tail = this.#gate = undefined;
    this.#input.end();
    this.#onFailure?.(this.#failure);
  }
}
