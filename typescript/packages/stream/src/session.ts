import { create, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { StreamLimit } from "../generated/proto/stream/v1/stream_pb.js";
import { checkedRecord } from "./record.js";
import { StreamError, type EncodedRecord, type FollowOptions, type ChildrenRequest, type StreamChild } from "./types.js";
import { checkedChild } from "./child.js";
import { SessionQueue } from "./session-queue.js";

export interface SessionTransport {
  readonly incoming: AsyncIterable<wire.SessionFrame>;
  /** Abort withdraws a write that has not yet entered the transport queue. */
  send(frame: wire.SessionFrame, signal?: AbortSignal): Promise<void>;
  close(): void;
}
export type SessionConnect = (node: wire.SessionNode | undefined, signal: AbortSignal) => Promise<SessionTransport>;
type Body = wire.SessionFrame["body"];
type Credit = { bytes: bigint; frames: number };
type StreamKind = Extract<wire.SessionSubscription["kind"], { case: "follow" | "children" }>;
type Delivery = { value: EncodedRecord | StreamChild; sequence: bigint; opaque: Uint8Array; generation: bigint; link: Link; release(): void };
type Route = {
  id: bigint; generation: bigint; kind: StreamKind; applied: bigint; received: bigint;
  appliedOpaque: Uint8Array; receivedOpaque: Uint8Array;
  placement: bigint; link: Link; queue: SessionQueue<Delivery>; credit: Credit;
  opened: boolean; moving: boolean; controller: AbortController; resolve(): void; release(): void;
};
type Connection = { promise: Promise<Link>; link?: Link };
type Link = {
  node: string; target: wire.SessionNode | undefined; transport: SessionTransport;
  controller: AbortController; revision: bigint; appliedRevision: bigint; credit: Credit;
  ready: Promise<void>; resolve(): void; reject(error: unknown): void;
};
const FRAMES = StreamLimit.MAX_ITEMS;
const BYTES = BigInt(StreamLimit.MAX_COMMAND_BYTES);
const RECORD_BYTES = BigInt(StreamLimit.MAX_RECORD_BYTES + 128);
const MAX_U64 = (1n << 64n) - 1n;
const emptyCredit = (): Credit => ({ bytes: 0n, frames: 0 });
const frame = (channelId: bigint, generation: bigint, body: Body): wire.SessionFrame => create(wire.SessionFrameSchema, { channelId, generation, body });
const failure = (): StreamError => new StreamError("invalid_response", "invalid Streams session frame");
function add(balance: Credit, credit: Credit): void {
  if (credit.bytes < 0n || credit.frames < 0 || balance.bytes + credit.bytes > MAX_U64 || balance.frames + credit.frames > 0xffff_ffff) throw failure();
  balance.bytes += credit.bytes;
  balance.frames += credit.frames;
}
function consume(balance: Credit, bytes: bigint): void {
  if (balance.bytes < bytes || balance.frames === 0) throw failure();
  balance.bytes -= bytes;
  balance.frames -= 1;
}
function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  return left.length === right.length && left.every((byte, index) => byte === right[index]);
}

/** One canonical Streams session per client/node, shared by every logical follow. */
export class StreamSessions {
  readonly #connect: SessionConnect;
  readonly #acknowledge: boolean;
  readonly #links = new Map<string, Connection>();
  readonly #routes = new Map<bigint, Route>();
  readonly #identity = crypto.getRandomValues(new Uint8Array(16));
  #nextId = 1n;
  #active = 0;
  constructor(connect: SessionConnect, acknowledge = true) { this.#connect = connect; this.#acknowledge = acknowledge; }

  follow(path: string, options: FollowOptions): AsyncGenerator<EncodedRecord> {
    return this.#stream({ case: "follow", value: create(wire.SessionFollowSchema, { path, delivery: wire.SessionDelivery.ALL }) }, options) as AsyncGenerator<EncodedRecord>;
  }
  children(request: ChildrenRequest): AsyncGenerator<StreamChild> {
    const { parent, signal, limit } = request;
    return this.#stream({ case: "children", value: { $typeName: "acyclic.stream.v1.ChildrenRequest", limit,
      ...(parent === undefined ? {} : { parent }) } },
    { from: 0n, ...(signal === undefined ? {} : { signal }) }) as AsyncGenerator<StreamChild>;
  }
  async *#stream(kind: StreamKind, options: FollowOptions): AsyncGenerator<EncodedRecord | StreamChild> {
    if (options.signal?.aborted) return;
    if (this.#active >= 16_384 || this.#nextId > MAX_U64 - 2n) throw new StreamError("capacity", "session channel capacity exceeded");
    const id = this.#nextId;
    this.#nextId += 2n;
    this.#active += 1;
    let reserved = true;
    const release = (): void => { if (reserved) { reserved = false; this.#active -= 1; } };
    const queue = new SessionQueue<Delivery>(2);
    let route: Route | undefined;
    const abort = (): void => { release(); queue.end(); queue.discard(delivery => delivery.release()); if (route !== undefined) this.#remove(route); };
    options.signal?.addEventListener("abort", abort, { once: true });
    try {
      const link = await untilAborted(this.#link(undefined), options.signal);
      if (link === undefined || options.signal?.aborted) return;
      if (link.controller.signal.aborted) throw new StreamError("unavailable", "Streams session ended before subscription");
      let resolve!: () => void;
      const opened = new Promise<void>(yes => { resolve = yes; });
      route = { id, generation: 1n, kind, applied: options.from, received: options.from, placement: 0n,
        appliedOpaque: new Uint8Array(), receivedOpaque: new Uint8Array(),
        link, queue, credit: emptyCredit(), opened: false, moving: false, controller: new AbortController(), resolve, release };
      this.#routes.set(id, route);
      try { await this.#subscribe(route); }
      catch (error) { if (!route.controller.signal.aborted) this.#lost(link, error); }
      await opened;
      for await (const delivery of queue) {
        delivery.release();
        if (options.signal?.aborted) return;
        if (delivery.generation !== route.generation || delivery.link !== route.link) { await this.#grant(route); continue; }
        if (delivery.sequence !== route.applied) throw failure();
        // No await between application-cursor advancement and the yield boundary.
        route.applied += 1n;
        route.appliedOpaque = delivery.opaque;
        yield delivery.value;
        if (this.#acknowledge && this.#routes.get(id) === route) {
          await this.#write(route.link, frame(id, route.generation, { case: "ack", value: create(wire.SessionAckSchema, { applied: { next: route.applied, opaque: route.appliedOpaque } }) }), route.controller.signal);
        }
        await this.#grant(route);
      }
    } finally {
      release();
      options.signal?.removeEventListener("abort", abort);
      if (route !== undefined) this.#remove(route);
      queue.end();
      queue.discard(delivery => delivery.release());
    }
  }

  #link(target: wire.SessionNode | undefined): Promise<Link> {
    const key = target?.nodeId ?? "";
    let connection = this.#links.get(key);
    if (connection === undefined) {
      const opened = { promise: this.#open(target) };
      connection = opened;
      this.#links.set(key, opened);
      void opened.promise.catch(() => { if (this.#links.get(key) === opened) this.#links.delete(key); });
    }
    return connection.promise;
  }
  async #open(target: wire.SessionNode | undefined): Promise<Link> {
    for (let attempt = 0; ; attempt += 1) {
      try { return await this.#handshake(target); }
      catch (error) {
        if (attempt === 2 || !(error instanceof StreamError) || (error.code !== "unavailable" && error.code !== "transport")) throw error;
      }
    }
  }
  async #handshake(target: wire.SessionNode | undefined): Promise<Link> {
    const controller = new AbortController();
    let transport: SessionTransport | undefined;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const expired = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new StreamError("unavailable", "Streams Welcome deadline expired")), 1_000);
    });
    const attempt = async (): Promise<Link> => {
      const connected = await this.#connect(target, controller.signal);
      if (controller.signal.aborted) { connected.close(); throw new StreamError("unavailable", "session cancelled"); }
      transport = connected;
      let resolve!: () => void;
      let reject!: (error: unknown) => void;
      const ready = new Promise<void>((yes, no) => { resolve = yes; reject = no; });
      const link: Link = { node: "", target, transport, controller, revision: 0n, appliedRevision: 0n,
        credit: { bytes: BYTES, frames: FRAMES }, ready, resolve, reject };
      void this.#read(link);
      await Promise.all([ready, transport.send(frame(0n, 0n, { case: "hello", value: create(wire.SessionHelloSchema, {
        version: 1, clientId: this.#identity, destinationNode: target?.nodeId ?? "", receiveBytes: BYTES, receiveFrames: FRAMES,
      }) }))]);
      return link;
    };
    try { return await Promise.race([attempt(), expired]); }
    catch (error) { controller.abort(); transport?.close(); throw error; }
    finally { clearTimeout(timer); }
  }
  async #read(link: Link): Promise<void> {
    try {
      for await (const input of link.transport.incoming) await this.#receive(link, input);
      throw new StreamError("unavailable", "Streams session ended");
    } catch (error) { this.#lost(link, error); }
  }
  #lost(link: Link, error: unknown): void {
    if (link.controller.signal.aborted) return;
    link.reject(error);
    link.controller.abort();
    link.transport.close();
    if (link.node === "") return; // The shared initializer owns handshake retries.
    // Read and write failures share one idempotent fencing/recovery transition.
    const affected = [...this.#routes.values()].filter(route => route.link === link && !route.moving);
    for (const route of affected) { route.generation += 1n; route.opened = false; }
    for (const key of [link.target?.nodeId ?? "", link.node]) {
      if (this.#links.get(key)?.link === link) this.#links.delete(key);
    }
    if (error instanceof StreamError && error.code !== "unavailable" && error.code !== "transport") {
      for (const route of affected) this.#fail(route, error);
    } else if (affected.length) void this.#recover(link, affected);
  }
  async #write(link: Link, input: wire.SessionFrame, signal?: AbortSignal): Promise<void> {
    if (link.controller.signal.aborted || signal?.aborted) return;
    try { await untilAborted(link.transport.send(input, signal), signal, link.controller.signal); }
    catch (error) { if (!signal?.aborted) this.#lost(link, error); }
  }
  async #recover(link: Link, routes: Route[]): Promise<void> {
    let next: Link;
    try {
      // A failed transport is retried once; idle live follows never use a timer.
      next = await this.#link(link.target);
    } catch (error) { for (const route of routes) if (route.link === link && !route.moving) this.#fail(route, error); return; }
    for (const route of routes) if (this.#routes.get(route.id) === route && route.link === link && !route.moving) {
      const generation = route.generation;
      route.link = next;
      try { await this.#subscribe(route); }
      catch (error) { if (route.link === next && route.generation === generation) this.#fail(route, error); }
    }
  }
  async #subscribe(route: Route): Promise<void> {
    if (this.#routes.get(route.id) !== route || route.controller.signal.aborted) return;
    const link = route.link;
    route.moving = false;
    route.received = route.applied;
    route.receivedOpaque = route.appliedOpaque;
    route.credit = emptyCredit();
    route.opened = false;
    await untilAborted(link.transport.send(frame(0n, 0n, { case: "interest", value: create(wire.SessionInterestSchema, {
      revision: ++route.link.revision, upsert: [{ channel: { channelId: route.id, generation: route.generation },
        resume: { next: route.applied, opaque: route.appliedOpaque }, kind: route.kind }],
    }) }), route.controller.signal), route.controller.signal, link.controller.signal);
  }
  async #receive(link: Link, input: wire.SessionFrame): Promise<void> {
    const bytes = BigInt(toBinary(wire.SessionFrameSchema, input).byteLength);
    if (bytes > BYTES) throw failure();
    const body = input.body;
    if (input.channelId === 0n) {
      if (input.generation !== 0n) throw failure();
      if (body.case === "welcome" && link.node === "") {
        if (body.value.version !== 1 || body.value.nodeId === "" || (link.target !== undefined && body.value.nodeId !== link.target.nodeId)) throw failure();
        link.node = body.value.nodeId;
        const opening = this.#links.get(link.target?.nodeId ?? "");
        if (opening !== undefined) opening.link = link;
        const installed = this.#links.get(link.node)?.link;
        if (installed !== undefined && installed !== link) throw failure();
        this.#links.set(link.node, { promise: Promise.resolve(link), link });
        link.resolve();
      } else if (body.case === "interestApplied") {
        if (body.value.revision < link.appliedRevision || body.value.revision > link.revision) throw failure();
        link.appliedRevision = body.value.revision;
      } else if (body.case !== "window") throw failure();
      return;
    }
    if (body.case === "data") consume(link.credit, bytes);
    const route = this.#routes.get(input.channelId);
    if (route === undefined || route.link !== link || route.generation !== input.generation) {
      if (body.case === "data") this.#release(link, bytes);
      return;
    }
    switch (body.case) {
      case "opened":
        if (route.opened || body.value.resume?.next !== route.received || !sameBytes(body.value.resume.opaque, route.receivedOpaque)) throw failure();
        route.opened = true;
        route.resolve();
        await this.#grant(route);
        break;
      case "data": {
        consume(route.credit, bytes);
        const data = body.value;
        if (!route.opened || data.after?.next !== route.received + 1n || data.durable > data.accepted || data.accepted < data.after.next) throw failure();
        let value: EncodedRecord | StreamChild;
        if (route.kind.case === "follow" && data.item.case === "record" && data.after.opaque.length === 0) {
          value = checkedRecord(data.item.value, route.received);
        } else if (route.kind.case === "children" && data.item.case === "child") {
          value = checkedChild(data.item.value, route.kind.value.parent);
        } else { throw failure(); }
        const sequence = route.received;
        const opaque = data.after.opaque.slice();
        route.received += 1n;
        route.receivedOpaque = opaque;
        let released = false;
        const release = (): void => {
          if (released) return;
          released = true;
          this.#release(link, bytes);
        };
        try { route.queue.push({ value, sequence, opaque, release, generation: route.generation, link }); }
        catch (error) { release(); throw error; }
        break;
      }
      case "closed": this.#fail(route, closedError(body.value.code, body.value.message)); break;
      case "moved": {
        const target = body.value.node;
        if (target === undefined || !target.nodeId || target.placementEpoch <= route.placement) throw failure();
        route.placement = target.placementEpoch;
        route.moving = true;
        route.generation += 1n;
        route.opened = false;
        const generation = route.generation;
        void this.#link(target).then(async next => {
          if (this.#routes.get(route.id) !== route || route.generation !== generation) return;
          route.link = next;
          await this.#subscribe(route);
        }).catch(error => this.#fail(route, error));
        break;
      }
      case "watermark": if (body.value.durable > body.value.accepted) throw failure(); break;
      default: throw failure();
    }
  }
  async #grant(route: Route): Promise<void> {
    if (!route.opened || this.#routes.get(route.id) !== route) return;
    const frameBytes = route.kind.case === "children" ? BigInt(2 * StreamLimit.MAX_PATH_BYTES + 128) : RECORD_BYTES;
    const grant = { frames: Math.max(0, route.queue.available - route.credit.frames),
      bytes: BigInt(route.queue.available) * frameBytes > route.credit.bytes ? BigInt(route.queue.available) * frameBytes - route.credit.bytes : 0n };
    if (grant.frames === 0 && grant.bytes === 0n) return;
    add(route.credit, grant);
    await this.#write(route.link, frame(route.id, route.generation, { case: "window", value: create(wire.SessionWindowSchema, grant) }), route.controller.signal);
  }
  #release(link: Link, bytes: bigint): void {
    if (link.controller.signal.aborted) return;
    const grant = { bytes, frames: 1 };
    add(link.credit, grant);
    void this.#write(link, frame(0n, 0n, { case: "window", value: create(wire.SessionWindowSchema, grant) }));
  }
  #fail(route: Route, error: unknown): void {
    if (this.#routes.get(route.id) !== route) return;
    this.#routes.delete(route.id);
    route.controller.abort();
    route.release();
    route.resolve();
    route.queue.end(error);
  }
  #remove(route: Route): void {
    if (this.#routes.get(route.id) !== route) return;
    this.#routes.delete(route.id);
    route.controller.abort();
    route.release();
    route.resolve();
    void this.#write(route.link, frame(0n, 0n, { case: "interest", value: create(wire.SessionInterestSchema, {
      revision: ++route.link.revision, remove: [{ channelId: route.id, generation: route.generation }],
    }) }));
  }
}

async function untilAborted<T>(pending: Promise<T>, signal: AbortSignal | undefined, other?: AbortSignal): Promise<T | undefined> {
  const signals = [signal, other].filter((value): value is AbortSignal => value !== undefined);
  if (signals.length === 0) return pending;
  if (signals.some(value => value.aborted)) { void pending.catch(() => {}); return undefined; }
  let abort!: () => void;
  const cancelled = new Promise<undefined>(resolve => {
    abort = () => resolve(undefined);
    for (const value of signals) value.addEventListener("abort", abort, { once: true });
  });
  try { return await Promise.race([pending, cancelled]); }
  finally { for (const value of signals) value.removeEventListener("abort", abort); }
}

function closedError(code: string, message: string): StreamError {
  switch (code) {
    case "not_found": return new StreamError("stream_not_found", message);
    case "already_exists": return new StreamError("destination_exists", message);
    case "capacity": return new StreamError("capacity_exhausted", message);
    case "invalid_path": case "invalid_argument": case "limit_exceeded":
    case "prefix_not_retained": case "out_of_range": case "idempotency_mismatch":
    case "access_denied": case "unavailable": case "hierarchy_changed":
    case "deadline_elapsed": case "unsupported": return new StreamError(code, message);
    default: return new StreamError("unavailable", message);
  }
}
