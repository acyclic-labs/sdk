import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import * as wire from "../generated/proto/stream/v1/session_pb.js";
import { StreamLimit, type Record as WireRecord } from "../generated/proto/stream/v1/stream_pb.js";
import { SessionQueue } from "../src/session-queue.js";
import type { SessionConnect, SessionTransport } from "../src/session.js";
import { StreamError } from "../src/types.js";

type Credit = { bytes: bigint; frames: number };
type Route = { id: bigint; generation: bigint; path: string; next: bigint; credit: Credit };
type Connection = { node: string; output: SessionQueue<wire.SessionFrame>; routes: Map<bigint, Route>; credit: Credit; closed: boolean };
export const frame = (id: bigint, generation: bigint, body: wire.SessionFrame["body"]): wire.SessionFrame => create(wire.SessionFrameSchema, { channelId: id, generation, body });
export const checkpoint = (): Promise<void> => new Promise(resolve => setImmediate(resolve));

/** Protocol peer for client tests, with finite aggregate and channel credit. */
export class SessionNode {
  readonly history = new Map<string, WireRecord[]>();
  readonly connections: Connection[] = [];
  readonly resumes: { node: string; path: string; next: bigint }[] = [];
  readonly received: wire.SessionFrame[] = [];
  readonly changed = new Set<() => void>();
  until(ready: () => boolean): Promise<void> {
    return new Promise(resolve => {
      const check = (): void => { if (ready()) { this.changed.delete(check); resolve(); } };
      this.changed.add(check);
      check();
    });
  }
  returned = 0;
  actorChannels = 0;
  beforeConnect: ((node: string) => Promise<void>) | undefined;
  readonly connect: SessionConnect = async node => {
    const name = node?.nodeId ?? "home";
    await this.beforeConnect?.(name);
    if (this.connections.some(connection => connection.node === name && !connection.closed)) throw new Error("duplicate physical node connection");
    const connection: Connection = { node: name, output: new SessionQueue(1024), routes: new Map(), credit: { bytes: 0n, frames: 0 }, closed: false };
    this.connections.push(connection);
    return {
      incoming: connection.output,
      send: async input => this.receive(connection, fromBinary(wire.SessionFrameSchema, toBinary(wire.SessionFrameSchema, input))),
      close: () => { connection.closed = true; connection.routes.clear(); connection.output.end(); },
    } satisfies SessionTransport;
  };
  append(path: string, count = 1): void {
    let history = this.history.get(path);
    if (history === undefined) { history = []; this.history.set(path, history); }
    for (let index = 0; index < count; index += 1) history.push({ $typeName: "acyclic.stream.v1.Record", sequence: BigInt(history.length), value: new Uint8Array([7]), commitId: new Uint8Array(32).fill(1), committedAtMicros: 0n });
    for (const connection of this.connections) this.pump(connection);
  }
  move(path: string, destination = "owner"): void {
    for (const connection of this.connections) for (const route of connection.routes.values()) if (route.path === path) {
      connection.routes.delete(route.id);
      connection.output.push(frame(route.id, route.generation, { case: "moved", value: create(wire.SessionMovedSchema, { node: { nodeId: destination, endpoint: `https://${destination}`, placementEpoch: 1n }, resume: { next: 999n } }) }));
    }
  }
  disconnect(node = "home"): void {
    for (const connection of this.connections) if (connection.node === node && !connection.closed) {
      connection.closed = true;
      connection.routes.clear();
      connection.output.end(new StreamError("unavailable", "test disconnect"));
    }
  }
  get active(): number { return this.connections.reduce((sum, connection) => sum + connection.routes.size, 0); }
  get outstandingFrames(): number { return this.connections.filter(connection => !connection.closed).reduce((sum, connection) => sum + StreamLimit.MAX_ITEMS - connection.credit.frames, 0); }
  receive(connection: Connection, input: wire.SessionFrame): void {
    if (connection.closed) throw new StreamError("unavailable", "test connection closed");
    this.received.push(input);
    switch (input.body.case) {
      case "hello":
        connection.credit = { bytes: input.body.value.receiveBytes, frames: input.body.value.receiveFrames };
        connection.output.push(frame(0n, 0n, { case: "welcome", value: create(wire.SessionWelcomeSchema, { version: 1, nodeId: connection.node }) }));
        break;
      case "interest":
        for (const removed of input.body.value.remove) connection.routes.delete(removed.channelId);
        for (const subscription of input.body.value.upsert) {
          if (subscription.kind.case === "actorSocket" || subscription.kind.case === "actorCall") this.actorChannels += 1;
          if (subscription.kind.case !== "follow" || subscription.channel === undefined || subscription.resume === undefined) throw new Error("expected a stream follow");
          const route: Route = { id: subscription.channel.channelId, generation: subscription.channel.generation, path: subscription.kind.value.path, next: subscription.resume.next, credit: { bytes: 0n, frames: 0 } };
          connection.routes.set(route.id, route);
          this.resumes.push({ node: connection.node, path: route.path, next: route.next });
          connection.output.push(frame(route.id, route.generation, { case: "opened", value: create(wire.SessionOpenedSchema, { resume: subscription.resume }) }));
        }
        connection.output.push(frame(0n, 0n, { case: "interestApplied", value: create(wire.SessionInterestAppliedSchema, { revision: input.body.value.revision }) }));
        break;
      case "window": {
        const credit = input.channelId === 0n ? connection.credit : connection.routes.get(input.channelId)?.credit;
        if (credit !== undefined) { credit.bytes += input.body.value.bytes; credit.frames += input.body.value.frames; }
        if (input.channelId === 0n) this.returned += input.body.value.frames;
        break;
      }
    }
    this.pump(connection);
    for (const notify of this.changed) notify();
  }
  pump(connection: Connection): void {
    if (connection.closed) return;
    for (const route of connection.routes.values()) {
      const history = this.history.get(route.path) ?? [];
      while (route.next < BigInt(history.length) && route.credit.frames > 0 && connection.credit.frames > 0) {
        const output = frame(route.id, route.generation, { case: "data", value: create(wire.SessionDataSchema, { after: { next: route.next + 1n }, accepted: BigInt(history.length), item: { case: "record", value: history[Number(route.next)]! } }) });
        const encoded = toBinary(wire.SessionFrameSchema, output);
        const bytes = BigInt(encoded.byteLength);
        if (bytes > route.credit.bytes || bytes > connection.credit.bytes) break;
        route.credit.frames -= 1; route.credit.bytes -= bytes;
        connection.credit.frames -= 1; connection.credit.bytes -= bytes;
        route.next += 1n;
        connection.output.push(fromBinary(wire.SessionFrameSchema, encoded));
      }
    }
  }
}
