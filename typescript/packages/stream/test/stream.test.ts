import { describe, expect, test } from "bun:test";
import {
  StreamClient,
  type AppendOutcome,
  type CommittedEnvelope,
  type DeleteReceipt,
  type ForkReceipt,
  type IdempotencyObservation,
  type Record,
  type StreamProvider,
  type TrimReceipt,
} from "../src/index.js";

const commitId = new Uint8Array(32);

class FixtureProvider implements StreamProvider<string> {
  readonly calls: string[] = [];
  async inspectIdempotency(idempotencyKey: Uint8Array): Promise<IdempotencyObservation<string>> {
    this.calls.push(`inspect:${idempotencyKey[0]}`);
    return {
      idempotencyKey,
      requestDigest: new Uint8Array(32),
      outcome: { type: "trim", receipt: { path: "runs/42", trimPoint: 1n, commitId } },
    };
  }
  async tail(path: string): Promise<bigint> { this.calls.push(`tail:${path}`); return 1n; }
  async append(path: string, values: readonly string[]): Promise<AppendOutcome> {
    this.calls.push(`append:${path}:${values.join(",")}`);
    return { ok: true, receipt: { start: 1n, end: 2n, tail: 2n, commitId } };
  }
  async fork(source: string, destination: string): Promise<ForkReceipt> {
    this.calls.push(`fork:${source}:${destination}`);
    return { source, destination, forkedAt: 1n, tail: 1n, commitId };
  }
  async trim(path: string, before: bigint, idempotencyKey?: Uint8Array): Promise<TrimReceipt> {
    this.calls.push(`trim:${path}:${before}:${idempotencyKey?.[0]}`);
    return { path, trimPoint: before, commitId };
  }
  async delete(path: string, idempotencyKey?: Uint8Array): Promise<DeleteReceipt> {
    this.calls.push(`delete:${path}:${idempotencyKey?.[0]}`);
    return { path, commitId };
  }
  async *read(path: string): AsyncIterable<Record<string>> {
    this.calls.push(`read:${path}`);
    yield { sequence: 0n, value: "one", commitId };
  }
  async *follow(path: string): AsyncIterable<Record<string>> {
    this.calls.push(`follow:${path}`);
    yield { sequence: 1n, value: "live", commitId };
  }
  async *children(parent: string | undefined): AsyncIterable<{ readonly path: string }> {
    yield { path: `${parent}/child` };
  }
  async commit(): Promise<{ readonly ok: false; readonly code: "conflict"; readonly conflicts: readonly [] }> {
    return { ok: false, code: "conflict", conflicts: [] };
  }
  async readCommit(): Promise<CommittedEnvelope<string>> { return { commitId, mutations: [] }; }
}

describe("StreamClient", () => {
  test("keeps provider mechanics behind one permanent-path handle", async () => {
    const provider = new FixtureProvider();
    const source = new StreamClient(provider).stream("runs/42");
    expect(await source.tail()).toBe(1n);
    expect((await source.append("two")).ok).toBe(true);
    const fork = await source.fork("runs/43", { atTail: 1n });
    expect(fork.stream.path).toBe("runs/43");
    expect((await source.trim(1n, new Uint8Array([7]))).trimPoint).toBe(1n);
    expect((await new StreamClient(provider).inspectIdempotency(new Uint8Array([7])))?.outcome.type).toBe("trim");
    expect((await source.delete(new Uint8Array([8]))).path).toBe("runs/42");
    const values: string[] = [];
    for await (const record of source.read(0n, 8)) values.push(record.value);
    expect(values).toEqual(["one"]);
    expect(provider.calls).toEqual([
      "tail:runs/42",
      "append:runs/42:two",
      "fork:runs/42:runs/43",
      "trim:runs/42:1:7",
      "inspect:7",
      "delete:runs/42:8",
      "read:runs/42",
    ]);
  });
});
