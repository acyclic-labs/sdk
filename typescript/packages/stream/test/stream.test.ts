import { describe, expect, test } from "bun:test";
import {
  StreamClient,
  type AppendOutcome,
  type CommittedEnvelope,
  type ForkReceipt,
  type Record,
  type StreamProvider,
} from "../src/index.js";

const commitId = new Uint8Array(32);

class FixtureProvider implements StreamProvider<string> {
  readonly calls: string[] = [];
  async tail(path: string): Promise<bigint> { this.calls.push(`tail:${path}`); return 1n; }
  async append(path: string, values: readonly string[]): Promise<AppendOutcome> {
    this.calls.push(`append:${path}:${values.join(",")}`);
    return { ok: true, receipt: { start: 1n, end: 2n, tail: 2n, commitId } };
  }
  async fork(source: string, destination: string): Promise<ForkReceipt> {
    this.calls.push(`fork:${source}:${destination}`);
    return { source, destination, forkedAt: 1n, tail: 1n, commitId };
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
    const values: string[] = [];
    for await (const record of source.read(0n, 8)) values.push(record.value);
    expect(values).toEqual(["one"]);
    expect(provider.calls).toEqual([
      "tail:runs/42",
      "append:runs/42:two",
      "fork:runs/42:runs/43",
      "read:runs/42",
    ]);
  });
});
