import { beforeAll, expect, test } from "bun:test";
import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { AppendRequestSchema, AppendResponseSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { WasmStream } from "../generated/wasm/acyclic_harness_wasm.js";
import { AgentExplorer } from "../src/explorer.js";
import { ensureClientWasm } from "../src/wasm-runtime.js";

beforeAll(ensureClientWasm);

test("the same explorer resumes a refused source after acknowledgment", async () => {
  const agent = `harness/agents/${"01".repeat(16)}`;
  const path = `${agent}/conversation`;
  const value = new TextEncoder().encode("original committed stream payload");
  let refused!: () => void, recovered!: () => void;
  const refusal = new Promise<void>(resolve => { refused = resolve; });
  const recovery = new Promise<void>(resolve => { recovered = resolve; });
  const stream = new WasmStream();
  const explorer = await AgentExplorer.open(stream, {
    render(_rows, current) {
      if (current.health().issues.some(issue => issue.stream === path)) refused();
      if (current.history().some(row => row.value !== null)) recovered();
    },
  });
  try {
    explorer.open(agent, 1);
    await refusal;
    expect(explorer.health().issues.find(issue => issue.stream === path))
      .toEqual({ stream: path, kind: "refused", code: "not_found" });
    // Use the public append wire encoder and the actual in-process provider.
    // History retains opaque bytes, so no actor record or provider is mocked.
    const response = fromBinary(AppendResponseSchema, await stream.dispatch("append",
      toBinary(AppendRequestSchema, create(AppendRequestSchema, {
        path, records: [{ value }], idempotencyKey: new TextEncoder().encode("publish-refused-source"),
      }))));
    expect(response.outcome.case).toBe("committed");
    expect(explorer.history()).toEqual([]);
    expect(explorer.health().issues.some(issue => issue.stream === path)).toBe(true);

    explorer.acknowledgeIssue(path);
    await recovery;
    expect(explorer.health().issues.some(issue => issue.stream === path)).toBe(false);
    expect(explorer.history()).toEqual([{ sequence: 0n, value }]);
  } finally {
    explorer.dispose();
    stream.free();
  }
});
