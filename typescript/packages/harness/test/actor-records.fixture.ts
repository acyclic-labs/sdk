import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { AppendRecordSchema, AppendRequestSchema, AppendResponseSchema } from "../../stream/generated/proto/stream/v1/stream_pb.js";
import { encodeAgentAppend, type AgentClient, type AgentEvent, type WasmStream } from "../src/client.js";

const encode = new TextEncoder();
let appendId = 0n;
export const rootId = "01".repeat(16);
export const root = `harness/agents/${rootId}`;
export const statusPath = `harness/status/${rootId}`;
const file = {
  volume: { provider: { namespace: "fixture", family: "filesystem", version: "1" },
    id: "fixture", class: "project", owner: { kind: "project", id: "fixture" } },
  path: "output.json", version: "v1", display_name: "output",
  descriptor: { sha256: Array(32).fill(0), byte_length: 0, media_type: "application/json" },
};

// Adapter fixtures only: these are canonical settled records, not an AgentHost
// submission, authorization, tool execution or durable outbox qualification.
export function conversation(id: string, inbox: bigint): string {
  // The production entry builder saturates its exclusive frontier at u64::MAX.
  const through = inbox === 18446744073709551615n ? inbox : inbox + 1n;
  return `{"observed_through":${through},"inbox":${inbox},"input":{"message":{"from":{"user":{"principal":"fixture"}},"id":${JSON.stringify(id)},"body":${JSON.stringify(file)}}},"output":${JSON.stringify(file)}}`;
}

// The production Rust writer supplies current record bytes and indexed status headers.
export async function appendRecord(stream: WasmStream, path: string, value: string): Promise<bigint> {
  const kind = path.endsWith("/conversation") ? "conversation" : path.startsWith("harness/status/") ? "status" : undefined;
  if (kind === undefined) throw new Error("actor fixture requires a conversation or status stream");
  const record = fromBinary(AppendRecordSchema, encodeAgentAppend(kind, encode.encode(value)));
  const response = fromBinary(AppendResponseSchema, await stream.dispatch("append",
    toBinary(AppendRequestSchema, create(AppendRequestSchema, {
      path, idempotencyKey: encode.encode(`actor-record-${appendId++}`),
      records: [record],
    }))));
  if (response.outcome.case !== "committed") throw new Error(`actor fixture append: ${response.outcome.case}`);
  return response.outcome.value.tail;
}

export function record(client: AgentClient, agent: string, index: number) {
  const event = client.agentRecord(agent, index);
  if (event?.kind !== "conversation") throw new Error("selected conversation record is missing");
  return event;
}

export function message(event: Extract<AgentEvent, { kind: "conversation" }>) {
  const input = event.entry.input;
  if (!("message" in input)) throw new Error("conversation input is not a message");
  return input.message;
}

// Consume real session transitions, including admission/watermark progress.
// There is no timer, polling read, synthetic status or second receive queue.
export async function receiveUntil(client: AgentClient, condition: () => boolean): Promise<void> {
  while (!condition()) await client.next();
}
