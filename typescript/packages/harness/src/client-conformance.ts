/** Identical host-provisioned bytes for the Rust and TS public-client runners. */
import { AgentClient, AgentControls, PreparedAgentCommand, type PreparedAgentInvocation, type WasmStream } from "./client-query.js";
import { clientValueKey as exactJson } from "./client-conformance-value.js";

export interface ClientControlVector {
  readonly case: string;
  readonly command: readonly number[];
  readonly invocation: readonly number[];
  readonly expected: {readonly kind: "reply"; readonly bytes: readonly number[]}
    | {readonly kind: "actor_status"; readonly status: number};
}
export interface ClientControlFixture {
  readonly binding: readonly number[];
  readonly cases: readonly ClientControlVector[];
}

/** Host-created settled records, shared with Rust client::Fixture. Decode with
 * decodeClientJson so stream cursors retain all 64 bits. Each supplied stream
 * must end immediately after its fixture record. */
export interface ClientObservationFixture {
  readonly agent: string;
  readonly conversation_from: bigint;
  readonly status_from: bigint;
  readonly entry: unknown;
  readonly status: unknown;
}

/** The same host-provisioned file consumed by the native tool-output runner. */
export interface ClientToolOutputFixture {
  readonly observation: ClientObservationFixture;
  readonly resource: string;
  readonly subscription: readonly number[];
  readonly record: readonly number[];
}

function observationCursors(fixture: ClientObservationFixture): void {
  if (fixture === null || typeof fixture !== "object" || typeof fixture.agent !== "string") {
    throw new Error("CLIENT_INPUT invalid observation fixture");
  }
  for (const cursor of [fixture.conversation_from, fixture.status_from]) {
    if (typeof cursor !== "bigint" || cursor < 0n || cursor >= (1n << 64n) - 1n) {
      throw new Error("CLIENT_INPUT invalid exact stream cursor");
    }
  }
}

function refused(step: string, code: string, action: () => unknown): void {
  try { action(); }
  catch (error) {
    if (error instanceof Error && error.name === code) return;
    throw error;
  }
  throw new Error(`CLIENT_CONFORMANCE ${step}: expected ${code}`);
}

/** The six observation oracles from Rust client::verify_observation, through
 * the public WASM client. Native verify additionally validates provider tails;
 * this runner requires the same host-provisioned settled-cut fixture. Neither
 * runner establishes turn execution, byte/credit bounds or endpoint bootstrap. */
export async function verifyClientObservation(stream: WasmStream, credential: number,
  fixture: ClientObservationFixture): Promise<{passed: string[]}> {
  observationCursors(fixture);
  const client = new AgentClient(stream, fixture.agent, fixture.conversation_from, fixture.status_from, credential);
  const select = (agent: string, conversation: bigint, status: bigint): void => {
    client.setView([agent], BigUint64Array.of(conversation), BigUint64Array.of(status));
  };
  let conversation = false, status = false;
  try {
    while (!(conversation && status)) {
      const event = await client.next();
      switch (event.kind) {
        case "conversation":
          if (conversation || event.agent !== fixture.agent || event.sequence !== fixture.conversation_from
            || exactJson(event.entry) !== exactJson(fixture.entry)) {
            throw new Error("CLIENT_CONFORMANCE exact_conversation: wrong identity, duplicate, sequence or payload");
          }
          conversation = true;
          break;
        case "status":
          if (status || event.agent !== fixture.agent || exactJson(event.status) !== exactJson(fixture.status)) {
            throw new Error("CLIENT_CONFORMANCE exact_status: wrong identity, duplicate or payload");
          }
          status = true;
          break;
        case "progress": break;
        default: throw new Error("CLIENT_CONFORMANCE not_requested: unrequested delivery");
      }
    }
    const before = client.agentWindow(fixture.agent);
    select(fixture.agent, fixture.conversation_from, fixture.status_from);
    const retained = client.agentWindow(fixture.agent);
    if (retained.nextSequence !== before.nextSequence || retained.revision !== before.revision || retained.recordCount !== 1n) {
      throw new Error("CLIENT_CONFORMANCE retain_selection: unchanged demand reset the fold or cursor");
    }
    // The production interest policy admits eight selected agents. Nine inputs
    // must be refused atomically, including when their identities overlap.
    const overCapacity = Array<string>(9).fill(fixture.agent);
    const composition = client.viewRevision();
    refused("atomic_capacity_refusal", "selected_capacity", () => client.setView(overCapacity,
      new BigUint64Array(9).fill(fixture.conversation_from), new BigUint64Array(9).fill(fixture.status_from)));
    const unchanged = client.agentWindow(fixture.agent);
    if (unchanged.nextSequence !== before.nextSequence || client.viewRevision() !== composition
      || exactJson(client.selectedAgents()) !== exactJson([fixture.agent])) {
      throw new Error("CLIENT_CONFORMANCE atomic_capacity_refusal: refusal changed admitted demand");
    }
    // This is the same fixed test identity used by the native suite, not a
    // frontend agent-address constructor. Rust admits each selected path.
    const parentEdge = fixture.agent.lastIndexOf("/children/");
    const unknown = parentEdge < 0 ? `${fixture.agent}/children/${"7f".repeat(16)}` : fixture.agent.slice(0, parentEdge);
    refused("unknown_authority", "not_requested", () => client.agentStatus(unknown));
    select(unknown, 0n, 0n);
    refused("reselection_cursor", "not_requested", () => client.agentWindow(fixture.agent));
    // Do not poll the replacement: selection is lazy and releases old custody.
    select(fixture.agent, before.nextSequence, fixture.status_from + 1n);
    const reselected = client.agentWindow(fixture.agent);
    if (reselected.nextSequence !== before.nextSequence || reselected.recordCount !== 0n) {
      throw new Error("CLIENT_CONFORMANCE reselection_cursor: evicted history retained or cursor reset");
    }
    return {passed: ["exact_conversation", "exact_status", "retain_selection", "atomic_capacity_refusal",
      "unknown_authority", "reselection_cursor"]};
  } finally { client.close(); client.free(); }
}

/** One public observer folds agent records and a host-filtered fs:/ follow.
 * This TS half checks four shared oracles; native additionally checks admission
 * of a foreign credential slot before any session mutation. */
export async function verifyClientToolOutput(stream: WasmStream, credential: number,
  fixture: ClientToolOutputFixture): Promise<{passed: string[]}> {
  const observed = fixture.observation;
  observationCursors(observed);
  const subscription = bytes(fixture.subscription, PreparedAgentCommand.maximumInvocationBytes());
  const record = bytes(fixture.record, PreparedAgentCommand.maximumReplyBytes());
  const client = new AgentClient(stream, observed.agent, observed.conversation_from, observed.status_from, credential);
  let conversation = false, status = false, output = false;
  try {
    client.openResource(subscription);
    while (!(conversation && status && output)) {
      const event = await client.next();
      switch (event.kind) {
        case "conversation":
          if (conversation || event.agent !== observed.agent || event.sequence !== observed.conversation_from
            || exactJson(event.entry) !== exactJson(observed.entry)) {
            throw new Error("CLIENT_CONFORMANCE tool_conversation: unexpected record");
          }
          conversation = true;
          break;
        case "status":
          if (status || event.agent !== observed.agent || exactJson(event.status) !== exactJson(observed.status)) {
            throw new Error("CLIENT_CONFORMANCE tool_status: unexpected record");
          }
          status = true;
          break;
        case "resource": {
          if (event.resource !== fixture.resource) throw new Error("CLIENT_CONFORMANCE tool_output: unrequested resource");
          const window = client.resourceWindow(fixture.resource);
          if (Number(window.recordCount) === 0) break;
          const actual = client.resourceRecord(fixture.resource, 0);
          if (Number(window.recordCount) !== 1 || !(actual instanceof Uint8Array)
            || actual.length !== record.length || !actual.every((n, i) => n === record[i])) {
            throw new Error("CLIENT_CONFORMANCE tool_output: publication differs from host record");
          }
          output = true;
          break;
        }
        case "progress": break;
        default: throw new Error("CLIENT_CONFORMANCE tool_observe: unknown event");
      }
    }
    if (!client.releaseResource(fixture.resource)) throw new Error("CLIENT_CONFORMANCE tool_release: resource missing");
    let released = false;
    try { client.resourceWindow(fixture.resource); }
    catch (error) { if (error instanceof Error && error.name === "not_requested") released = true; else throw error; }
    if (!released) throw new Error("CLIENT_CONFORMANCE tool_release: view retained");
    return {passed: ["tool_conversation", "tool_status", "tool_output", "tool_release"]};
  } finally { client.close(); client.free(); }
}

const cases = ["root", "prompt", "cancel", "prompt_after_cancel", "spawn", "children_page",
  "approvals_page", "approval_decision", "typed_refusal", "unknown_custody"] as const;
type Outcome = {value: unknown} | {error: {name: string; source?: string; request?: Uint8Array; status?: number}};
function bytes(value: readonly unknown[], maximum: number): Uint8Array {
  if (!Array.isArray(value) || value.length > maximum || value.some(n =>
    typeof n === "bigint" ? n < 0n || n > 255n : typeof n !== "number" || !Number.isInteger(n) || n < 0 || n > 255)) {
    throw new Error("CLIENT_CONFORMANCE control_fixture: invalid bounded bytes");
  }
  return Uint8Array.from(value, n => Number(n));
}
function failed(error: unknown): Outcome {
  if (!(error instanceof Error)) throw error;
  return {error: error as Error & {source?: string; request?: Uint8Array; status?: number}};
}
function same(left: Outcome, right: Outcome): boolean {
  if ("value" in left && "value" in right) {
    return exactJson(left.value) === exactJson(right.value);
  }
  if (!("error" in left) || !("error" in right) || left.error.name !== right.error.name
    || left.error.source !== right.error.source) return false;
  if (left.error.name === "agent_unknown") {
    const a = left.error.request, b = right.error.request;
    return a instanceof Uint8Array && b instanceof Uint8Array && a.length === b.length && a.every((n, i) => n === b[i]);
  }
  return left.error.status === right.error.status;
}

/** Executes vectors through the public WASM client. Transport interruption is
 * always a failure, never an expected result, retry, skip or zero-case pass. */
export async function verifyClientControls(stream: WasmStream, credential: number,
  fixture: ClientControlFixture): Promise<{passed: string[]}> {
  if (!Array.isArray(fixture.cases) || fixture.cases.length !== cases.length) {
    throw new Error("CLIENT_CONFORMANCE control_fixture: complete finite vector set required");
  }
  const binding = bytes(fixture.binding, PreparedAgentCommand.maximumInvocationBytes());
  const controls = new AgentControls(stream, binding, credential);
  const prepared: {name: string; invocation: PreparedAgentInvocation; expected: Outcome}[] = [];
  const seen = new Set<string>();
  let retained = binding.length;
  try {
    for (const vector of fixture.cases) {
      if (vector.case !== cases[prepared.length] || seen.has(vector.case)
        || (vector.case === "prompt_after_cancel" && !seen.has("cancel"))) {
        throw new Error("CLIENT_CONFORMANCE control_fixture: invalid case/order");
      }
      seen.add(vector.case);
      const commandBytes = bytes(vector.command, PreparedAgentCommand.maximumCommandBytes());
      const invocation = bytes(vector.invocation, PreparedAgentCommand.maximumInvocationBytes());
      const encoded = vector.expected.kind === "reply" ? bytes(vector.expected.bytes, PreparedAgentCommand.maximumReplyBytes()) : undefined;
      retained += commandBytes.length + invocation.length + (encoded?.length ?? 0);
      if (retained > 8 * 1024 * 1024) throw new Error("CLIENT_CONFORMANCE control_fixture: byte capacity exceeded");
      const command = new PreparedAgentCommand(commandBytes);
      let held: PreparedAgentInvocation | undefined;
      try {
        held = controls.prepare(command);
        const exact = held.bytes();
        if (exact.length !== invocation.length || !exact.every((n, i) => n === invocation[i])) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: vector differs from public host binding");
        }
        let expected: Outcome;
        if (encoded) {
          try { expected = {value: command.reply(encoded)}; } catch (error) {
            expected = failed(error);
            if ("error" in expected && (expected.error.source === "session"
              || ["invalid_client_command", "client_command_capacity"].includes(expected.error.name))) throw error;
          }
        } else {
          if (vector.expected.kind !== "actor_status" || vector.case !== "typed_refusal"
            || !Number.isInteger(vector.expected.status) || vector.expected.status < 400 || vector.expected.status > 599) {
            throw new Error("CLIENT_CONFORMANCE control_fixture: expected refusal status required");
          }
          expected = {error: {name: "actor_invocation_refused", status: vector.expected.status}};
        }
        if (vector.case === "unknown_custody" && (!("error" in expected) || expected.error.name !== "agent_unknown")) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: unknown case needs owned unknown outcome");
        }
        if (vector.case === "typed_refusal" && (!("error" in expected) || expected.error.name === "agent_unknown")) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: definitive refusal required");
        }
        if (!["unknown_custody", "typed_refusal"].includes(vector.case) && !("value" in expected)) {
          throw new Error("CLIENT_CONFORMANCE control_fixture: successful result required");
        }
        if ("value" in expected) {
          const kinds: Record<string, readonly [string, string]> = {
            root: ["root", "root"], prompt: ["prompt", "delivered"], prompt_after_cancel: ["prompt", "delivered"],
            cancel: ["cancel", "delivered"], spawn: ["spawn", "spawned"], children_page: ["children", "children"],
            approvals_page: ["approvals", "approvals"], approval_decision: ["decide", "delivered"],
          };
          const kind = kinds[vector.case];
          const value = expected.value as {kind: string; delivery?: {kind: string}};
          if (!kind || command.value().kind !== kind[0] || value.kind !== kind[1]
            || (kind[1] === "delivered" && value.delivery?.kind !== "committed")) {
            throw new Error("CLIENT_CONFORMANCE control_fixture: case differs from its command/result oracle");
          }
        }
        prepared.push({name: vector.case, invocation: held, expected});
        held = undefined;
      } finally { held?.free(); command.free(); }
    }
    const passed: string[] = [];
    for (const vector of prepared) {
      let actual: Outcome;
      try { actual = {value: await vector.invocation.execute()}; }
      catch (error) { actual = failed(error); }
      if ("error" in actual && actual.error.source === "session") {
        throw new Error(`CLIENT_CONFORMANCE ${vector.name}: session ${actual.error.name}; not retried`);
      }
      if (!same(vector.expected, actual)) throw new Error(`CLIENT_CONFORMANCE ${vector.name}: public result differs; not retried`);
      passed.push(vector.name);
    }
    return {passed};
  } finally {
    for (const vector of prepared) vector.invocation.free();
    controls.free();
  }
}
