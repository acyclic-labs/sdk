/** A public reducer-backed message projection; no queue/admission implementation. */
import { Harness, DEFAULT_LIMITS, type ConversationMessage } from "@acyclic-labs/harness";
import { ClientViews, type ClientDomain, type ClientFact, type OperationOutcome } from "@acyclic-labs/harness/client";

function immutable<Value>(value: Value): Value {
  if (value !== null && typeof value === "object") {
    for (const child of Object.values(value)) immutable(child);
    Object.freeze(value);
  }
  return value;
}

/** Execute through the existing public Rust reducer with its original operation. */
export async function messageViewExample(): Promise<{
  provisional: string | null; unknownOutcome: OperationOutcome | null;
  finalProvenance: string | null; finalPrediction: string | undefined; canonicalMessages: bigint;
}> {
  const authority = { kind: "conversation", id: "client-message-example" } as const;
  const reducer = await Harness.create({ authority, issuerId: "example", issuerKey: new Uint8Array(32).fill(7) });
  const scope = reducer.issueScope("host", ["conversation:bind", "conversation:append"]);
  const agent = reducer.identity("agent", "01010101-0101-0101-0101-010101010101");
  const operation = reducer.identity("operation", "02020202-0202-0202-0202-020202020202");
  reducer.apply({ authority, operation_id: reducer.identity("operation", "03030303-0303-0303-0303-030303030303"),
    idempotency_key: "bind", expected_revision: 0n, scope, causal_parent: null, action: { kind: "bind_conversation", agent } });
  const content = reducer.validateFileRef({ volume: { provider: { namespace: "example", family: "filesystem", version: "1" },
    id: "project", class: "project", owner: { kind: "project", id: "project" } }, path: "messages/hello.txt", version: "generation-1",
    descriptor: reducer.fileDescriptor(new TextEncoder().encode("hello"), "text/plain"), display_name: "hello.txt" });
  const predicted = immutable(reducer.validateConversationMessage({
    id: reducer.conversationMessageId("04040404-0404-0404-0404-040404040404"), sequence: 1n, kind: "user", content,
    attachments: { kind: "inline", items: [] }, reply_to: null, tool_call_id: null, extensions: {},
  }, DEFAULT_LIMITS));
  type Evidence = Readonly<{ fact: ClientFact<ConversationMessage | null>; operation: readonly [string, OperationOutcome] | null }>;
  const trusted = new WeakSet<Evidence>();
  const pin = JSON.stringify([authority.kind, authority.id, "local-host-generation-1"]);
  const metadata = (value: unknown): number => {
    const bytes = reducer.canonicalJsonBytes(value).byteLength;
    if (bytes > 4096) throw new RangeError("example ref metadata bound");
    return bytes * 8 + 1024; // Deep strings/record/index clones, not body bytes.
  };
  const read = (outcome: Evidence["operation"]): Evidence => {
    const page = reducer.conversationPage(0n, 1);
    const canonical = immutable(page.messages[0] ?? null);
    const digest = [...reducer.canonicalJsonDigest(canonical)].map(byte => byte.toString(16).padStart(2, "0")).join("");
    const fact = Object.freeze({ key: predicted.id, basis: `${pin}:${reducer.head()[1]}:${digest}`, value: canonical, bytes: metadata(canonical) });
    const evidence = Object.freeze({ fact, operation: outcome });
    trusted.add(evidence);
    return evidence;
  };
  const domain: ClientDomain<ConversationMessage | null, "absent", Evidence> = {
    identity: "1",
    validate(fact, message, assumption, work) {
      if (work < 2 || assumption !== "absent" || fact.value !== null || message?.kind !== "user") throw new Error("unsupported hypothesis");
      reducer.validateConversationMessage(message, DEFAULT_LIMITS);
      return { bytes: metadata([fact.basis, message, assumption]), work: 2 };
    },
    observe(evidence, current, work) {
      if (work < 2 || !trusted.has(evidence)) throw new Error("untrusted host evidence");
      if (current) {
        const revision = (basis: string): bigint => {
          if (!basis.startsWith(`${pin}:`)) throw new Error("authority/generation conflict");
          return BigInt(basis.slice(pin.length + 1).split(":")[0]!);
        };
        const oldRevision = revision(current.basis);
        const newRevision = revision(evidence.fact.basis);
        if (newRevision < oldRevision || (newRevision === oldRevision && current.basis !== evidence.fact.basis)) throw new Error("basis conflict");
      }
      return { ...evidence, work: 2 };
    },
    corresponds(left, right, work) {
      if (work < 1) throw new Error("work bound");
      const predicted = reducer.canonicalJsonBytes(left);
      const canonical = reducer.canonicalJsonBytes(right);
      return { matches: predicted.byteLength === canonical.byteLength && predicted.every((byte, index) => byte === canonical[index]), work: 1 };
    },
  };
  const client = new ClientViews(domain, "11", 0n, { records: 1, branches: 1, edges: 1, bytes: 100_000, work: 32, retention: 10n, visible: 1 });
  try {
    const initial = read(null);
    client.observe(predicted.id, initial);
    const branch = client.begin({ key: predicted.id, basis: initial.fact.basis, operation, predicted, assumption: "absent", dependencies: [], expires: 5n });
    const selected = client.select(predicted.id, [branch]);
    const provisional = selected.getSnapshot().branch;
    const unknownOutcome = selected.getSnapshot().outcome;
    if (reducer.conversationPage(0n, 1).total_messages !== 0n) throw new Error("prediction mutated canonical messages");
    const command = { authority, operation_id: operation, idempotency_key: "message", expected_revision: 1n,
      scope, causal_parent: null, action: { kind: "append_conversation_message", message: predicted } };
    const applied = reducer.apply(command);
    if (applied.event.operationId !== operation) throw new Error("operation correlation changed");
    client.observe(predicted.id, read([operation, "Completed"]));
    reducer.apply(command); // Existing idempotency, rather than UI de-duplication.
    const final = selected.getSnapshot();
    const canonicalMessages = reducer.conversationPage(0n, 1).total_messages;
    selected.dispose(); client.discard(branch); client.release(predicted.id);
    return { provisional, unknownOutcome, finalProvenance: final.branch, finalPrediction: final.hypotheses[0]?.prediction, canonicalMessages };
  } finally { client.dispose(); reducer.free(); }
}
