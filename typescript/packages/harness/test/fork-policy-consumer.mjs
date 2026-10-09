/** Pure pre-admission policy consumer shared by installed Bun and Chromium.
 * Provider capture/publication is exercised separately by the Rust preparer consumer. */
export async function exerciseForkPolicy(Harness, parentOptions, contracts) {
  const assert = (value, message) => { if (!value) throw new Error(message); };
  const rejects = (action, message) => { let failed = false; try { action(); } catch { failed = true; } assert(failed, message); };
  const authority = { kind: "conversation", id: `${parentOptions.authority.id}-fork-policy` };
  const options = { ...parentOptions, authority };
  const harness = await Harness.create(options);
  const parentAgent = "11111111-1111-1111-1111-111111111111";
  const childAgent = "22222222-2222-2222-2222-222222222222";
  const provider = { namespace: "fork-policy", family: "filesystem", version: "2" };
  const volume = { provider, id: "parent-private", class: "agent_private", owner: { kind: "agent", id: parentAgent } };
  const file = contracts.validate("file_ref", { volume, path: "request.txt", version: "1",
    descriptor: contracts.fileDescriptor(new TextEncoder().encode("parent"), "text/plain"), display_name: "request.txt" });
  const scope = harness.issueScopeForAgent(parentAgent, "fork-policy", ["conversation:bind", "conversation:append", harness.fileReadCapability(file)]);
  const command = (operation_id, action) => ({ authority, operation_id, idempotency_key: operation_id,
    expected_revision: harness.head()[1], scope, causal_parent: null, action });
  const message = (id, sequence) => ({ id, sequence, kind: "user", content: file, attachments: { kind: "inline", items: [] },
    reply_to: null, tool_call_id: null, extensions: {} });
  try {
    harness.apply(command("10101010-1010-1010-1010-101010101010", { kind: "bind_conversation", agent: parentAgent }));
    harness.apply(command("12121212-1212-1212-1212-121212121212", { kind: "append_conversation_message", message: message("13131313-1313-1313-1313-131313131313", 1n) }));
    const request = { operation_id: "14141414-1414-1414-1414-141414141414", parent: authority, parent_revision: harness.head()[1],
      child: { kind: "conversation", id: `${authority.id}-child` }, child_agent: childAgent, attached_agents: [],
      preparation: { child_project_volume: { provider, id: "child-project", class: "project", owner: { kind: "project", id: "project" } },
        child_private_volume: { provider, id: "child-private", class: "agent_private", owner: { kind: "agent", id: childAgent } },
        inherited_through_sequence: 0n, maximum_inherited_messages: 8n, maximum_inherited_bytes: 65536n, maximum_inherited_references: 8 },
      selections: [
        { required: true, revision: { kind: "history", reference: { kind: "stream", provider: { ...provider, family: "stream" },
          key: [...new TextEncoder().encode(`harness/v2/conversations/${authority.id}`)], version: String(harness.head()[1]) } } },
        { required: true, revision: { kind: "project", reference: { volume: { provider, id: "parent-project", class: "project", owner: { kind: "project", id: "project" } }, generation: { kind: "generation", provider, key: [1], version: null } } } },
        { required: true, revision: { kind: "private_volume", reference: { volume, generation: { kind: "generation", provider, key: [2], version: null }, paths: [] } } },
      ], boundary: null };
    const inherited = harness.prepareForkRequest(request);
    assert(request.preparation.inherited_through_sequence === 0n, "policy mutated caller request");
    assert(inherited.preparation.inherited_through_sequence === 1n, "default did not pin logical tail");
    assert(harness.prepareForkRequest(request, "fresh").preparation.inherited_through_sequence === 0n, "fresh inherited history");
    rejects(() => harness.prepareForkRequest(request, "summary"), "unimplemented summary was silently substituted");
    const summary = { checkpoint: { ...file, path: "checkpoint.json",
      descriptor: contracts.fileDescriptor(new TextEncoder().encode("{}"), "application/json") },
      limits: { file_bytes: BigInt(Number.MAX_SAFE_INTEGER), path_bytes: 0xffff_ffffn, attachments: 0xffff_ffffn,
        render_bytes: BigInt(Number.MAX_SAFE_INTEGER), model_steps: 0xffff_ffffn, model_events_per_step: 0xffff_ffffn,
        tool_calls_per_step: 0xffff_ffffn, context_messages: 0xffff_ffffn },
      history_limits: { maximum_events: 16, maximum_bytes: 65536n } };
    const summarized = harness.prepareForkRequest(request, { summary });
    for (const [field, value] of Object.entries(summary.limits)) {
      const admitted = summarized.preparation.summary.limits[field];
      assert(typeof admitted === "number" && BigInt(admitted) === value, `Summary ${field} lost exact numeric limits`);
      assert(typeof summary.limits[field] === "bigint", "Summary policy mutated caller limits");
    }
    rejects(() => harness.prepareForkRequest(request, { summary: { ...summary, limits: {
      ...summary.limits, file_bytes: BigInt(Number.MAX_SAFE_INTEGER) + 1n,
    } } }), "unsafe Summary limit was rounded");
    const restored = await Harness.restore(harness.snapshot(), options);
    try { assert(contracts.canonicalEqual(restored.prepareForkRequest(request), inherited), "checkpoint changed selected boundary"); }
    finally { restored.free(); }
    harness.apply(command("15151515-1515-1515-1515-151515151515", { kind: "append_conversation_message", message: message("16161616-1616-1616-1616-161616161616", 2n) }));
    assert(inherited.preparation.inherited_through_sequence === 1n, "later parent changed frozen boundary");
    rejects(() => harness.prepareForkRequest(request), "stale parent policy admission accepted");
    const later = structuredClone(request);
    later.parent_revision = harness.head()[1];
    later.selections[0].revision.reference.version = String(later.parent_revision);
    later.preparation.maximum_inherited_messages = 1n;
    rejects(() => harness.prepareForkRequest(later), "oversized logical history silently truncated");
  } finally { harness.free(); }
}
