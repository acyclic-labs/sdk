import { expect, test } from "bun:test";
import { create, toBinary } from "@bufbuild/protobuf";
import {
  ContextViewSchema,
  contextRevision,
  CreateContextRequestSchema,
  CreateEvaluationRequestSchema,
  EvaluationAggregation,
  EvaluationSpecSchema,
  EvaluationResultSchema,
  EvaluationState,
  EvaluationViewSchema,
  GenerateRunRequestSchema,
  GenerateRunResponseSchema,
  Inference,
  InferenceClient,
  InferenceProtocolError,
  itemId,
  WarmState,
  InspectContextRequestSchema,
  InspectEvaluationRequestSchema,
  InspectRunRequestSchema,
  InspectWarmRequestSchema,
  ItemKind,
  ItemSchema,
  ListModelsResponseSchema,
  MutateContextRequestSchema,
  MutationReceiptSchema,
  ReleaseWarmRequestSchema,
  RenewWarmRequestSchema,
  RetainWarmRequestSchema,
  RunEventSchema,
  RunResultSchema,
  RunTerminal,
  RunViewSchema,
  runId,
  WarmViewSchema,
  WatchRunRequestSchema,
  type InferenceTransport,
} from "../src/index.js";
import { runTerminalMetadata, validateRunTerminalMetadata, validateRuntimeShape } from "../src/contract.js";
import { INFERENCE_FIXED_WIDTHS, validateInferenceFixedWidthMetadata } from "../src/widths.js";
import { RUN_TERMINAL_METADATA } from "../generated/terminal-metadata.js";

const bytes = (value: number) => new Uint8Array([value]);
const revision = (value: number) => new Uint8Array(32).fill(value);
const runIdentity = (value: number) => new Uint8Array(16).fill(value);

test("Rust reflection supplies every nonzero terminal and validates request shape", async () => {
  const metadata = await runTerminalMetadata();
  expect(metadata).toEqual(RUN_TERMINAL_METADATA);
  expect(metadata.map(item => item.kind)).toEqual([
    "completed", "output-limited", "tool-call", "refusal", "cancelled", "failed", "indeterminate",
  ]);
  expect(metadata.filter(item => item.partial).map(item => item.kind)).toEqual(["cancelled", "failed", "indeterminate"]);
  await expect(validateRuntimeShape(CreateContextRequestSchema, JSON.parse('{"model":7}'))).rejects.toThrow("invalid protobuf type");
});

test("terminal metadata validation rejects Rust/protobuf drift", () => {
  const valid = RUN_TERMINAL_METADATA.map(item => ({ ...item }));
  expect(validateRunTerminalMetadata(JSON.stringify(valid))).toEqual(valid);
  expect(() => validateRunTerminalMetadata(JSON.stringify(valid.slice(1)))).toThrow("does not cover the generated enum");
  expect(() => validateRunTerminalMetadata(JSON.stringify(valid.map((item, index) =>
    index === 0 ? { ...item, kind: "renamed" } : item)))).toThrow("does not cover the generated enum");
  expect(() => validateRunTerminalMetadata(JSON.stringify(valid.map((item, index) =>
    index === 0 ? { ...item, partial: !item.partial } : item)))).toThrow("does not cover the generated enum");
  expect(() => validateRunTerminalMetadata("[{\"number\":1,\"kind\":\"completed\"}]")).toThrow("invalid entry");
});

test("ergonomic identity helpers enforce Rust-derived fixed widths at both boundaries", () => {
  expect(INFERENCE_FIXED_WIDTHS.runId).toBe(16);
  expect(INFERENCE_FIXED_WIDTHS.contextRevision).toBe(32);
  expect(() => runId(new Uint8Array(15))).toThrow("exactly 16 bytes");
  expect(() => runId(new Uint8Array(17))).toThrow("exactly 16 bytes");
  expect(itemId(new Uint8Array(15))).toHaveLength(15);
  expect(itemId(new Uint8Array(16))).toHaveLength(16);
  expect(itemId(new Uint8Array(17))).toHaveLength(17);
  expect(itemId(new Uint8Array(0))).toHaveLength(0);
  expect(() => contextRevision(new Uint8Array(31))).toThrow("exactly 32 bytes");
  expect(() => contextRevision(new Uint8Array(33))).toThrow("exactly 32 bytes");
  expect(runId(new Uint8Array(16))).toHaveLength(16);
  expect(contextRevision(new Uint8Array(32))).toHaveLength(32);
  expect(() => validateInferenceFixedWidthMetadata([{
    message: "inference.customer.v1.RequestIdentity", field: "request_id", width: 0,
  }])).toThrow("non-positive width");
});
const receipt = (value: number) => create(MutationReceiptSchema, { revision: revision(value), commandDigest: revision(value + 32), sequence: 1n });
const contextView = (value: Uint8Array, model = "model") => create(ContextViewSchema, {
  revision: value,
  lineage: revision(20),
  executionProfile: revision(21),
  contentDigest: revision(22),
  model,
  provenance: { origin: { case: "created", value: {} } },
});
const warmView = (commitment: Uint8Array, context = revision(1), expiresAtMs = 10n) => create(WarmViewSchema, {
  commitment,
  context,
  modelProfile: revision(23),
  latencyProfile: revision(24),
  expiresAtMs,
  state: WarmState.ACTIVE,
  evidenceDigest: revision(25),
  admissionReceiptId: revision(26),
  sequence: 1n,
});
const evaluationSpec = (specDigest = revision(30)) => create(EvaluationSpecSchema, {
  candidates: [{ digest: revision(32), mediaType: "text/plain", logicalSize: 1n }],
  suite: { identity: "suite", digest: revision(33), cases: [{ caseId: runIdentity(34), input: bytes(1) }] },
  grader: { handle: bytes(2), artifactDigest: revision(35) },
  metrics: [{ identity: "score", aggregation: EvaluationAggregation.MEAN }],
  maximumCaseResults: 1n,
  specDigest,
});
const evaluationView = (evaluationId = runIdentity(5), specDigest = revision(30)) => create(EvaluationViewSchema, {
  evaluationId,
  spec: evaluationSpec(specDigest),
  state: EvaluationState.ADMITTED,
  sequence: 1n,
});

test("generated lifecycle client covers contexts, warm commitments, runs, watch, and cancellation", async () => {
  const called: string[] = [];
  const transport: InferenceTransport = {
    async listModels() { called.push("models"); return create(ListModelsResponseSchema); },
    async createContext() { called.push("create"); return receipt(1); },
    async inspectContext(request) { called.push(`inspect:${request.revision[0]}`); return contextView(request.revision); },
    async mutateContext() { called.push("mutate"); return receipt(2); },
    async retainWarm(request) { called.push("retain"); return warmView(revision(3), request.context); },
    async inspectWarm(request) { called.push("inspect-warm"); return warmView(request.commitment); },
    async renewWarm(request) { called.push("renew"); return warmView(request.commitment, revision(1), request.expiresAtMs); },
    async releaseWarm(request) { called.push("release"); return warmView(request.commitment); },
    async generateRun() { called.push("generate"); return create(GenerateRunResponseSchema, { run: { runId: runIdentity(4), input: revision(1), model: "model" } }); },
    async inspectRun(request) { called.push("inspect-run"); return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" }); },
    async *watchRun(request) { called.push(`watch:${request.fromSequence}`); yield create(RunEventSchema, { sequence: request.fromSequence, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { called.push("cancel"); return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model", cancellationRequested: true }); },
    async createEvaluation(request) { called.push("create-evaluation"); return evaluationView(request.identity!.requestId, request.spec!.specDigest); },
    async inspectEvaluation(request) { called.push("inspect-evaluation"); return evaluationView(request.evaluationId); },
  };
  const client = new InferenceClient(transport);
  await client.listModels();
  await client.createContext(create(CreateContextRequestSchema));
  await client.inspectContext(revision(1));
  await client.mutateContext(create(MutateContextRequestSchema, { action: { case: "fork", value: {} } }));
  await client.retainWarm(create(RetainWarmRequestSchema, { context: revision(1) }));
  await client.inspectWarm(revision(3));
  await client.renewWarm(create(RenewWarmRequestSchema, { commitment: revision(3), expiresAtMs: 20n }));
  await client.releaseWarm(create(ReleaseWarmRequestSchema, { commitment: revision(3) }));
  await client.generate(create(GenerateRunRequestSchema, { identity: { clientInstance: runIdentity(3), requestId: runIdentity(4) }, context: revision(1) }));
  await client.inspectRun(runIdentity(4));
  for await (const event of client.watchRun(runIdentity(4), 7n)) expect(event.sequence).toBe(7n);
  expect((await client.cancelRun(runIdentity(4))).cancellationRequested).toBeTrue();
  const evaluationId = runIdentity(5);
  const specDigest = revision(30);
  await client.createEvaluation(create(CreateEvaluationRequestSchema, {
    identity: { clientInstance: runIdentity(6), requestId: evaluationId },
    spec: evaluationSpec(specDigest),
  }));
  await client.inspectEvaluation(evaluationId);
  expect(called).toEqual(["models", "create", "inspect:1", "mutate", "retain", "inspect-warm", "renew", "release", "generate", "inspect-run", "inspect-run", "watch:7", "cancel", "create-evaluation", "inspect-evaluation"]);
});

test("high-level handles preserve typed context, run, and warm identities", async () => {
  let revisionCounter = 0;
  const transport: InferenceTransport = {
    async listModels() { return create(ListModelsResponseSchema); },
    async createContext(request) { expect(request.items[0]?.kind).toBe(ItemKind.USER); return receipt(++revisionCounter); },
    async inspectContext(request) { const view = contextView(request.revision); view.items.push(create(ItemSchema, { kind: ItemKind.USER })); return view; },
    async mutateContext(request) { expect(request.action.case).not.toBeUndefined(); return receipt(++revisionCounter); },
    async retainWarm(request) { return warmView(revision(7), request.context); },
    async inspectWarm(request) { return warmView(request.commitment, revision(3)); },
    async renewWarm(request) { return warmView(request.commitment, revision(3), request.expiresAtMs); },
    async releaseWarm(request) { return warmView(request.commitment, revision(3)); },
    async generateRun(request) { return create(GenerateRunResponseSchema, { run: { runId: request.identity!.requestId, input: request.context, model: "model" } }); },
    async inspectRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(3), model: "model", lastSequence: 4n, result: create(RunResultSchema, { output: bytes(9), terminal: RunTerminal.COMPLETED, context: create(ContextViewSchema, { revision: revision(10) }) }) }); },
    async *watchRun(request) { yield create(RunEventSchema, { sequence: request.fromSequence, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(3), model: "model", cancellationRequested: true }); },
    async createEvaluation(request) { return evaluationView(request.identity!.requestId, request.spec!.specDigest); },
    async inspectEvaluation(request) { return evaluationView(request.evaluationId); },
  };
  const inference = new Inference(new InferenceClient(transport));
  const item = create(ItemSchema, { kind: ItemKind.USER, payload: bytes(1) });
  const context = await inference.create("model", [item]);
  expect((await context.items())[0]?.kind).toBe(ItemKind.USER);
  const fork = await context.append(item).then(value => value.fork());
  expect(fork.id()).toEqual(revision(3));
  const run = await fork.generate(item, { maximumOutput: 32n });
  expect((await inference.attach(context.id())).id()).toEqual(context.id());
  expect((await inference.recoverRun(run.id())).id()).toEqual(run.id());
  const result = await run.result();
  expect(result.terminal).toBe("completed");
  expect(result.output).toEqual(bytes(9));
  expect(result.continuationValid).toBeTrue();
  expect(result.context?.id()).toEqual(revision(10));
  expect((await run.cancel()).cancellationRequested).toBeTrue();
  const events = []; for await (const event of run.events({ from: 4n })) events.push(event.sequence);
  expect(events).toEqual([4n]);
  const warm = await fork.retain({ latencyProfile: revision(2), expiresAtMs: 10n });
  expect(warm.id()).toEqual(revision(7));
  await warm.inspect();
  expect((await warm.renew(20n)).expiresAtMs).toBe(20n);
  expect((await warm.release()).commitment).toEqual(revision(7));
});

test("run recovery rejects substituted or malformed streams and observes an inclusive zero cursor", async () => {
  const id = runIdentity(4);
  let inspectCount = 0;
  const transport: InferenceTransport = {
    async listModels() { return create(ListModelsResponseSchema); }, async createContext() { return create(MutationReceiptSchema); },
    async inspectContext() { return create(ContextViewSchema); }, async mutateContext() { return create(MutationReceiptSchema); },
    async retainWarm() { return create(WarmViewSchema); }, async inspectWarm() { return create(WarmViewSchema); },
    async renewWarm() { return create(WarmViewSchema); }, async releaseWarm() { return create(WarmViewSchema); },
    async generateRun() { return create(GenerateRunResponseSchema); },
    async inspectRun(request) { inspectCount += 1; return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model", lastSequence: 0n, ...(inspectCount > 1 ? { result: create(RunResultSchema, { terminal: RunTerminal.COMPLETED }) } : {}) }); },
    async *watchRun(request) { expect(request.fromSequence).toBe(0n); yield create(RunEventSchema, { sequence: 0n, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
    async cancelRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" }); },
    async createEvaluation(request) { return evaluationView(request.identity!.requestId, request.spec!.specDigest); },
    async inspectEvaluation(request) { return evaluationView(request.evaluationId); },
  };
  const result = await new Inference(new InferenceClient(transport)).run(runId(id)).result();
  expect(result.terminal).toBe("completed");

  let reopened = false;
  const completed = new InferenceClient({
    ...transport,
    async inspectRun(request) { return create(RunViewSchema, {
      runId: request.runId, input: revision(1), model: "model", lastSequence: 7n,
      result: { terminal: RunTerminal.COMPLETED },
    }); },
    async *watchRun() { reopened = true; throw new Error("completed run was reopened"); },
  });
  let observed = 0;
  for await (const _event of completed.watchRun(id, 8n)) observed += 1;
  expect(observed).toBe(0);
  expect(reopened).toBeFalse();
  await expect(async () => {
    for await (const _event of completed.watchRun(id, 9n)) { /* exhaust */ }
  }).toThrow("Run cursor exceeds retained events");
  expect(reopened).toBeFalse();

  const controller = new AbortController();
  const cancellable = new InferenceClient({
    ...transport,
    inspectRun(_request, signal) {
      return new Promise((_resolve, reject) => {
        signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")), { once: true });
      });
    },
    async *watchRun() { reopened = true; throw new Error("watch opened after cancellation"); },
  });
  const pending = cancellable.watchRun(id, 0n, controller.signal)[Symbol.asyncIterator]().next();
  controller.abort();
  await expect(pending).rejects.toThrow("aborted");
  expect(reopened).toBeFalse();

  const substituted = new InferenceClient({ ...transport, async inspectRun() { return create(RunViewSchema, { runId: runIdentity(9), input: revision(1), model: "model" }); } });
  await expect(substituted.inspectRun(id)).rejects.toThrow("identity differs");

  const substitutedGeneration = new InferenceClient({ ...transport, async generateRun(request) { return create(GenerateRunResponseSchema, { run: { runId: runIdentity(9), input: request.context, model: "model" } }); } });
  await expect(substitutedGeneration.generate(create(GenerateRunRequestSchema, { identity: { clientInstance: runIdentity(2), requestId: id }, context: revision(1) }))).rejects.toThrow("identity differs");

  const emptyContext = new InferenceClient({ ...transport, async generateRun(request) {
    return create(GenerateRunResponseSchema, { run: { runId: request.identity!.requestId, input: revision(1), model: "model" } });
  } });
  await expect(emptyContext.generate(create(GenerateRunRequestSchema, {
    identity: { clientInstance: runIdentity(2), requestId: id },
  }))).rejects.toThrow("identity length differs");

  const malformed = new InferenceClient({ ...transport, async *watchRun() { yield create(RunEventSchema, { sequence: 2n }); } });
  await expect(async () => { for await (const _event of malformed.watchRun(id)) { /* exhaust */ } }).toThrow("run event order or shape differs");

  const undefinedEvent = new InferenceClient({ ...transport, async *watchRun() { yield undefined as never; } });
  await expect(async () => { for await (const _event of undefinedEvent.watchRun(id)) { /* exhaust */ } }).toThrow(InferenceProtocolError);

  const truncated = new InferenceClient({ ...transport, async *watchRun() { yield create(RunEventSchema, { sequence: 0n, event: { case: "progress", value: { kind: "queued" } } }); } });
  await expect(async () => { for await (const _event of truncated.watchRun(id)) { /* exhaust */ } }).toThrow("run stream ended before terminal");

  const duplicate = new InferenceClient({ ...transport, async *watchRun() {
    yield create(RunEventSchema, { sequence: 0n, event: { case: "progress", value: { kind: "queued" } } });
    yield create(RunEventSchema, { sequence: 0n, event: { case: "terminal", value: RunTerminal.COMPLETED } });
  } });
  await expect(async () => { for await (const _event of duplicate.watchRun(id)) { /* exhaust */ } }).toThrow("run event order or shape differs");

  const postTerminal = new InferenceClient({ ...transport, async *watchRun() {
    yield create(RunEventSchema, { sequence: 0n, event: { case: "terminal", value: RunTerminal.COMPLETED } });
    yield create(RunEventSchema, { sequence: 1n, event: { case: "progress", value: { kind: "late" } } });
  } });
  await expect(async () => { for await (const _event of postTerminal.watchRun(id)) { /* exhaust */ } }).toThrow("run event order or shape differs");

  const overflow = new InferenceClient({ ...transport,
    async inspectRun(request) { return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" }); },
    async *watchRun() { yield create(RunEventSchema, { sequence: (1n << 64n) - 1n, event: { case: "terminal", value: RunTerminal.COMPLETED } }); },
  });
  await expect(async () => { for await (const _event of overflow.watchRun(id, (1n << 64n) - 1n)) { /* exhaust */ } }).toThrow("Run sequence exhausted");

  expect(() => contextRevision(bytes(1))).toThrow("exactly 32 bytes");
  const substitutedContext = new InferenceClient({ ...transport, async inspectContext() { return contextView(revision(9)); } });
  await expect(substitutedContext.inspectContext(revision(1))).rejects.toThrow("revision differs");
  const substitutedWarm = new InferenceClient({ ...transport, async inspectWarm() { return warmView(revision(9)); } });
  await expect(substitutedWarm.inspectWarm(revision(8))).rejects.toThrow("warm commitment shape differs");
  const malformedReceipt = new InferenceClient({ ...transport, async createContext() { return create(MutationReceiptSchema); } });
  await expect(malformedReceipt.createContext(create(CreateContextRequestSchema))).rejects.toThrow("identity length differs");

  const zeroIdentity = new InferenceClient({ ...transport, async inspectRun(request) {
    return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model" });
  } });
  await expect(zeroIdentity.inspectRun(new Uint8Array(16))).rejects.toThrow("identity");

  const malformedContinuation = new InferenceClient({ ...transport, async inspectRun(request) {
    return create(RunViewSchema, { runId: request.runId, input: revision(1), model: "model",
      result: { terminal: RunTerminal.COMPLETED, context: { revision: bytes(1) } } });
  } });
  await expect(malformedContinuation.inspectRun(id)).rejects.toThrow("identity length differs");

  let admitted = false;
  const invalidSpec = new InferenceClient({ ...transport, async createEvaluation() {
    admitted = true;
    return evaluationView();
  } });
  const unbounded = evaluationSpec();
  unbounded.maximumCaseResults = 0n;
  await expect(invalidSpec.createEvaluation(create(CreateEvaluationRequestSchema, {
    identity: { clientInstance: runIdentity(6), requestId: runIdentity(5) }, spec: unbounded,
  }))).rejects.toThrow("evaluation result bound is invalid");
  expect(admitted).toBeFalse();

  const substitutedEvaluation = new InferenceClient({
    ...transport,
    async createEvaluation(request) { return evaluationView(request.identity!.requestId, revision(31)); },
  });
  await expect(substitutedEvaluation.createEvaluation(create(CreateEvaluationRequestSchema, {
    identity: { clientInstance: runIdentity(6), requestId: runIdentity(5) },
    spec: evaluationSpec(),
  }))).rejects.toThrow("evaluation admission spec differs");

  const completedWithoutResult = new InferenceClient({
    ...transport,
    async inspectEvaluation(request) {
      const view = evaluationView(request.evaluationId);
      view.state = EvaluationState.COMPLETED;
      return view;
    },
  });
  await expect(completedWithoutResult.inspectEvaluation(runIdentity(5))).rejects.toThrow("evaluation state is invalid");

  const runningWithResult = new InferenceClient({
    ...transport,
    async inspectEvaluation(request) {
      const view = evaluationView(request.evaluationId);
      view.state = EvaluationState.RUNNING;
      view.result = create(EvaluationResultSchema, { specDigest: revision(30), resultDigest: revision(31) });
      return view;
    },
  });
  await expect(runningWithResult.inspectEvaluation(runIdentity(5))).rejects.toThrow("evaluation state is invalid");

  const forgedObservation = new InferenceClient({ ...transport, async inspectEvaluation(request) {
    const view = evaluationView(request.evaluationId);
    view.state = EvaluationState.COMPLETED;
    view.result = create(EvaluationResultSchema, { specDigest: revision(30), resultDigest: revision(31),
      caseResults: [{ candidateDigest: revision(32), caseId: runIdentity(34),
        observation: { nativeOutputDigest: revision(36), observationDigest: revision(37), bindingDigest: revision(38) } }] });
    return view;
  } });
  await expect(forgedObservation.inspectEvaluation(runIdentity(5))).rejects.toThrow("observation binding differs");
});
