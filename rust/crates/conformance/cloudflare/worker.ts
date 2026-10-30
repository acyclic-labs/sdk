import { DurableObject } from "cloudflare:workers";
import { initSync, qualifyCloudflare, qualifyCloudflareRpc, qualifyJournalDeadline, appendWakeRecord, spawnAdoptionFixture,
  hostFixtureGrants, hostFixtureTrust, hostFixtureCredential, hostFixtureModelEvents, hostFixtureTool } from "./generated/acyclic_cloudflare_conformance.js";
import wasm from "./generated/acyclic_cloudflare_conformance_bg.wasm";
import { SqliteJournal, type StreamChange } from "../../../../typescript/packages/harness/src/cloudflare-storage.js";
import { openCloudflareAgent, openCloudflareLedger } from "../../../../typescript/packages/harness/src/cloudflare.js";
import { streamLedgerPort } from "../../../../typescript/packages/harness/src/event-ledger.js";
import { initSync as initGuest, WasmAgentAuthority, WasmAgentRegistry, WasmObjects, type WasmFileRefWire, type Preset } from "../../../../typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js";
import { fixtureAuthority } from "./fixture-authority.mjs";
import guestWasm from "./generated/agent_guest_bg.wasm";

initSync({ module: wasm });
initGuest({ module: guestWasm });

const options = {
  filesystem_database: "unused", stream_database: "unused", maximum_object_bytes: 1048576n,
  filesystem_provider: { namespace: "do-conformance", family: "filesystem", version: "2" },
  stream_limits: { commands: 65536n, journal_bytes: 268435456n, paths: 1000, path_bytes: 262144,
    records: 10000, payload_bytes: 16777216, commits: 10000, idempotency_results: 10000 },
  limits: { file_bytes: 1048576n, path_bytes: 4096n, attachments: 32n, render_bytes: 1048576n,
    model_steps: 16n, model_events_per_step: 64n, tool_calls_per_step: 16n, context_messages: 128n },
  principal: "do-conformance",
  // This host explicitly accepts every action its agents take.
  preset: { model: null, prompt: null, tools: [], web_tools: false, max_steps: null, max_children: 8, approval_policy: { approvers: [{ kind: "rules", principal: "do-conformance", rules: [], fallback: "allow" }] } },
  handler_timeout_ms: 30_000n,
};

export class Ledger extends DurableObject {
  private readonly ledger = openCloudflareLedger(this.ctx,
    actor => this.env.AGENT.getByName(actor).wake(), { ...options.stream_limits, commands: 65536 });
  async register(actor: string, paths: string[]) {
    const placement = await this.ledger.register(actor, paths);
    if (actor === "uncertain-placement" && "admitted" in placement
      && !(await this.ctx.storage.get("lost-placement-ack"))) {
      await this.ctx.storage.put("lost-placement-ack", true);
      throw new Error("STEP injected placement acknowledgement loss; recovery: retry the original named placement");
    }
    return placement;
  }
  async dispatch(actor: string, operation: string, input: Uint8Array) {
    if (actor === "saved-creating" && operation === "commit"
      && !(await this.ctx.storage.get("resume-saved-creation"))) {
      throw new Error("STEP injected root commit interruption; recovery: resume the saved creation after emulator restart");
    }
    return this.ledger.dispatch(actor, operation, input);
  }
  resumeCreation() { return this.ctx.storage.put("resume-saved-creation", true); }
  open(operation: string, input: Uint8Array, preceding: Uint8Array) { return this.ledger.open(operation, input, preceding); }
  /** The domain's one Objects v1 account: one route's reply body per RPC. */
  async invoke(route: string, input: Uint8Array) {
    const request = this.ledger.objects.open(route, input);
    try {
      const reply = await request.next();
      if (!reply) throw new Error("STEP Objects request closed before its reply; recovery: retry the agent operation");
      return reply.body;
    } finally { request.free(); }
  }
  alarm() { return this.ledger.alarm(); }
  pending() { return this.ledger.pending(); }
}

/** The domain's Objects RPC as an `ObjectsV1BinaryPort`: an RPC cannot be
 * cancelled once sent, so closing only settles a request that never pulled. */
function rpcPort(domain: { invoke(route: string, input: Uint8Array): Promise<Uint8Array> }) {
  return {
    open(route: string, input: Uint8Array) {
      const bytes = input.slice();
      let done = false;
      return {
        async next() {
          if (done) return null;
          done = true;
          return { body: await domain.invoke(route, bytes) };
        },
        close() { done = true; },
      };
    },
  };
}

export class Agent extends DurableObject {
  private owner?: WasmAgentAuthority;
  private custody?: ReturnType<typeof fixtureAuthority>;
  private readonly registry = new WasmAgentRegistry();
  private agent?: ReturnType<typeof openCloudflareAgent>;
  private executions = 0;
  private failAlarmClear = false;
  private retainedRecoveryAlarm = false;
  private grants = hostFixtureGrants();
  private readonly timeout = { started: false, aborted: false };
  private readonly started = Promise.withResolvers<void>();
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    this.ctx.blockConcurrencyWhile(async () => {
      this.grants = await this.ctx.storage.get("fixture-grants") ?? this.grants;
    });
    const timeout = this.timeout;
    const started = this.started;
    const hangs = ctx.id.name === "timeout" || ctx.id.name === "cancel";
    const protocol = ctx.id.name?.startsWith("protocol-");
    if (protocol) this.registry.registerTool(hostFixtureTool(),
      (invocation: { arguments: unknown }) => { ++this.executions; return { value: invocation.arguments }; },
      () => null, (_invocation: unknown, result: { value: unknown }) => result.value);
    this.registry.configureModel({ provider: "conformance", name: "scripted", revision: "1", options: {} },
      async function* (_bytes: Uint8Array, dispatch: { step: number }, signal: AbortSignal) {
        if (hangs) {
          timeout.started = true;
          started.resolve();
          await new Promise<void>(resolve => {
            const aborted = () => { timeout.aborted = true; resolve(); };
            if (signal.aborted) aborted();
            else signal.addEventListener("abort", aborted, { once: true });
          });
          return;
        }
        if (protocol) { yield* hostFixtureModelEvents(dispatch.step); return; }
        yield { kind: "content", delta: "workerd guest reply" };
        yield { kind: "completed", metadata: {} };
      }, () => null, () => ({ context_tokens: 131072, output_tokens: 8192 }),
      (bytes: Uint8Array, request_digest: Uint8Array) => ({ request_digest, input_tokens: BigInt(bytes.byteLength),
        fixed_tokens: bytes.byteLength, message_tokens: JSON.parse(new TextDecoder().decode(bytes)).messages.map(() => 0) }),
      { kind: "disabled" });
  }
  private host(preset: Preset = options.preset): ReturnType<typeof openCloudflareAgent> {
    if (this.agent) return this.agent;
    const custody = this.custody ??= fixtureAuthority("00112233-4455-6677-8899-aabbccddeeff", this.grants);
    const owner = this.owner ??= new WasmAgentAuthority(custody.capability);
    const name = this.ctx.id.name!;
    const context = { storage: {
      sql: this.ctx.storage.sql,
      transactionSync: <T>(operation: () => T) => this.ctx.storage.transactionSync(operation),
      sync: () => this.ctx.storage.sync(),
      setAlarm: (time: number) => this.ctx.storage.setAlarm(time),
      deleteAlarm: async () => {
        if (this.failAlarmClear) {
          this.failAlarmClear = false;
          this.retainedRecoveryAlarm = await this.ctx.storage.getAlarm() !== null;
          throw new Error("STEP injected alarm-clear failure; recovery: deliver the retained recovery alarm");
        }
        return this.ctx.storage.deleteAlarm();
      },
    } };
    const domain = this.env.LEDGER.getByName(name.startsWith("protocol-") ? name : "suite");
    const ledger = { register: (actor: string, paths: string[]) => domain.register(actor, paths),
      dispatch: (actor: string, operation: string, input: Uint8Array) => domain.dispatch(actor, operation, input),
      open: (operation: string, input: Uint8Array, preceding: Uint8Array) => domain.open(operation, input, preceding),
      objects: new WasmObjects({ port: rpcPort(domain) }) };
    return this.agent = openCloudflareAgent(context, ledger, name,
      { ...options, preset, handler_timeout_ms: name === "timeout" ? 1000n : options.handler_timeout_ms }, owner, this.registry,
      { trust: { trust: new TextEncoder().encode(hostFixtureTrust()), audience: "workers:/test", holder: "account" },
      credential: hostFixtureCredential(), policy: () => true });
  }
  async configureAdmission(grants: string[]) {
    if (this.agent) throw new Error("STEP fixture-admission failed: host already opened; recovery: use a fresh named fixture host");
    await this.ctx.storage.put("fixture-grants", grants);
    this.grants = grants;
  }
  configureProtocol(policy: Preset["approval_policy"]) { this.host({ ...options.preset, approval_policy: policy }); }
  failNextAlarmClear() { this.failAlarmClear = true; }
  alarmFailureRetainedRecovery() { return this.retainedRecoveryAlarm; }
  binding() { return this.host().binding(); }
  call(invocation: Uint8Array) { return this.host().call(invocation, hostFixtureCredential()); }
  effects() { return BigInt(this.executions); }
  create(intent: string, brief?: string) { return this.host().create(intent, brief); }
  placementStage() {
    const [row] = this.ctx.storage.sql.exec("SELECT value FROM acyclic_host_metadata WHERE domain = ? AND key = ?", "actor", "identity");
    return row ? JSON.parse(String(row.value)).stage as string : undefined;
  }
  adopt(path: string) { return this.host().adopt(path); }
  send(intent: string, text: string) { return this.host().send(intent, text); }
  read(stream: "inbox" | "conversation" | "status") { return this.host().read(stream); }
  pause(paused: boolean) { return this.host().pause(paused); }
  cancel(intent: string) { return this.host().cancel(intent); }
  timeoutState() { return this.timeout; }
  waitStarted() { return this.started.promise; }
  readFile(file: WasmFileRefWire) { return this.host().readFile(file); }
  wake() { return this.host().wake(); }
  alarm() { return this.host().wake(); }
}

interface Env {
  SUITE: DurableObjectNamespace<Suite>;
  LEDGER: DurableObjectNamespace<Ledger>;
  AGENT: DurableObjectNamespace<Agent>;
}

async function endpoint(env: Env, policy: Preset["approval_policy"], name = `protocol-${crypto.randomUUID()}`) {
  const agent = env.AGENT.getByName(name);
  await agent.configureProtocol(policy);
  await agent.create("protocol-root");
  const ledger = env.LEDGER.getByName(name.startsWith("protocol-") ? name : "suite");
  return { binding: () => agent.binding(), call: (invocation: Uint8Array) => agent.call(invocation),
    provider: () => streamLedgerPort(ledger, ""), drain: () => agent.wake(),
    readFile: (_agent: string, file: WasmFileRefWire) => agent.readFile(file), effects: () => agent.effects() };
}

export class Suite extends DurableObject {
  private wakeLedger?: ReturnType<typeof openCloudflareLedger>;
  alarm() { return this.wakeLedger?.alarm(); }

  async deadline(retry: boolean): Promise<void> {
    const journal = new SqliteJournal(this.ctx.storage, "deadline-admission");
    await qualifyJournalDeadline({
      configure: (configuration: Uint8Array, maximum: number, authority: Uint8Array) => journal.configure(configuration, maximum, authority),
      flush: () => journal.flush(),
      read: (index: number) => journal.read(index),
      async append(index: number, admit: () => Uint8Array, changes: readonly StreamChange[]) {
        await new Promise(resolve => setTimeout(resolve, 100));
        await journal.append(index, admit, changes);
      },
    }, retry);
  }

  async wakeIsolation(): Promise<void> {
    const delivered: string[] = [];
    const ledger = openCloudflareLedger(this.ctx, async actor => {
      delivered.push(actor);
      if (actor === "a-failing") throw new Error("injected wake delivery failure");
    }, { ...options.stream_limits, commands: 65536 });
    this.wakeLedger = ledger;
    try {
      for (const [actor, prefix] of [["a-failing", "wake/a"], ["z-healthy", "wake/z"]]) {
        const placement = await ledger.register(actor!, [`${prefix}/inbox`, `${prefix}/state`]);
        if (!("admitted" in placement)) {
          throw new Error("STEP wake-fixture placement failed; recovery: bash scripts/qualify-cloudflare.sh");
        }
      }
      const port = streamLedgerPort(ledger, "");
      await appendWakeRecord(port, "wake/a/inbox");
      await appendWakeRecord(port, "wake/z/inbox");
      let failed = false;
      try { await ledger.alarm(); } catch { failed = true; }
      if (!failed || !delivered.includes("a-failing") || !delivered.includes("z-healthy") || ledger.pending() !== 1) {
        throw new Error("STEP wake-failure isolation failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      delivered.length = 0;
      try { await ledger.alarm(); } catch { /* The same failed delivery remains pending. */ }
      if (delivered.length === 0 || delivered.some(actor => actor !== "a-failing") || ledger.pending() !== 1) {
        throw new Error("STEP wake-failure retention failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
    } finally { await this.ctx.storage.deleteAlarm(); this.wakeLedger = undefined; }
  }

  async fetch(): Promise<Response> {
    const started = Date.now();
    const hosted = (policy: Preset["approval_policy"]) => endpoint(this.env, policy);
    await qualifyCloudflare(() => new SqliteJournal(this.ctx.storage, crypto.randomUUID()), hosted);
    const sqlite = Date.now() - started;
    await qualifyCloudflareRpc(() => streamLedgerPort(this.env.LEDGER.getByName(crypto.randomUUID()), ""), hosted);
    return Response.json({ sqlite, rpc: Date.now() - started - sqlite, stream: "passed", agentHost: "passed" });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    if (new URL(request.url).pathname === "/alarm-recovery") {
      const agent = env.AGENT.getByName("alarm-recovery");
      await agent.create("alarm-root", "Reply once after committing this prompt");
      await agent.failNextAlarmClear();
      let failed = false;
      try { await agent.wake(); }
      catch (error) { failed = String(error).includes("injected alarm-clear failure"); }
      const before = await agent.read("conversation");
      if (!failed || before.length !== 1 || !await agent.alarmFailureRetainedRecovery()) {
        throw new Error("STEP alarm-clear retention failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await agent.wake();
      await agent.create("alarm-root", "Reply once after committing this prompt");
      if ((await agent.read("conversation")).length !== before.length) {
        throw new Error("STEP alarm recovery replayed a committed turn; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await agent.send("after-alarm-recovery", "Reply after the failed alarm write");
      if ((await agent.read("conversation")).length !== before.length + 1) {
        throw new Error("STEP alarm recovery closed its healthy host; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return Response.json({ alarm: "failed clear retains recovery; the same host resumes without replay and accepts the next turn" });
    }
    if (new URL(request.url).pathname === "/creation-interrupted") {
      const agent = env.AGENT.getByName("saved-creating");
      let interrupted = false;
      console.log("STEP saved-creation capture: submitting the original root before injected commit interruption");
      try { await agent.create("saved-creation-root"); } catch { interrupted = true; }
      const stage = await agent.placementStage();
      if (!interrupted || stage !== "creating") {
        throw new Error(`STEP saved-creation capture failed: ${stage}; recovery: bash scripts/qualify-cloudflare.sh`);
      }
      return Response.json({ creation: "captured actual durable creating stage before interrupted root commit" });
    }
    if (new URL(request.url).pathname === "/creation-replay") {
      await env.LEDGER.getByName("suite").resumeCreation();
      const agent = env.AGENT.getByName("saved-creating");
      const path = await agent.create("saved-creation-root");
      if (path !== await agent.create("saved-creation-root") || await agent.placementStage() !== "ready"
        || (await agent.read("inbox")).length !== 1) {
        throw new Error("STEP saved-creation restart failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return Response.json({ creation: "saved creating stage resumes exactly once after emulator restart" });
    }
    if (["/deadline", "/deadline-replay"].includes(new URL(request.url).pathname)) {
      const retry = new URL(request.url).pathname === "/deadline-replay";
      await env.SUITE.getByName("deadline").deadline(retry);
      return Response.json({ deadline: retry ? "expired acceptance remains refused after emulator restart; same key commits and replays" : "delayed acceptance refuses expired commit without records or retry-key effects" });
    }
    if (new URL(request.url).pathname === "/placement") {
      const owner = env.AGENT.getByName("placement-owner");
      const path = await owner.create("placement-root");
      const inbox = (await owner.read("inbox")).length;
      const adopter = env.AGENT.getByName("placement-adopter");
      const creator = env.AGENT.getByName("placement-creator");
      let refused = 0;
      try { await adopter.adopt(path); } catch { refused += 1; }
      try { await creator.create("placement-root"); } catch { refused += 1; }
      await adopter.wake();
      await creator.wake();
      if (refused !== 2 || (await adopter.read("inbox")).length !== 0
        || (await creator.read("inbox")).length !== 0 || (await owner.read("inbox")).length !== inbox) {
        throw new Error("STEP rejected-placement refusal failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await adopter.create("after-refused-adoption");
      await creator.create("after-refused-creation");
      const uncertain = env.AGENT.getByName("uncertain-placement");
      let unknown = false;
      try { await uncertain.create("uncertain-root", "Respond after recovering placement"); }
      catch { unknown = true; }
      let changed = false;
      try { await uncertain.create("changed-after-unknown"); } catch { changed = true; }
      if (!unknown || !changed) {
        throw new Error("STEP uncertain-placement custody failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return Response.json({ placement: "refused adoption and creation leave named hosts reusable; unknown retains original intent" });
    }
    if (new URL(request.url).pathname === "/placement-replay") {
      await env.AGENT.getByName("placement-adopter").create("after-refused-adoption");
      await env.AGENT.getByName("placement-creator").create("after-refused-creation");
      const uncertain = env.AGENT.getByName("uncertain-placement");
      const path = await uncertain.create("uncertain-root", "Respond after recovering placement");
      if (path !== await uncertain.create("uncertain-root", "Respond after recovering placement")
        || (await uncertain.read("conversation")).length !== 1) {
        throw new Error("STEP uncertain-placement replay failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return Response.json({ placement: "refusal recovery and lost-ack exact replay survive emulator restart" });
    }
    if (new URL(request.url).pathname === "/review") {
      const agent = env.AGENT.getByName("invalid-create");
      let rejected = 0;
      for (const [intent, brief] of [["", undefined], ["has space", undefined], ["valid-intent", "x".repeat(1048577)]]) {
        try { await agent.create(intent!, brief); } catch { rejected += 1; }
      }
      if (rejected !== 3 || (await agent.read("inbox")).length !== 0) {
        throw new Error("STEP invalid-create refusal failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      const path = await agent.create("valid-root");
      if (path !== await agent.create("valid-root")) {
        throw new Error("STEP valid-create recovery failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      const fixture = await spawnAdoptionFixture((policy: Preset["approval_policy"]) => endpoint(env, policy, "adoption-parent")) as { child: string; grants: string[] };
      const { child } = fixture;
      const adoption = env.AGENT.getByName("invalid-adopt");
      await adoption.configureAdmission(fixture.grants);
      let refused = 0;
      for (const candidate of ["not-an-agent", "harness/agents/ffffffffffffffffffffffffffffffff",
        "harness/agents/ffffffffffffffffffffffffffffffff/children/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"]) {
        try { await adoption.adopt(candidate); } catch { refused += 1; }
      }
      await adoption.wake();
      if (refused !== 3 || (await adoption.read("inbox")).length !== 0) {
        throw new Error("STEP invalid-adopt refusal failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await adoption.adopt(child);
      await adoption.adopt(child);
      await adoption.cancel("adopted-control");
      if ((await adoption.read("status")).length === 0) {
        throw new Error("STEP valid-adopt recovery failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await env.SUITE.getByName("wake-failure").wakeIsolation();
      return Response.json({ creation: "invalid input leaves identity available", adoption: "invalid or absent agents leave placement available; saved child runs", child, wakes: "failed delivery retains only itself" });
    }
    if (new URL(request.url).pathname === "/adoption-replay") {
      const adoption = env.AGENT.getByName("invalid-adopt");
      // Cancellation is deliberately the first request after the full restart.
      await adoption.cancel("cold-adopted-control");
      await adoption.adopt(new URL(request.url).searchParams.get("path")!);
      await adoption.cancel("adopted-control");
      if ((await adoption.read("status")).length === 0) {
        throw new Error("STEP adopted-host restart failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return Response.json({ adoption: "saved child placement survives emulator restart" });
    }
    if (new URL(request.url).pathname === "/cancel") {
      const agent = env.AGENT.getByName("cancel");
      const creation = agent.create("cancel-root", "Wait for an inbox cancellation").then(
        value => ({ value }), error => ({ error: String(error) }));
      await Promise.race([agent.waitStarted(), creation.then(outcome => {
        throw new Error(`STEP guest-cancel start failed: ${JSON.stringify(outcome)}; recovery: bash scripts/qualify-cloudflare.sh`);
      })]);
      await agent.cancel("cancel-running-guest");
      const outcome = await creation;
      if ("error" in outcome || !(await agent.timeoutState()).aborted) {
        throw new Error(`STEP guest-cancel failed: ${JSON.stringify(outcome)}; recovery: bash scripts/qualify-cloudflare.sh`);
      }
      return Response.json({ cancel: "aborted running guest through its committed inbox" });
    }
    if (new URL(request.url).pathname === "/timeout") {
      const agent = env.AGENT.getByName("timeout");
      let failed = false;
      try { await agent.create("timeout-root", "Wait until the host aborts this guest"); }
      catch { failed = true; }
      const state = await agent.timeoutState();
      if (!failed || !state.started || !state.aborted || (await agent.read("conversation")).length !== 0) {
        throw new Error("STEP guest-timeout abort failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      await agent.pause(true);
      return Response.json({ timeout: "aborted", input: "unconsumed" });
    }
    if (new URL(request.url).pathname === "/guest") {
      const agent = env.AGENT.getByName("guest");
      const first = await agent.create("guest-root");
      const second = await agent.create("guest-root");
      if (first !== second) throw new Error("STEP guest-create replay failed; recovery: bash scripts/qualify-cloudflare.sh");
      await agent.send("guest-message", "Respond once through the guest facade");
      await agent.send("guest-message", "Respond once through the guest facade");
      const records = await agent.read("conversation");
      if (records.length !== 1 || !records[0].value.output) {
        throw new Error(`STEP guest-turn failed: ${records.length} conversation records; recovery: bash scripts/qualify-cloudflare.sh`);
      }
      const output = JSON.parse(new TextDecoder().decode(await agent.readFile(records[0].value.output))).text;
      if (output !== "workerd guest reply") throw new Error("STEP guest-output failed; recovery: bash scripts/qualify-cloudflare.sh");
      const peer = env.AGENT.getByName("peer");
      await peer.create("peer-root");
      await peer.send("peer-message", "Respond independently");
      if (await env.LEDGER.getByName("suite").pending() !== 0) {
        throw new Error("STEP guest-wake isolation failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      return new Response(JSON.stringify({ guest: "passed", records, output },
        (_, value) => typeof value === "bigint" ? value.toString() : value),
      { headers: { "content-type": "application/json" } });
    }
    return env.SUITE.getByName("conformance").fetch(request);
  },
};
