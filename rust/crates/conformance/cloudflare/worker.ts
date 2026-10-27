import { DurableObject } from "cloudflare:workers";
import { initSync, qualifyCloudflare, qualifyCloudflareRpc, appendWakeRecord, spawnAdoptionFixture } from "./generated/acyclic_cloudflare_conformance.js";
import wasm from "./generated/acyclic_cloudflare_conformance_bg.wasm";
import { SqliteJournal } from "../../../../typescript/packages/harness/src/cloudflare-storage.js";
import { openCloudflareAgent, openCloudflareLedger } from "../../../../typescript/packages/harness/src/cloudflare.js";
import { streamLedgerPort } from "../../../../typescript/packages/harness/src/event-ledger.js";
import { initSync as initGuest, WasmReducer, WasmAgentRegistry, type WasmFileRefWire } from "../../../../typescript/packages/harness/generated/wasm/acyclic_harness_wasm.js";
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
  handler_timeout_ms: 30_000n,
};

export class Ledger extends DurableObject {
  private readonly ledger = openCloudflareLedger(this.ctx,
    actor => this.env.AGENT.getByName(actor).wake(), { ...options.stream_limits, commands: 65536 });
  register(actor: string, paths: string[]) { return this.ledger.register(actor, paths); }
  dispatch(actor: string, operation: string, input: Uint8Array) { return this.ledger.dispatch(actor, operation, input); }
  open(operation: string, input: Uint8Array, preceding: Uint8Array) { return this.ledger.open(operation, input, preceding); }
  objectLength(key: Uint8Array) { return this.ledger.objectLength(key); }
  objectRead(key: Uint8Array, maximum: bigint) { return this.ledger.objectRead(key, maximum); }
  objectPut(keys: Uint8Array[], values: Uint8Array[]) { return this.ledger.objectPut(keys, values); }
  alarm() { return this.ledger.alarm(); }
  pending() { return this.ledger.pending(); }
}

export class Agent extends DurableObject {
  private readonly owner = new WasmReducer({ kind: "task", id: "00112233-4455-6677-8899-aabbccddeeff" },
    "do-conformance", new Uint8Array(32).fill(23), []);
  private readonly registry = new WasmAgentRegistry();
  private readonly agent: ReturnType<typeof openCloudflareAgent>;
  private readonly timeout = { started: false, aborted: false };
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    const timeout = this.timeout;
    const hangs = ctx.id.name === "timeout" || ctx.id.name === "cancel";
    this.registry.configureModel({ provider: "conformance", name: "scripted", revision: "1", options: {} },
      async function* (_bytes: Uint8Array, _dispatch: unknown, signal: AbortSignal) {
        if (hangs) {
          timeout.started = true;
          await new Promise<void>(resolve => {
            const aborted = () => { timeout.aborted = true; resolve(); };
            if (signal.aborted) aborted();
            else signal.addEventListener("abort", aborted, { once: true });
          });
          return;
        }
        yield { kind: "content", delta: "workerd guest reply" };
        yield { kind: "completed", metadata: {} };
      }, () => null, () => ({ context_tokens: 131072, output_tokens: 8192 }),
      (bytes: Uint8Array, request_digest: Uint8Array) => ({ request_digest, input_tokens: BigInt(bytes.byteLength),
        fixed_tokens: bytes.byteLength, message_tokens: JSON.parse(new TextDecoder().decode(bytes)).messages.map(() => 0) }),
      { kind: "disabled" });
    this.agent = openCloudflareAgent(ctx, env.LEDGER.getByName("suite"), ctx.id.name!, { ...options, handler_timeout_ms: ctx.id.name === "timeout" ? 1000n : options.handler_timeout_ms }, this.owner, this.registry);
  }
  create(intent: string, brief?: string) { return this.agent.create(intent, brief); }
  adopt(path: string) { return this.agent.adopt(path); }
  send(intent: string, text: string) { return this.agent.send(intent, text); }
  read(stream: "inbox" | "conversation" | "status") { return this.agent.read(stream); }
  pause(paused: boolean) { return this.agent.pause(paused); }
  cancel(intent: string) { return this.agent.cancel(intent); }
  timeoutState() { return this.timeout; }
  readFile(file: WasmFileRefWire) { return this.agent.readFile(file); }
  wake() { return this.agent.wake(); }
  alarm() { return this.agent.wake(); }
}

interface Env {
  SUITE: DurableObjectNamespace<Suite>;
  LEDGER: DurableObjectNamespace<Ledger>;
  AGENT: DurableObjectNamespace<Agent>;
}

export class Suite extends DurableObject {
  private wakeLedger?: ReturnType<typeof openCloudflareLedger>;
  alarm() { return this.wakeLedger?.alarm(); }

  async wakeIsolation(): Promise<void> {
    const delivered: string[] = [];
    const ledger = openCloudflareLedger(this.ctx, async actor => {
      delivered.push(actor);
      if (actor === "a-failing") throw new Error("injected wake delivery failure");
    }, { ...options.stream_limits, commands: 65536 });
    this.wakeLedger = ledger;
    try {
      await ledger.register("a-failing", ["wake/a/inbox", "wake/a/state"]);
      await ledger.register("z-healthy", ["wake/z/inbox", "wake/z/state"]);
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
    await qualifyCloudflare(() => new SqliteJournal(this.ctx.storage, crypto.randomUUID()));
    const sqlite = Date.now() - started;
    await qualifyCloudflareRpc(() => streamLedgerPort(this.env.LEDGER.getByName(crypto.randomUUID()), ""));
    return Response.json({ sqlite, rpc: Date.now() - started - sqlite, stream: "passed", agentHost: "passed" });
  }
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
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
      const adoption = env.AGENT.getByName("invalid-adopt");
      let refused = 0;
      for (const candidate of ["not-an-agent", "harness/agents/ffffffffffffffffffffffffffffffff",
        "harness/agents/ffffffffffffffffffffffffffffffff/children/eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"]) {
        try { await adoption.adopt(candidate); } catch { refused += 1; }
      }
      await adoption.wake();
      if (refused !== 3 || (await adoption.read("inbox")).length !== 0) {
        throw new Error("STEP invalid-adopt refusal failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
      const child = await spawnAdoptionFixture(streamLedgerPort(env.LEDGER.getByName("suite"), ""));
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
      for (let attempt = 0; attempt < 100 && !(await agent.timeoutState()).started; attempt += 1) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }
      if (!(await agent.timeoutState()).started) {
        throw new Error("STEP guest-cancel start failed; recovery: bash scripts/qualify-cloudflare.sh");
      }
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
