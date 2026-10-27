// Cloudflare transport only: portable lifecycle, replay and expiry
// belong to the Rust Machines core. Uses the native Container API and Files 1.0.
import { DurableObject } from "cloudflare:workers";
import { Files } from "@cloudflare/sandbox";

interface Env { MACHINES: DurableObjectNamespace<Machine>; TRANSPORT_TOKEN?: string }
type Event = { stream: "stdout" | "stderr"; value: number[] } | { stream: "exit"; value: number } | { stream: "error"; value: string };
const MAXIMUM_LIFETIME_MS = 6 * 60 * 60 * 1000;

export class Machine extends DurableObject<Env> {
  readonly container: Container;
  readonly files: Files;
  readonly commands = new Map<string, AbortController>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    if (ctx.container === undefined) throw new Error("Container binding missing; recovery: deploy wrangler.jsonc with its containers binding");
    this.container = ctx.container;
    this.files = new Files(this.container);
    if (this.container.running) ctx.blockConcurrencyWhile(() => this.container.setInactivityTimeout(MAXIMUM_LIFETIME_MS));
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const segments = url.pathname.split("/").filter(Boolean);
    const action = segments[2];
    if (action === undefined) {
      if (request.method === "GET") return Response.json({ running: this.container.running });
      if (request.method === "DELETE") {
        for (const control of this.commands.values()) control.abort();
        if (this.container.running) await this.container.destroy();
        return new Response(null, { status: 204 });
      }
      if (request.method === "POST") {
        const body = await request.json<{ image: string; snapshot?: ContainerSnapshot; env: Record<string, string>; internet: boolean }>();
        if (body.image !== "workspace" || body.internet !== false) return failure(400, "Container create admission failed; recovery: use registered image workspace with blocked internet");
        if (this.container.running) return failure(409, "Container already running; recovery: recover the existing create operation");
        this.container.start({ ...(body.snapshot === undefined ? { image: this.container.images.workspace } : { containerSnapshot: body.snapshot }), instance: "lite", enableInternet: false, env: body.env });
        // An explicit Machines contract owns lifetime. Provider idle shutdown must
        // not silently replace it while a quiet command is running.
        await this.container.setInactivityTimeout(MAXIMUM_LIFETIME_MS);
        // Exec is a readiness barrier, with no user command side effects.
        const ready = await this.container.exec(["/bin/true"]);
        if (await ready.exitCode !== 0) return failure(503, "Container readiness failed; recovery: inspect the registered image");
        return new Response(null, { status: 201 });
      }
    }
    if (!this.container.running) return failure(404, "Container stopped; recovery: inspect its Machines observation");
    if (action === "snapshot" && request.method === "POST") {
      return Response.json(await this.container.snapshotContainer({}));
    }
    if (action === "file") {
      const path = url.searchParams.get("path");
      if (path === null || !path.startsWith("/") || path.includes("\0")) return failure(400, "File path admission failed; recovery: use an absolute guest path");
      if (request.method === "GET") return this.files.readFile(path);
      if (request.method === "PUT") {
        await this.files.writeFile(path, request.body ?? new Uint8Array());
        return new Response(null, { status: 204 });
      }
    }
    const id = segments[3];
    if (action === "exec" && id !== undefined && /^[a-f0-9-]{36}$/.test(id)) {
      if (request.method === "POST") {
        if (this.commands.has(id)) return failure(409, "Exec identity already used; recovery: inspect output without redispatching");
        const body = await request.json<{ argv: string[] }>();
        if (!Array.isArray(body.argv) || body.argv.length === 0 || body.argv.some(arg => typeof arg !== "string" || arg.includes("\0"))) return failure(400, "Exec argv admission failed; recovery: use a nonempty array of NUL-free strings");
        const control = new AbortController();
        this.commands.set(id, control);
        let process: Awaited<ReturnType<Container["exec"]>>;
        try {
          process = await this.container.exec(body.argv, { signal: control.signal });
        } catch {
          this.commands.delete(id);
          return failure(503, "Container exec dispatch failed; recovery: inspect the machine without redispatching the command");
        }
        const encoder = new TextEncoder();
        const { readable, writable } = new TransformStream<Uint8Array, Uint8Array>();
        const writer = writable.getWriter();
        let finished = false;
        const stop = () => { if (!finished) control.abort(); };
        void writer.closed.catch(stop);
        const emit = (event: Event) => writer.write(encoder.encode(JSON.stringify(event) + "\n"));
        const pump = async (source: ReadableStream<Uint8Array> | null, stream: "stdout" | "stderr") => {
          if (source === null) throw new Error("Container stream missing; recovery: inspect the native exec configuration");
          for await (const data of source) {
            for (let start = 0; start < data.length; start += 16_384) await emit({ stream, value: Array.from(data.subarray(start, start + 16_384)) });
          }
        };
        this.ctx.waitUntil((async () => {
          try {
            await Promise.all([pump(process.stdout, "stdout"), pump(process.stderr, "stderr")]);
            const code = await process.exitCode;
            finished = true;
            await emit({ stream: "exit", value: code });
            await writer.close();
          } catch (error) {
            console.error("Container execution failed; recovery: inspect the machine without replaying the command", error);
            stop();
            await writer.abort(new Error("Container execution failed; recovery: inspect the machine without replaying the command"));
          } finally {
            this.commands.delete(id);
          }
        })());
        return new Response(readable, { headers: { "content-type": "application/x-ndjson", "cache-control": "no-store" } });
      }
    }
    return failure(404, "Transport route missing; recovery: use the matching Rust adapter and Worker version");
  }
}

function failure(status: number, message: string): Response { return new Response(message, { status }); }

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const url = new URL(request.url);
    const name = url.pathname.split("/")[2];
    if (env.TRANSPORT_TOKEN === undefined && !["127.0.0.1", "localhost", "[::1]"].includes(url.hostname)) return failure(503, "Hosted transport token missing; recovery: run npx wrangler secret put TRANSPORT_TOKEN before deploying the Worker");
    if (!/^acyclic-[a-f0-9-]{36}-[a-f0-9-]{36}-[0-9]+$/.test(name ?? "")) return failure(400, "Machine name admission failed; recovery: use an identity created by RemoteMachines");
    if (env.TRANSPORT_TOKEN !== undefined && request.headers.get("authorization") !== `Bearer ${env.TRANSPORT_TOKEN}`) return failure(403, "Transport authorization failed; recovery: supply the configured transport token");
    return env.MACHINES.getByName(name).fetch(request);
  },
} satisfies ExportedHandler<Env>;
