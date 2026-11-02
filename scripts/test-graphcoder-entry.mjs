#!/usr/bin/env node
// Shipping binaries, original PTY custody and ordinary native configuration.
// No provider response is fabricated: a first frame needs no model request.
import assert from "node:assert/strict";
import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:net";
import { isAbsolute, join } from "node:path";
import { Processes, OutputWatch } from "./graphcoder-e2e/process.mjs";
import { TerminalFrames } from "./graphcoder-e2e/frames.mjs";
import { environment, scratchDirectory } from "./graphcoder-e2e/scratch.mjs";
import { commandCancellation, withOwnedCase } from "./graphcoder-e2e/suite.mjs";
import { Terminal, screenText } from "./graphcoder-pty/screen.mjs";

const recovery = "node scripts/test-graphcoder-entry.mjs ABSOLUTE_GRAPHCODER ABSOLUTE_ACYCLIC";
const cancellation = commandCancellation();
let directory;
try {
  const [graphcoder, acyclic, ...extra] = process.argv.slice(2);
  assert.ok(graphcoder && acyclic && !extra.length && isAbsolute(graphcoder) && isAbsolute(acyclic),
    "both shipping executable paths must be absolute");
  directory = scratchDirectory("graphcoder-entry-");
  for (const [name, binary, args, terminal] of [
    ["bare", graphcoder, [], true],
    ["preset", acyclic, ["run", "--template", "graphcoder"], true],
    ["piped", graphcoder, [], false],
  ]) {
    const root = join(directory, name), settings = join(root, "settings"), workspace = join(root, "workspace");
    mkdirSync(workspace, { recursive: true });
    // Retain only tool discovery and the original native terminal custodians.
    const inherited = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
      /^(PATH|PATHEXT|SYSTEMROOT|WINDIR|COMSPEC|E2E_PROCESS_HOST|E2E_CONPTY_HOST)$/i.test(key)));
    const env = environment(settings, inherited);
    const processes = new Processes();
    let running, failure, requests = 0;
    const provider = createServer(socket => {
      requests++;
      failure ??= new Error("first-frame admission unexpectedly contacted the model provider");
      socket.destroy();
      running?.killExecutable();
    });
    const screen = new Terminal({ cols: 80, rows: 24, allowProposedApi: true });
    const frames = new TerminalFrames(screen);
    let parsed = Promise.resolve(), firstFrame = false;
    const raw = new OutputWatch(["\x1b[?25h"]);
    try {
      await new Promise((resolve, reject) => {
        provider.once("error", reject);
        provider.listen(0, "127.0.0.1", resolve);
      });
      env.ACYCLIC_MODEL = "local/first-frame";
      env.ACYCLIC_CATALOG = join(root, "catalog.json");
      writeFileSync(env.ACYCLIC_CATALOG, JSON.stringify({ local: {
        adapter: "responses", authentication: "optional",
        base_url: `http://127.0.0.1:${provider.address().port}`,
        models: { "first-frame": { capacity: { context_tokens: 8192, output_tokens: 1024 },
          features: { tools: true, images: false } } },
        token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
      } }));
      const { run: result } = await withOwnedCase({ processes, signal: cancellation.signal, run: async () => {
        running = terminal ? processes.startTerminal(binary, args, { cwd: workspace, env })
          : processes.startOwned(binary, args, { cwd: workspace, env });
        // Device-query replies come from the actual terminal parser.
        const replies = screen.onData(data => running.input.write(data));
        try {
          running.child.stdout.on("data", bytes => {
            raw.write(bytes);
            parsed = frames.write(bytes, () => {
              if (firstFrame || failure) return;
              if (screenText(screen).includes("GraphCoder") && raw.has("\x1b[?25h")) {
                firstFrame = true;
                if (terminal) running.input.write("\x04");
                else { failure = new Error("piped invocation opened the TUI"); running.killExecutable(); }
              }
            });
          });
          if (!terminal) running.input.end();
          // The same output producer owns this join. Early EOF reaches the
          // failed first-frame assertion instead of leaving a separate wait.
          const result = await running.closed;
          await parsed;
          return result;
        } finally { replies.dispose(); }
      } });
      if (failure) throw failure;
      assert.equal(result.code, 0, `${name}: executable failed before physical retirement`);
      assert.equal(result.signal, null, `${name}: executable was interrupted`);
      assert.equal(firstFrame, terminal, `${name}: first-frame contract failed`);
      assert.equal(requests, 0, `${name}: first frame dispatched model work`);
      if (!terminal) assert.ok(!result.stdout.includes("\x1b["), "piped invocation emitted terminal controls");
      console.log(`${name}: ${terminal ? "first TUI frame and clean exit" : "headless EOF"}`);
    } finally {
      await processes.close();
      frames.dispose(); screen.dispose();
      if (provider.listening) await new Promise((resolve, reject) => provider.close(error => error ? reject(error) : resolve()));
    }
  }
} catch (cause) {
  console.error(`step=graphcoder-entry: ${cause.message}; recovery command: ${recovery}`);
  process.exitCode = 1;
} finally {
  cancellation.dispose();
  if (directory) rmSync(directory, { recursive: true, force: true });
}
