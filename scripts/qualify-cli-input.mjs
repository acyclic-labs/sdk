// The shipping entry points with pipes, through the shared selected-process
// report owner. No provider request or fabricated response participates.
import assert from "node:assert/strict";
import { mkdirSync, writeFileSync } from "node:fs";
import { isAbsolute, join } from "node:path";
import { Processes } from "./graphcoder-e2e/process.mjs";
import { stageArtifacts } from "./graphcoder-e2e/artifacts.mjs";
import { environment, scratchDirectory } from "./graphcoder-e2e/scratch.mjs";
import { commandCancellation, withOwnedCase } from "./graphcoder-e2e/suite.mjs";

const recovery = "node scripts/qualify-cli-input.mjs ABSOLUTE_ACYCLIC ABSOLUTE_GRAPHCODER (or --build ABSOLUTE_CARGO)";
const processes = new Processes();
const cancellation = commandCancellation();
try {
  const root = scratchDirectory("cli-input-");
  const [first, second, ...extra] = process.argv.slice(2);
  assert.ok(first && second && !extra.length && isAbsolute(second)
    && (first === "--build" || isAbsolute(first)), "select the compiler or both shipping executables by absolute path");
  const result = await withOwnedCase({ processes, signal: cancellation.signal, prepare: async () => {
    let acyclic = first, graphcoder = second;
    if (first === "--build") {
      const built = await processes.run(second, ["build", "--locked", "-p", "acyclic-plugin",
        "--bin", "acyclic", "--bin", "graphcoder", "--message-format=json"]);
      assert.equal(built.code, 0, built.stderr);
      const { artifacts } = await stageArtifacts(["acyclic", "graphcoder"].map(name => ({
        name, target: name, test: false, records: built.stdout,
      })), join(root, "artifacts"));
      acyclic = artifacts.acyclic.path;
      graphcoder = artifacts.graphcoder.path;
    }
    return { acyclic, graphcoder };
  }, run: async ({ acyclic, graphcoder }) => {
    for (const [index, binary, args] of [[0, graphcoder, []], [1, acyclic, ["run", "--template", "graphcoder"]]]) {
      const workspace = join(root, `workspace-${index}`);
      mkdirSync(workspace);
      const env = environment(join(root, `settings-${index}`));
      env.ACYCLIC_LOG = "off";
      const catalog = join(workspace, "catalog.json");
      writeFileSync(catalog, JSON.stringify({ "graphcoder-qual": {
        adapter: "responses", authentication: "optional", base_url: "http://127.0.0.1:0",
        models: { tiny: { capacity: { context_tokens: 8192, output_tokens: 1024 },
          features: { tools: true, images: false } } },
        max_event_bytes: 65536,
        token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
      } }));
      const arguments_ = ["--workspace", workspace, "--settings-root", env.HOME,
        "--catalog", catalog, "--model", "graphcoder-qual/tiny", ...args];
      // Prove setup through the shipping event output before attributing an
      // absent report to text input. Empty JSONL admits no model work.
      const setup = await processes.startReported(binary, [...arguments_,
        "--operation", "00000000-0000-0000-0000-000000000001", "--input-format", "jsonl", "--output-format", "jsonl"],
      { cwd: workspace, env });
      setup.input.end();
      const prepared = await setup.closed;
      assert.equal(prepared.code, 0, `step=cli-input-setup ${JSON.stringify(prepared)}`);
      assert.equal(prepared.signal, null, JSON.stringify(prepared));
      for (const input of ["unbound task\n", "", " \n\n"]) {
        const running = await processes.startReported(binary, arguments_, { cwd: workspace, env });
        // The refusal oracle requires the selected launch's admitted channel.
        // EOF/owner loss rejects this same wait; no readiness probe is added.
        await running.reportReady;
        running.input.end(input);
        const actual = await running.closed;
        assert.equal(actual.stdout, "", JSON.stringify(actual));
        assert.equal(actual.signal, null, JSON.stringify(actual));
        if (input.trim()) {
          assert.notEqual(actual.code, 0, JSON.stringify(actual));
          const failure = actual.termination?.execution_failure;
          assert.equal(failure?.phase, "execution", JSON.stringify(actual));
          assert.match(failure.cause, /text input: no task or pending approval/);
          assert.match(failure.cause, /recovery: use acyclic run/);
        } else assert.equal(actual.code, 0, JSON.stringify(actual));
        assert.equal(processes.children.size, 0, "the original process must physically close");
      }
    }
  } });
  console.log(JSON.stringify({ status: "pass", launches: 8, seconds: result.seconds,
    preparation_seconds: result.preparation_seconds, root }));
} catch (error) {
  console.error(`step=cli-input failed: ${error.stack ?? error}; recovery=${recovery}`);
  process.exitCode = 1;
} finally {
  cancellation.dispose();
  await processes.close();
}
