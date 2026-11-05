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
  const result = await withOwnedCase({ processes, signal: cancellation.signal, run: async () => {
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
        token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 },
      } }));
      for (const input of ["unbound task\n", "", " \n\n"]) {
        const running = await processes.startReported(binary, ["--workspace", workspace,
          "--settings-root", env.HOME, "--catalog", catalog, "--model", "graphcoder-qual/tiny", ...args],
        { cwd: workspace, env });
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
  console.log(JSON.stringify({ status: "pass", launches: 6, seconds: result.seconds, root }));
} catch (error) {
  console.error(`step=cli-input failed: ${error.stack ?? error}; recovery=${recovery}`);
  process.exitCode = 1;
} finally {
  cancellation.dispose();
  await processes.close();
}
