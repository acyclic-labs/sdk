// The shipping CLI composes the shared workspace commands. These operations
// are deliberately outside the model's child-only diff/merge tool surface.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { readFile, writeFile } from "node:fs/promises";
import { dirname, isAbsolute, join } from "node:path";
import { startWorkspaceOwner } from "../../plugin/tests/hosts/workspace-owner.mjs";

export async function workspaceFs(processes, input, fixture, capture) {
  const binary = fixture.env.E2E_PLUGIN_BINARY;
  assert.ok(binary && isAbsolute(binary),
    "step=admit filesystem CLI; recovery=set E2E_PLUGIN_BINARY to the matching prebuilt absolute executable");
  assert.ok(input.steps?.length, "step=admit filesystem operations; recovery=select a nonempty workspace-fs case");
  const { workspace, env } = fixture;
  let step = "start workspace owner";
  const run = async (args, body) => {
    const options = { cwd: workspace, env };
    const result = await (body === undefined ? processes.runOwned(binary, args, options)
      : processes.run(binary, args, options, JSON.stringify(body)));
    capture({ args, ...result });
    return result;
  };
  try {
    await startWorkspaceOwner({ scope: processes, binary: fixture.binary, workspace,
      state: join(dirname(workspace), "state"), root: dirname(workspace), env, capture });
    step = "open workspace through the shipping API";
    const opened = await run(["__workspace"], { kind: "open", host: "codex", session: randomUUID() });
    assert.equal(opened.code, 0, opened.stderr);
    const { task, cwd } = JSON.parse(opened.stdout);
    assert.equal(cwd, workspace, "filesystem operations must use the original workspace view");
    for (const [index, operation] of input.steps.entries()) {
      step = `filesystem operation ${index}`;
      if (operation.write) {
        await writeFile(join(workspace, operation.write.path), operation.write.content);
      } else if (operation.absent) {
        await assert.rejects(readFile(join(workspace, operation.absent)), { code: "ENOENT" });
      } else {
        const result = await run(["--task", task, "fs", ...operation.args]);
        if (operation.error) {
          assert.notEqual(result.code, 0, "the divergent merge must fail before resolution");
          assert.ok(result.stderr.includes(operation.error), result.stderr);
        } else {
          assert.equal(result.code, 0, result.stderr);
          if (operation.text !== undefined)
            assert.equal(JSON.parse(result.stdout).Filesystem.Data.value.text, operation.text);
        }
      }
    }
  } catch (cause) {
    throw new Error(`step=${step}: ${cause.message}; recovery=node scripts/graphcoder-e2e/run.mjs GRAPHCODER MOCK_MODEL ${fixture.id}`, { cause });
  }
}
