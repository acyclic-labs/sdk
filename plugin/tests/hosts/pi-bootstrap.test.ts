import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { invokePi } from "./pi-fixture.ts";
const provider = `
import { fauxProvider, fauxAssistantMessage, getCurrentTools, Type } from "@earendil-works/pi-ai";
export default function (pi) {
  const faux = fauxProvider({
    provider: "acyclic-qualification", models: [{ id: "probe" }],
  });
  faux.setResponses([context => fauxAssistantMessage(JSON.stringify({
    tools: getCurrentTools(context.messages).map(tool => tool.name).sort(),
  }))]);
  pi.registerProvider(faux.provider);
  pi.registerTool({
    name: "qualification_probe", label: "Qualification probe",
    description: "Inert bootstrap qualification tool", parameters: Type.Object({}),
    async execute() { return { content: [{ type: "text", text: "probe" }], details: {} }; },
  });
}
`;

async function bootstrap(failingExtension: boolean) {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-bootstrap-", import.meta.url)));
  try {
    const project = join(directory, "project");
    const agent = join(directory, "agent");
    for (const extensions of [join(project, ".pi", "extensions"), join(agent, "extensions")]) {
      mkdirSync(extensions, { recursive: true });
      writeFileSync(join(extensions, "unexpected.ts"),
        'throw new Error("ACYCLIC_UNEXPECTED_DISCOVERY");');
    }
    const fixture = join(directory, "provider.ts");
    writeFileSync(fixture, provider);
    const args = [
      "--print", "--mode", "text", "--no-session", "--provider", "acyclic-qualification",
      "--model", "probe", "--no-extensions", "--no-mcp", "--no-builtin-tools", "-e", fixture,
    ];
    if (failingExtension) {
      const failed = join(directory, "failed.ts");
      writeFileSync(failed, 'throw new Error("ACYCLIC_EXPLICIT_EXTENSION_FAILED");');
      args.push("-e", failed);
    }
    args.push("Report the enabled tools.");
    return await invokePi(args, project, directory);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
}

test("installed Pi disables discovery, MCP and built-ins while loading the explicit extension", async () => {
  const result = await bootstrap(false);
  assert.doesNotMatch(result.stderr, /ACYCLIC_UNEXPECTED_DISCOVERY/);
  assert.deepEqual(JSON.parse(result.stdout.trim()), { tools: ["qualification_probe"] });
});

test("installed Pi aborts when an explicit extension fails to load", async () => {
  await assert.rejects(() => bootstrap(true), (error: unknown) => {
    assert.ok(error instanceof Error && "code" in error && "stdout" in error && "stderr" in error);
    assert.equal(error.code, 1);
    assert.equal(error.stdout, "");
    assert.match(String(error.stderr), /ACYCLIC_EXPLICIT_EXTENSION_FAILED/);
    assert.doesNotMatch(String(error.stderr), /ACYCLIC_UNEXPECTED_DISCOVERY/);
    return true;
  });
});
