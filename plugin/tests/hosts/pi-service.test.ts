import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const faux = fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"},{id:"switched"}]});
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("acyclic_agents", {}, {id:"observe-original-task"}), {stopReason:"toolUse"}),
    context => {
      const message = context.messages.filter(message => message.role === "toolResult").at(-1);
      if (!message || message.isError) throw new Error("actual Acyclic task control failed");
      return fauxAssistantMessage(message.content.map(part => part.type === "text" ? part.text : "").join(""));
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

test("installed Pi controls retain one authenticated root across service death and native model switch", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-service-", import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    fixture = await installedNativeFixture(directory, provider, {}, 30_000);
    const { project, manager, command } = fixture;
    writeFileSync(join(project, "user.txt"), "concurrent user content");
    manager.appendMessage({ role: "user", content: "original native service instruction", timestamp: 1 });
    await fixture.start();
    let original: unknown;
    for (const model of ["probe", "switched"]) {
      const result = await fixture.run("Observe only the original task.", model);
      const observed = JSON.parse(result.stdout.trim());
      assert.equal(observed.session, manager.getSessionId());
      assert.equal(observed.tasks.length, 1);
      assert.equal(observed.tasks[0].state, "pending");
      if (original === undefined) original = observed;
      else assert.deepEqual(observed, original, "restart/model selection must not admit another root");
      assert.equal(readFileSync(join(project, "user.txt"), "utf8"), "concurrent user content");
      await fixture.kill();
      if (model === "probe") await fixture.start();
    }
    await fixture.start();
    const observation = original as {tasks:Array<{task:string}>};
    const cancelled = JSON.parse((await command(["__pi", "TaskCancel"], JSON.stringify({
      cwd:project, session_id:manager.getSessionId(), task:observation.tasks[0].task,
    }))).stdout);
    assert.equal(cancelled.tasks[0].state, "outcome");
    assert.equal(cancelled.tasks[0].task, observation.tasks[0].task);
    await fixture.close();
  } finally {
    await fixture?.kill();
    rmSync(directory, {recursive:true});
  }
}, { timeout: 120_000 });
