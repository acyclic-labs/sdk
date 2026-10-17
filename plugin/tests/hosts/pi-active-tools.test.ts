import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider=`
import { fauxProvider, fauxAssistantMessage, fauxToolCall, getCurrentTools } from "@earendil-works/pi-ai";
import { writeFileSync } from "node:fs";
import { Type } from "typebox";
export default function(pi) {
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  pi.registerTool({
    name:"foreign_host_echo",label:"Native additive tool",description:"Execute in the native host",
    parameters:Type.Object({text:Type.String()}),
    async execute(_id,input) {
      writeFileSync(process.env.ACYCLIC_FOREIGN_EXECUTED,input.text,{flag:"wx"});
      return {content:[{type:"text",text:input.text}],details:{executor:"native-host"}};
    },
  });
  let offered;
  faux.setResponses([
    context=>{
      offered=getCurrentTools(context.messages).map(tool=>tool.name).sort();
      return fauxAssistantMessage(fauxToolCall("write",{
        path:"activated.txt",text:"first model turn",media_type:"text/plain",display_name:"activated.txt"
      },{id:"activated-write"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError) throw new Error("replacement write did not execute");
      return fauxAssistantMessage(fauxToolCall("foreign_host_echo",{
        text:"additive host execution",
      },{id:"foreign-host-call"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError || result.toolCallId !== "foreign-host-call" ||
        result.content.filter(part=>part.type === "text").map(part=>part.text).join("") !== "additive host execution") {
        throw new Error("native additive tool did not execute or its result was replaced");
      }
      return fauxAssistantMessage(fauxToolCall("acyclic_inspect",{
        task:process.env.ACYCLIC_NATIVE_TASK,
      },{id:"inspect-original-task"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role === "toolResult").at(-1);
      if (!result || result.isError) throw new Error("original task inspection did not execute");
      const inspected=JSON.parse(result.content.filter(part=>part.type === "text").map(part=>part.text).join(""));
      return fauxAssistantMessage(JSON.stringify({marker:"replacement-active",offered,inspected}));
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

test("pinned Pi activates harness tools on its first turn and preserves native additive execution",async () => {
  const directory=mkdtempSync(fileURLToPath(new URL(".pi-active-",import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    const foreign=join(directory,"foreign-executed.txt");
    fixture=await installedNativeFixture(directory,provider,{ACYCLIC_FOREIGN_EXECUTED:foreign});
    const catalog=JSON.parse((await fixture.command(["__catalog"])).stdout);
    for (const tool of catalog.tools) {
      for (const field of ["caller","callerproof","caller_proof","signer","environment","tasklease","approval_digest"]) {
        assert.ok(!Object.hasOwn(tool.input_schema.properties,field),`${tool.name} must not expose ${field}`);
      }
    }
    await fixture.start();
    const answer=await fixture.run("Write activated.txt through the available write tool.");
    assert.ok(answer.stdout.includes("replacement-active"),answer.stdout);
    const report=JSON.parse(answer.stdout.trim());
    assert.deepEqual(report.offered,[...catalog.tools.map(tool=>tool.name),"foreign_host_echo"].sort(),"the pinned native loader must expose the complete harness catalog and retain additive tools on its first turn");
    assert.equal(report.inspected.task,fixture.environment.ACYCLIC_NATIVE_TASK);
    assert.equal(report.inspected.admission.parent,null);
    assert.equal(readFileSync(join(fixture.project,"activated.txt"),"utf8"),"first model turn");
    assert.equal(readFileSync(foreign,"utf8"),"additive host execution");
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
});
