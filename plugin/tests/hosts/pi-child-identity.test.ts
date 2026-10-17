import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { test } from "bun:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { installedNativeFixture } from "./pi-native-fixture.ts";
import { invokePi } from "./pi-fixture.ts";

const execute = promisify(execFile);
const provider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  let session;
  pi.on("session_start",(_event,ctx)=>{session=ctx.sessionManager.getSessionId();});
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  faux.setResponses([
    context=>{
      const bytes=JSON.stringify(context.messages);
      if(bytes.includes("ROOT_ONLY_NATIVE_INSTRUCTION") || !bytes.includes("CHILD_ONLY_NATIVE_INSTRUCTION")) {
        throw new Error("the fresh child did not receive its own native context");
      }
      if(session!==process.env.ACYCLIC_EXPECTED_SESSION) throw new Error("native session was substituted");
      return fauxAssistantMessage(fauxToolCall("write",{
        path:"fresh-child.txt",text:"fresh native child",media_type:"text/plain",display_name:"fresh-child.txt",
      },{id:"child-write"}),{stopReason:"toolUse"});
    },
    context=>{
      if(context.messages.filter(message=>message.role==="toolResult").at(-1)?.isError) throw new Error("child write failed");
      return fauxAssistantMessage(fauxToolCall("acyclic_inspect",{task:process.env.ACYCLIC_EXPECTED_TASK},{id:"child-inspect"}),{stopReason:"toolUse"});
    },
    context=>{
      const result=context.messages.filter(message=>message.role==="toolResult").at(-1);
      if(!result || result.isError) throw new Error("child inspection failed");
      const inspected=JSON.parse(result.content.filter(part=>part.type==="text").map(part=>part.text).join(""));
      return fauxAssistantMessage(JSON.stringify({marker:"fresh-child-bound",session,inspected}));
    },
  ]);
  pi.registerProvider(faux.provider);
}
`;

test("a fresh Pi CLI uses its declared child session and cannot fall back to a root task", async () => {
  const directory=mkdtempSync(fileURLToPath(new URL(".pi-child-identity-",import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    fixture=await installedNativeFixture(directory,provider,{});
    await fixture.start();
    const cwd=join(directory,"child-view");
    mkdirSync(cwd);
    const child=SessionManager.create(cwd,join(directory,"child-sessions"));
    child.appendMessage({role:"user",content:"CHILD_ONLY_NATIVE_INSTRUCTION",timestamp:2});
    assert.notEqual(child.getSessionId(),fixture.manager.getSessionId());
    const target={host:"pi",session_id:child.getSessionId(),caller:null};
    const fork=JSON.parse((await fixture.command(["__hook","pi","subagent_start"],JSON.stringify({
      sessionId:fixture.manager.getSessionId(),agentId:"native-worker-label",childSession:target,
    }))).stdout);
    assert.equal(typeof fork.Succeeded,"string");
    const binding=join(directory,"child-binding.json");
    writeFileSync(binding,JSON.stringify({provider:"local",workspace:fixture.project,task:fork.Succeeded}));
    const environment={...fixture.environment,ACYCLIC_HARNESS_BINDING:binding,
      ACYCLIC_EXPECTED_TASK:fork.Succeeded,ACYCLIC_EXPECTED_SESSION:child.getSessionId()};
    const wrong=execute(process.env.ACYCLIC_BINARY!,["__hook","pi","session_start"],{
      cwd,env:environment,windowsHide:true,timeout:3000,encoding:"utf8",maxBuffer:65536,
    });
    wrong.child.stdin?.end(JSON.stringify({sessionId:"wrong-native-session",cwd}));
    await assert.rejects(wrong,error=>{
      assert.equal((error as {code:unknown}).code,2);
      assert.ok(String((error as {stdout:unknown}).stdout).includes("admitted task binding"));
      return true;
    });
    const answer=await invokePi([
      "--print","--mode","text","--session",child.getSessionFile()!,
      "--provider","acyclic-native-qualification","--model","probe",
      "--no-extensions","--no-mcp","--no-builtin-tools","-e",fixture.extension,"-e",fixture.provider,
      "--","Run the child write and inspect its admitted task.",
    ],cwd,directory,environment);
    const reported=JSON.parse(answer.stdout.trim());
    assert.equal(reported.marker,"fresh-child-bound");
    assert.equal(reported.session,child.getSessionId());
    assert.equal(reported.inspected.task,fork.Succeeded);
    assert.equal(reported.inspected.admission.parent,fixture.environment.ACYCLIC_NATIVE_TASK);
    assert.ok(!readFileSync(child.getSessionFile()!,"utf8").includes("ROOT_ONLY_NATIVE_INSTRUCTION"));
    assert.ok(!existsSync(join(fixture.project,"fresh-child.txt")));
    assert.ok(!existsSync(join(cwd,"fresh-child.txt")));
    const call=async (id:string,name:string,input:unknown)=>JSON.parse((await fixture!.command(["__hook","pi","tool_call"],JSON.stringify({
      sessionId:fixture!.manager.getSessionId(),toolCallId:id,toolName:name,input,
    }))).stdout);
    assert.equal((await call("wait-native-child","acyclic_wait",{child:fork.Succeeded})).isError,false);
    const diff=await call("diff-native-child","fs",{command:"diff agents/"+fork.Succeeded});
    assert.equal(diff.isError,false);
    assert.equal(diff.details.result.Filesystem.Data.kind,"diff");
    assert.ok(diff.details.result.Filesystem.Data.value.fileChanges>0);
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
},8000);
