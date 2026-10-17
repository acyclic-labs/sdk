import assert from "node:assert/strict";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

const provider = `
import { readFileSync, appendFileSync } from "node:fs";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function(pi) {
  const native=Boolean(process.env.ACYCLIC_HARNESS_BINDING);
  const task=native ? JSON.parse(readFileSync(process.env.ACYCLIC_HARNESS_BINDING,"utf8")).task : null;
  let session;
  pi.on("session_start",(_event,ctx)=>{
    session=ctx.sessionManager.getSessionId();
    if(native) appendFileSync(process.env.ACYCLIC_NATIVE_ORACLE,JSON.stringify({session,task,entries:ctx.sessionManager.getBranch()})+"\\n");
  });
  const faux=fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  const call=(name,args,id)=>fauxAssistantMessage(fauxToolCall(name,args,{id}),{stopReason:"toolUse"});
  const result=context=>{
    const value=context.messages.filter(message=>message.role==="toolResult").at(-1);
    if(!value || value.isError) throw new Error("native tool failed: "+JSON.stringify(value));
    return JSON.parse(value.content.filter(part=>part.type==="text").map(part=>part.text).join(""));
  };
  let child;
  faux.setResponses(native ? [
    context=>{
      const bytes=JSON.stringify(context.messages);
      if(bytes.includes("ROOT_ONLY_NATIVE_INSTRUCTION") !== Boolean(process.env.ACYCLIC_NATIVE_CUT) || !bytes.includes("CHILD_ONLY_NATIVE_INSTRUCTION")) {
        throw new Error("native launch substituted the selected fresh context");
      }
      return call("write",{path:"automatic-child.txt",content:"automatic native child"},"child-write");
    },
    context=>{result(context);return call("acyclic_inspect",{task},"child-inspect");},
    context=>{
      const inspected=result(context);
      if(inspected.task!==task || !inspected.admission.parent) throw new Error("native child lost its admitted identity");
      return fauxAssistantMessage("automatic-child-complete");
    },
  ] : [
    ()=>call("acyclic_spawn",{id:"automatic-child",prompt:"CHILD_ONLY_NATIVE_INSTRUCTION",...(process.env.ACYCLIC_NATIVE_CUT ? {context:"native_cut"} : {})},"native-spawn"),
    context=>{child=result(context);return call("acyclic_wait",{child},"native-wait");},
    context=>{
      const outcome=result(context);
      if(outcome.success!==true) throw new Error("owned native execution did not complete: "+JSON.stringify(outcome));
      return call("fs",{command:"diff agents/"+child},"native-diff");
    },
    context=>{result(context);return fauxAssistantMessage(JSON.stringify({marker:"automatic-native-launch",child}));},
  ]);
  pi.registerProvider(faux.provider);
}
`;

for(const selected of [false,true]) test(`the installed Pi spawn tool launches and reaps its own ${selected?"selected":"fresh"} native child`, async () => {
  const directory=mkdtempSync(fileURLToPath(new URL(".pi-native-launch-",import.meta.url)));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  try {
    const oracle=join(directory,"native-oracle.jsonl");
    fixture=await installedNativeFixture(directory,provider,{ACYCLIC_NATIVE_ORACLE:oracle,...(selected?{ACYCLIC_NATIVE_CUT:"1"}:{})});
    await fixture.start();
    const answer=await fixture.run("ROOT_ONLY_NATIVE_INSTRUCTION Launch and wait for the automatic native child.");
    const reported=JSON.parse(answer.stdout.trim());
    assert.equal(reported.marker,"automatic-native-launch");
    const children=readFileSync(oracle,"utf8").trim().split("\n").map(line=>JSON.parse(line));
    assert.equal(children.length,1,"native custody launched the child more than once");
    assert.equal(children[0].task,reported.child);
    assert.match(children[0].session,/^[0-9a-f-]{36}$/);
    assert.notEqual(children[0].session,fixture.manager.getSessionId());
    const inherited=JSON.stringify(children[0].entries);
    assert.equal(inherited.includes("ROOT_ONLY_NATIVE_INSTRUCTION"),selected);
    assert.ok(!inherited.includes("native-spawn"),"the invoking assistant entered the child seed");
    assert.ok(!existsSync(join(fixture.project,"automatic-child.txt")));
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]});
  } finally {
    await fixture?.close();
    rmSync(directory,{recursive:true});
  }
},8000);
