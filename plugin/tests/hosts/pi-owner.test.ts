import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { test } from "bun:test";
import { installedNativeFixture } from "./pi-native-fixture.ts";

async function project(fixture: Awaited<ReturnType<typeof installedNativeFixture>>, toolCallId: string) {
  const answer=JSON.parse((await fixture.command(["__hook","pi","tool_call"],JSON.stringify({
    sessionId:fixture.manager.getSessionId(),toolCallId,toolName:"bash",
    input:{command:"printf owner-projection"},
  }))).stdout);
  assert.equal(answer.isError,false);
  if (process.platform === "linux") assert.ok(fixture.resources().mounts.length > 0,
    "the shutdown probe must own a real lazy volume projection");
}

test("the installed Pi fixture reconnects through task files and awaits owner shutdown",async () => {
  const directory=mkdtempSync(join(tmpdir(),"acyclic-pi-owner-"));
  let fixture: Awaited<ReturnType<typeof installedNativeFixture>> | undefined;
  let failure: unknown;
  let phase="install";
  let crashed: unknown;
  try {
    const bash=Bun.which("bash");
    assert.ok(bash,"the native projection probe requires bash");
    fixture=await installedNativeFixture(directory,"export default function () {}",{PATH:dirname(bash)});
    phase="initial attach";
    await fixture.start();
    const original=fixture.environment.ACYCLIC_NATIVE_TASK;
    const input=JSON.stringify({sessionId:fixture.manager.getSessionId(),toolCallId:"original-write",
      toolName:"write",input:{path:"written.txt",text:"original contents",media_type:"text/plain",display_name:"written.txt"}});
    phase="write";
    const first=JSON.parse((await fixture.command(["__hook","pi","tool_call"],input)).stdout);
    assert.equal(first.isError,false);
    assert.equal(readFileSync(join(fixture.project,"written.txt"),"utf8"),"original contents");
    phase="project before crash";
    await project(fixture,"project-before-crash");
    phase="crash";
    await fixture.kill();
    crashed=fixture.resources();
    writeFileSync(join(fixture.project,"written.txt"),"later user contents");
    phase="reattach";
    await fixture.start();
    assert.equal(fixture.environment.ACYCLIC_NATIVE_TASK,original);
    phase="retry";
    assert.deepEqual(JSON.parse((await fixture.command(["__hook","pi","tool_call"],input)).stdout),first);
    assert.equal(readFileSync(join(fixture.project,"written.txt"),"utf8"),"later user contents");
    phase="project before shutdown";
    await project(fixture,"project-before-shutdown");
    phase="shutdown";
    await fixture.close();
    assert.deepEqual(fixture.resources(),{mounts:[],processes:[]},"shutdown must release native mounts and child processes");
  } catch (error) {failure=new Error(`native owner failed during ${phase}`,{cause:error});}
  finally {
    const failures: unknown[] = [];
    try {await fixture?.close();} catch (error) {failures.push(error);}
    try {rmSync(directory,{recursive:true});} catch (error) {failures.push(error);}
    if (failure !== undefined || failures.length) {
      throw new AggregateError(failure === undefined ? failures : [failure,...failures],
        `native owner execution or cleanup failed: ${JSON.stringify({crashed,cleanup:fixture?.resources()})}`);
    }
  }
});
