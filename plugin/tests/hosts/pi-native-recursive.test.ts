import assert from "node:assert/strict";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { installedNativeFixture, type InstalledNativeFixture } from "./pi-native-fixture.ts";
import { PiOperatorDriver } from "./pi-operator-driver.ts";
import { blake3 } from "@noble/hashes/blake3";
import { openNativeFs, DEFAULT_OBJECT_CACHE_OPTIONS } from "../../../typescript/packages/filesystem/src/native.ts";
import { loadPiContextHelpers } from "./pi-fixture.ts";
function volumeName(volume: unknown): string {
  const canonical = (value: unknown): unknown => Array.isArray(value) ? value.map(canonical)
    : value !== null && typeof value === "object" ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0)
      .map(([key, entry]) => [key, canonical(entry)])) : value;
  return `harness-${Buffer.from(blake3(new TextEncoder().encode(JSON.stringify(canonical(volume))))).toString("hex")}`;
}
const recursiveProvider = `
import { VERSION } from "@earendil-works/pi-coding-agent";
import { fauxProvider, fauxAssistantMessage, fauxToolCall, getCurrentTools } from "@earendil-works/pi-ai";
import { existsSync, statSync, writeFileSync, watch } from "node:fs";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { promisify } from "node:util";
import { join } from "node:path";
export default function(pi) {
  if (VERSION !== "1.1.0") throw new Error("unpinned native Pi");
  let sessionId, role, first = true, concurrentWritten = false;
  pi.on("session_start", (_event, ctx) => { sessionId = ctx.sessionManager.getSessionId(); });
  pi.on("before_agent_start", event => {
    const selected = event.prompt.match(/^ACYCLIC_NATIVE_(ROOT|CHILD_A|CHILD_B|GRANDCHILD|FOREIGN|REOPEN|CONTROL)(?:\\s|$)/);
    if (!selected) throw new Error("missing actual current native invocation instruction");
    role = selected[1];
  });
  pi.on("tool_result", async (event, ctx) => {
    if (event.toolCallId !== "a-edit" || event.isError) return;
    const path = join(ctx.cwd, "child-a.txt");
    const before = statSync(path, {bigint:true});
    const replay = promisify(execFile)(process.env.ACYCLIC_BINARY, ["__pi", "NativeBuiltinEdit"], {
      cwd:ctx.cwd, env:process.env, windowsHide:true, timeout:30000, encoding:"utf8", maxBuffer:1048576,
    });
    replay.child.stdin.end(JSON.stringify({cwd:ctx.cwd,session_id:sessionId,
      caller:JSON.parse(process.env.ACYCLIC_PI_CALLER), operation:"a-edit", path:"child-a.txt", phase:"write",
      content:"child A edited\\n", expected_sha256:createHash("sha256").update("child A original\\n").digest("hex"),
    }));
    await replay;
    const after = statSync(path, {bigint:true});
    if (after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs || after.size !== before.size) {
      throw new Error("lost exact-edit acknowledgement repeated the original native mutation");
    }
  });
  const provider = fauxProvider({provider:"acyclic-native-qualification", models:[{id:"probe"},{id:"switched"}]});
  const response = async (context, _options, _state, model) => {
    if (!role) throw new Error("actual native invocation instruction was not captured");
    if (first) {
      const userTexts = context.messages.filter(message => message.role === "user").map(message =>
        typeof message.content === "string" ? message.content
          : message.content.filter(part => part.type === "text").map(part => part.text).join(""));
      const has = sentinel => userTexts.some(text => text.includes(sentinel));
      const fullContext = JSON.stringify(context.messages);
      const contains = sentinel => fullContext.includes(sentinel);
      if (["CHILD_A", "CHILD_B", "GRANDCHILD"].includes(role) && contains("ROOT_ONLY_NATIVE_INSTRUCTION")) {
        throw new Error("default scratch child received original ROOT-ONLY native context");
      }
      if (role === "CHILD_A" && (!has("CHILD_A_ONLY_NATIVE_INSTRUCTION") || contains("CHILD_B_ONLY_NATIVE_INSTRUCTION"))) {
        throw new Error("child A did not receive independently fresh original child context");
      }
      if (role === "CHILD_B" && (!has("CHILD_B_ONLY_NATIVE_INSTRUCTION") || contains("CHILD_A_ONLY_NATIVE_INSTRUCTION"))) {
        throw new Error("child B did not receive independently fresh original child context");
      }
      if (role === "GRANDCHILD" && (!has("GRANDCHILD_ONLY_NATIVE_INSTRUCTION") || contains("CHILD_A_ONLY_NATIVE_INSTRUCTION"))) {
        throw new Error("default scratch grandchild inherited its parent transcript");
      }
      if (role === "FOREIGN" && (!has("ROOT_ONLY_NATIVE_INSTRUCTION") || has("CHILD_A_ONLY_NATIVE_INSTRUCTION"))) {
        throw new Error("foreign workspace source substituted its transcript for the caller's explicit cut");
      }
      const oracle = process.env.ACYCLIC_NATIVE_ORACLE_DIRECTORY;
      if (!oracle) throw new Error("missing independent native context/process oracle");
      writeFileSync(join(oracle, sessionId + "." + role + (role === "CONTROL" ? "." + model.id : "") + ".json"), JSON.stringify({
        role, sessionId, pid: process.pid, ppid: process.ppid, model: model.id, messages: context.messages,
      }), {flag:"wx"});
      first = false;
      if (role === "CHILD_B" && process.env.ACYCLIC_NATIVE_PAUSE) {
        const release = join(oracle, sessionId + ".release");
        await new Promise((resolve, reject) => {
          const watcher = watch(oracle, () => {
            if (existsSync(release)) { clearTimeout(timeout); watcher.close(); resolve(); }
          });
          const timeout = setTimeout(() => { watcher.close(); reject(new Error("native child cut was never released")); }, 30000);
          if (existsSync(release)) { clearTimeout(timeout); watcher.close(); resolve(); }
        });
      }
    }
    const results = context.messages.filter(message => message.role === "toolResult");
    const done = id => results.some(message => message.toolCallId === id);
    for (const result of results) {
      if (result.isError && !result.toolCallId.startsWith("deny-")) {
        throw new Error("actual production operation failed: " + JSON.stringify(result));
      }
      const expectedRead = {"a-read":"original source\\n","g-read":"child A edited\\n","foreign-read":"child A edited\\n"}[result.toolCallId];
      if (expectedRead !== undefined) {
        const actualRead = result.content.filter(part => part.type === "text").map(part => part.text).join("");
        if (actualRead !== expectedRead) throw new Error("actual native read did not observe its approved isolated source: " + result.toolCallId);
      }
    }
    const call = (id, name, args) => fauxAssistantMessage(fauxToolCall(name, args, {id}), {stopReason:"toolUse"});
    if (role === "ROOT") {
      if (done("root-spawn-a") && !concurrentWritten) {
        const original = process.env.ACYCLIC_NATIVE_ORIGINAL_PROJECT;
        if (!original) throw new Error("missing explicit concurrent-source fixture");
        writeFileSync(join(original, "concurrent.txt"), "external root edit after original child admission\\n");
        concurrentWritten = true;
      }
      const steps = [
        ["root-spawn-a", "acyclic_spawn", {id:"child-a",prompt:"ACYCLIC_NATIVE_CHILD_A CHILD_A_ONLY_NATIVE_INSTRUCTION"}],
        ["root-spawn-b", "acyclic_spawn", {id:"child-b",prompt:"ACYCLIC_NATIVE_CHILD_B CHILD_B_ONLY_NATIVE_INSTRUCTION"}],
        ["root-send-a", "acyclic_send", {id:"child-a",message:"original root mailbox instruction"}],
        ["root-steer-a", "acyclic_steer", {id:"child-a",message:"queued native child steering"}],
        ["root-wait-a", "acyclic_wait", {id:"child-a"}],
        ["root-wait-b", "acyclic_wait", {id:"child-b"}],
        ["root-spawn-foreign", "acyclic_spawn", {id:"foreign-source-child",source:"child-a",context:"native_cut",prompt:"ACYCLIC_NATIVE_FOREIGN FOREIGN_ONLY_NATIVE_INSTRUCTION"}],
        ["root-wait-foreign", "acyclic_wait", {id:"foreign-source-child"}],
        ["root-inspect-a", "acyclic_inspect", {id:"child-a",maximum_entries:1}],
        ["root-diff-a", "acyclic_diff", {id:"child-a",maximum_entries:1}],
        ["root-import-g", "acyclic_import", {source:"grandchild",destination:sessionId,paths:["grandchild.txt"],confirm:true}],
        ["root-import-a", "acyclic_import", {source:"child-a",destination:sessionId,paths:["child-a.txt"],confirm:true}],
        ["root-import-b", "acyclic_import", {source:"child-b",destination:sessionId,paths:["child-b.txt"],confirm:true}],
        ["root-writeback", "acyclic_writeback", {source:sessionId,paths:["child-a.txt","child-b.txt","grandchild.txt"],confirm:true}],
        ["deny-source", "acyclic_spawn", {id:"forged-source-child",source:"unregistered-source",prompt:"must not run"}],
        ["deny-destination", "acyclic_import", {source:"child-a",destination:"unregistered-destination",paths:["child-a.txt"],confirm:true}],
        ["deny-rebind", "acyclic_spawn", {id:"child-a",prompt:"changed duplicate child instruction"}],
        ["root-status", "acyclic_agents", {}],
      ];
      for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
    } else if (role === "CHILD_A") {
      const steps = [
        ["a-read", "read", {path:"seed.txt"}],
        ["a-write", "write", {path:"child-a.txt",content:"child A original\\n"}],
        ["a-edit", "edit", {path:"child-a.txt",oldText:"child A original",newText:"child A edited"}],
        ["a-spawn-g", "acyclic_spawn", {id:"grandchild",source:"child-a",prompt:"ACYCLIC_NATIVE_GRANDCHILD GRANDCHILD_ONLY_NATIVE_INSTRUCTION"}],
        ["a-wait-g", "acyclic_wait", {id:"grandchild"}],
      ];
      for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
    } else if (role === "CHILD_B") {
      if (!done("b-write")) return call("b-write","write",{path:"child-b.txt",content:"child B isolated\\n"});
    } else if (role === "GRANDCHILD") {
      if (!done("g-read")) return call("g-read","read",{path:"child-a.txt"});
      if (!done("g-write")) return call("g-write","write",{path:"grandchild.txt",content:"grandchild inherited edited source\\n"});
    } else if (role === "FOREIGN") {
      if (!done("foreign-read")) return call("foreign-read","read",{path:"child-a.txt"});
    } else if (role === "REOPEN") {
      const steps = [
        ["reopen-inspect-a", "acyclic_inspect", {id:"child-a",maximum_entries:128}],
        ["reopen-diff-a", "acyclic_diff", {id:"child-a",maximum_entries:128}],
        ["reopen-status", "acyclic_agents", {}],
      ];
      for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
    }
    return fauxAssistantMessage(JSON.stringify({role,sessionId,model:model.id,
      tools:getCurrentTools(context.messages).map(tool=>tool.name).sort(),
      results:results.map(message=>({id:message.toolCallId,isError:message.isError,
        text:message.content.filter(part=>part.type==="text").map(part=>part.text).join("")})),
      messages:context.messages,
    }));
  };
  provider.setResponses(Array.from({length:64},()=>response));
  pi.registerProvider(provider.provider);
}
`;

test("required pinned Pi recursively launches original native effects and reopens without repeating work", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-native-recursive-", import.meta.url)));
  let fixture: InstalledNativeFixture | undefined;
  const oracleDirectory = join(directory, "native-oracles");
  const ownedServicePids: number[] = [];
  let originalFailure: unknown;
  try {
    mkdirSync(oracleDirectory);
    fixture = await installedNativeFixture(directory, recursiveProvider, {
      ACYCLIC_NATIVE_ORACLE_DIRECTORY:oracleDirectory, ACYCLIC_BINARY:process.env.ACYCLIC_BINARY!,
    });
    await fixture.start();
    ownedServicePids.push(fixture.servicePid()!);
    fixture.environment.ACYCLIC_NATIVE_ORIGINAL_PROJECT = fixture.project;
    const operator = new PiOperatorDriver({
      session:fixture.manager.getSessionId(), cwd:fixture.project, command:fixture.command,
      operation:"root-writeback", selected:{
        "child-a.txt":{baseline:null,candidate:"child A edited\n",current:null},
        "child-b.txt":{baseline:null,candidate:"child B isolated\n",current:null},
        "grandchild.txt":{baseline:null,candidate:"grandchild inherited edited source\n",current:null},
      }, preserved:{"concurrent.txt":"external root edit after original child admission\n"},
    });
    const result = await fixture.runJson("ACYCLIC_NATIVE_ROOT", line => operator.consumeJsonLine(line));
    await operator.assertPublished();
    const finalMessages = result.stdout.split("\n").filter(line => line.trim()).map(line => JSON.parse(line))
      .filter(event => event.type === "message_end" && event.message?.role === "assistant")
      .flatMap(event => event.message.content.filter(part => part.type === "text").map(part => part.text));
    assert.equal(finalMessages.length, 1, "actual JSON stream did not contain one completed original Root report");
    const report = JSON.parse(finalMessages[0]);
    assert.equal(report.role, "ROOT");
    assert.equal(report.sessionId, fixture.manager.getSessionId());
    const outputs = new Map<string, {id:string;isError:boolean;text:string}>(report.results.map((value: {id:string;isError:boolean;text:string}) => [value.id,value]));
    for (const id of ["root-spawn-a","root-spawn-b","root-send-a","root-steer-a","root-wait-a","root-wait-b",
      "root-spawn-foreign","root-wait-foreign","root-inspect-a","root-diff-a","root-import-g","root-import-a","root-import-b","root-writeback","root-status"]) {
      assert.equal(outputs.get(id)?.isError, false, `${id} did not complete through the actual production tool`);
    }
    for (const id of ["deny-source","deny-destination","deny-rebind"]) assert.equal(outputs.get(id)?.isError, true);
    const first = JSON.parse(outputs.get("root-spawn-a")!.text).child;
    const second = JSON.parse(outputs.get("root-spawn-b")!.text).child;
    assert.equal(first.id, "child-a"); assert.equal(second.id, "child-b");
    assert.notEqual(first.task, second.task);
    assert.notEqual(first.native_session_id, fixture.manager.getSessionId());
    assert.notEqual(first.native_session_id, second.native_session_id);
    const grandchildSpawn = SessionManager.open(first.session_file, directory).getEntries().find(entry =>
      entry.type === "message" && entry.message.role === "toolResult" && entry.message.toolCallId === "a-spawn-g");
    assert.ok(grandchildSpawn?.type === "message" && grandchildSpawn.message.role === "toolResult");
    const grandchildResult = JSON.parse(grandchildSpawn.message.content.filter(part => part.type === "text").map(part => part.text).join(""));
    const filesystem = await openNativeFs({
      root: join(directory, process.platform === "win32" ? "local/Acyclic/state-v5/filesystem" : "state/acyclic/state-v5/filesystem"),
      objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
    });
    try {
      for (const [id, spawned] of [["root-spawn-a", JSON.parse(outputs.get("root-spawn-a")!.text)],
        ["root-spawn-b", JSON.parse(outputs.get("root-spawn-b")!.text)], ["a-spawn-g", grandchildResult]] as const) {
        const created = spawned.preparation.outcome.Succeeded;
        const capture = created.captures.find((value: {kind: string; value?: {source: {kind: string}}}) =>
          value.kind === "captured" && value.value?.source.kind === "private_volume")?.value;
        assert.ok(capture, "installed creator must retain its actual scratch fork");
        const source = capture.source.reference, revision = capture.revision.reference;
        assert.deepEqual(source.paths, []);
        assert.deepEqual(revision.paths, []);
        assert.deepEqual(revision.volume, created.child_private_volume);
        const parent = await filesystem.openWorkspace(volumeName(source.volume));
        const child = await filesystem.openWorkspace(volumeName(revision.volume));
        const parentGeneration = await parent.generation(Uint8Array.from(source.generation.key));
        const initial = await child.generation(Uint8Array.from(revision.generation.key));
        assert.notDeepEqual(initial.workspaceId, parentGeneration.workspaceId);
        assert.deepEqual((await initial.listDirectory("/", undefined, 256)).entries, []);
        assert.ok((await parentGeneration.listDirectory("/", undefined, 256)).entries.length > 0);
        const destination = await parent.forkAt(`scratch-exclusion-${id}`, parentGeneration);
        const parentHead = await parent.head(), childHead = await child.head();
        const plan = await child.joinInto(destination, {
          history: "merge", maximumGenerations: 256, maximumChanges: 256, maximumConflicts: 16,
          sourceGeneration: initial.id, targetGeneration: await destination.head(),
        });
        try {
          assert.deepEqual(plan.commonAncestor, parentGeneration.id, "actual scratch generation must retain parent lineage");
          assert.equal((await plan.apply(plan.targetHead)).status, "applied");
          assert.deepEqual((await destination.diff(parentGeneration, await destination.sync(), 256)).changes().files, [],
            "filtered exclusions must not publish parent deletions");
          assert.deepEqual(await parent.head(), parentHead);
          assert.deepEqual(await child.head(), childHead);
        } finally { await plan.close(); }
      }
    } finally { filesystem.cancel(); }
    const originalStatus = JSON.parse(outputs.get("root-status")!.text);
    assert.equal(originalStatus.tasks.length, 5, "root/two fresh children/grandchild/foreign-workspace child must be the actual retained task topology");
    assert.equal(new Set(originalStatus.tasks.map((task: {task:string}) => task.task)).size, 5);
    assert.equal(readFileSync(join(fixture.project, "child-a.txt"), "utf8"), "child A edited\n");
    assert.equal(readFileSync(join(fixture.project, "child-b.txt"), "utf8"), "child B isolated\n");
    assert.equal(readFileSync(join(fixture.project, "grandchild.txt"), "utf8"), "grandchild inherited edited source\n");
    assert.equal(readFileSync(join(fixture.project, "concurrent.txt"), "utf8"), "external root edit after original child admission\n");
    const childA = SessionManager.open(first.session_file, directory);
    const childB = SessionManager.open(second.session_file, directory);
    assert.equal(childA.getSessionId(), first.native_session_id);
    assert.equal(childB.getSessionId(), second.native_session_id);
    const childABytes = readFileSync(first.session_file, "utf8");
    const childBBytes = readFileSync(second.session_file, "utf8");
    assert.doesNotMatch(childABytes, /original native recursive instruction|ROOT_ONLY_NATIVE_INSTRUCTION/);
    assert.doesNotMatch(childBBytes, /original native recursive instruction|ROOT_ONLY_NATIVE_INSTRUCTION/);
    const foreign = JSON.parse(outputs.get("root-spawn-foreign")!.text).child;
    const firstRoot = JSON.parse(readFileSync(join(oracleDirectory, `${fixture.manager.getSessionId()}.ROOT.json`), "utf8"));
    const firstA = JSON.parse(readFileSync(join(oracleDirectory, `${first.native_session_id}.CHILD_A.json`), "utf8"));
    const firstB = JSON.parse(readFileSync(join(oracleDirectory, `${second.native_session_id}.CHILD_B.json`), "utf8"));
    const firstForeign = JSON.parse(readFileSync(join(oracleDirectory, `${foreign.native_session_id}.FOREIGN.json`), "utf8"));
    const grandchildOracles = readdirSync(oracleDirectory).filter(name => name.endsWith(".GRANDCHILD.json"));
    assert.equal(grandchildOracles.length, 1, "the actual native grandchild must retain an independent first-context oracle");
    const firstGrandchild = JSON.parse(readFileSync(join(oracleDirectory, grandchildOracles[0]), "utf8"));
    assert.notEqual(firstA.pid, firstRoot.pid);
    assert.notEqual(firstB.pid, firstA.pid);
    assert.notEqual(firstForeign.pid, firstRoot.pid);
    assert.notEqual(firstGrandchild.pid, firstA.pid);
    assert.notEqual(firstGrandchild.pid, firstRoot.pid);
    assert.equal(firstA.sessionId, first.native_session_id);
    assert.equal(firstB.sessionId, second.native_session_id);
    const userTexts = (messages: Array<{role:string;content:string|Array<{type:string;text:string}>}>) =>
      messages.filter(message => message.role === "user").map(message => typeof message.content === "string" ? message.content
        : message.content.filter(part => part.type === "text").map(part => part.text).join(""));
    assert.ok(userTexts(firstRoot.messages).some(text => text.includes("ROOT_ONLY_NATIVE_INSTRUCTION")));
    assert.ok(userTexts(firstA.messages).some(text => text.includes("CHILD_A_ONLY_NATIVE_INSTRUCTION")));
    assert.ok(userTexts(firstB.messages).some(text => text.includes("CHILD_B_ONLY_NATIVE_INSTRUCTION")));
    assert.ok(userTexts(firstGrandchild.messages).some(text => text.includes("GRANDCHILD_ONLY_NATIVE_INSTRUCTION")));
    for (const child of [firstA, firstB, firstGrandchild]) {
      assert.doesNotMatch(JSON.stringify(child.messages), /ROOT_ONLY_NATIVE_INSTRUCTION/);
    }
    assert.doesNotMatch(JSON.stringify(firstA.messages), /CHILD_B_ONLY_NATIVE_INSTRUCTION/);
    assert.doesNotMatch(JSON.stringify(firstB.messages), /CHILD_A_ONLY_NATIVE_INSTRUCTION/);
    assert.doesNotMatch(JSON.stringify(firstGrandchild.messages), /CHILD_A_ONLY_NATIVE_INSTRUCTION/);
    assert.ok(userTexts(firstForeign.messages).some(text => text.includes("ROOT_ONLY_NATIVE_INSTRUCTION")));
    assert.ok(!userTexts(firstForeign.messages).some(text => text.includes("CHILD_A_ONLY_NATIVE_INSTRUCTION")));
    assert.doesNotMatch(childABytes, /ACYCLIC_NATIVE_CHILD_B|changed duplicate child instruction/);
    assert.doesNotMatch(childBBytes, /grandchild inherited edited source|child A edited/);
    assert.ok(childA.getEntries().some(entry => entry.type === "message" && entry.message.role === "toolResult" && entry.message.toolName === "edit"));
    await fixture.close();
    await fixture.start();
    ownedServicePids.push(fixture.servicePid()!);
    const reopened = JSON.parse((await fixture.run("ACYCLIC_NATIVE_REOPEN", "switched")).stdout.trim());
    assert.equal(reopened.model, "switched");
    for (const id of ["reopen-inspect-a", "reopen-diff-a"]) {
      const selected = reopened.results.find((result: {id:string}) => result.id === id);
      assert.equal(selected?.isError, false, `${id} must select the protected final publication after cold service reopen`);
      assert.match(selected.text, /child-a\.txt/, `${id} substituted the initial project generation for the published child edit`);
    }
    const reopenedStatus = JSON.parse(reopened.results.find((result: {id:string}) => result.id === "reopen-status").text);
    assert.deepEqual(reopenedStatus, originalStatus, "actual service/runtime reopen repeated a task/effect or changed original outcomes");
    assert.equal(readFileSync(first.session_file, "utf8"), childABytes);
    assert.equal(readFileSync(second.session_file, "utf8"), childBBytes);
    await assert.rejects(() => fixture!.command(["__pi", "TaskStatus"], JSON.stringify({
      cwd: first.cwd, session_id: first.native_session_id, caller: "forged-original-child-credential",
    })), /./, "native child identity/cwd alone must not authorize its recursive caller");
  } catch (error) { originalFailure = error; }
  finally {
    const cleanupFailures: unknown[] = [];
    try { await fixture?.close(); } catch (error) { cleanupFailures.push(error); }
    try { await fixture?.kill(); } catch (error) { cleanupFailures.push(error); }
    let ownedNativePids: number[] = [];
    try { ownedNativePids = existsSync(oracleDirectory) ? readdirSync(oracleDirectory).map(name =>
      JSON.parse(readFileSync(join(oracleDirectory, name), "utf8")).pid as number) : []; }
    catch (error) { cleanupFailures.push(error); }
    for (const pid of [...ownedServicePids, ...ownedNativePids]) {
      try { assertNativePidExited(pid); } catch (error) { cleanupFailures.push(error); }
    }
    try { rmSync(directory, {recursive:true}); assert.equal(existsSync(directory), false, "owned native fixture namespace was not released"); }
    catch (error) { cleanupFailures.push(error); }
    if (originalFailure !== undefined && cleanupFailures.length) {
      throw new AggregateError([originalFailure, ...cleanupFailures], "native recursive execution and owned cleanup failed", {cause:originalFailure});
    }
    if (originalFailure !== undefined) throw originalFailure;
    if (cleanupFailures.length) throw new AggregateError(cleanupFailures, "native fixture cleanup failed");
  }
}, { timeout: 240_000 });

test("installed Pi retains split spawn, import and cancellation identities across lost replies and extension reload", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-native-cuts-", import.meta.url)));
  const oracleDirectory = join(directory, "native-oracles");
  let fixture: InstalledNativeFixture | undefined;
  try {
    mkdirSync(oracleDirectory);
    fixture = await installedNativeFixture(directory, recursiveProvider, {
      ACYCLIC_NATIVE_ORACLE_DIRECTORY: oracleDirectory, ACYCLIC_NATIVE_PAUSE: "1",
    });
    const { nativeChildSession, observePiOperation } = await loadPiContextHelpers();
    await fixture.start();
    await fixture.run("ACYCLIC_NATIVE_CONTROL");
    const fields = { cwd: fixture.project, session_id: fixture.manager.getSessionId() };
    const control = async (event: string, arguments_: Record<string, unknown>) =>
      JSON.parse((await fixture!.command(["__pi", event], JSON.stringify({ ...fields, ...arguments_ }))).stdout);
    const restart = async () => { await fixture!.kill(); await fixture!.start(); };
    const rootBytes = readFileSync(fixture.session);
    const sourceBytes = readFileSync(join(fixture.project, "seed.txt"));
    const spawn = { operation: "cut-spawn", id: "cut-child", context: "scratch",
      prompt: "ACYCLIC_NATIVE_CHILD_B CHILD_B_ONLY_NATIVE_INSTRUCTION", provider: "acyclic-native-qualification", model: "probe" };
    const prepared = await control("SpawnPrepare", spawn);
    assert.equal(existsSync(prepared.child.session_file), false);
    await restart();
    assert.deepEqual(await control("SpawnPrepare", spawn), prepared);
    assert.equal(existsSync(prepared.child.session_file), false, "prepare retry must not start native work");
    const commit = { ...spawn, ...nativeChildSession(fixture.manager, prepared.child.cwd) };
    const started = await control("SpawnCommit", commit);
    assert.equal(started.child.task, prepared.child.task);
    const nativeOracle = join(oracleDirectory, `${started.child.native_session_id}.CHILD_B.json`);
    await observePiOperation("cut-native-start", async () => ({
      state: existsSync(nativeOracle) ? "completed" : "pending", outcome: {},
    }));
    const originalNative = readFileSync(nativeOracle);
    const beforeReload = await control("TaskStatus", {});
    const reloaded = JSON.parse((await fixture.run("ACYCLIC_NATIVE_CONTROL", "switched")).stdout.trim());
    assert.equal(reloaded.model, "switched");
    assert.deepEqual((await control("TaskStatus", {})).tasks, beforeReload.tasks);
    assert.equal(JSON.parse(originalNative.toString()).model, "probe");
    writeFileSync(join(oracleDirectory, `${started.child.native_session_id}.release`), "release");
    const completed = await observePiOperation("cut-wait", () => control("TaskWait", { id: spawn.id }));
    const childBytes = readFileSync(started.child.session_file);
    await restart();
    const duplicate = await control("SpawnCommit", commit);
    for (const key of ["child", "fork_operation", "native_operation", "native_effect", "preparation"]) {
      assert.deepEqual(duplicate[key], started[key], `lost start reply changed ${key}`);
    }
    assert.deepEqual(duplicate.outcome, completed.outcome);
    assert.deepEqual(readFileSync(started.child.session_file), childBytes);
    assert.deepEqual(readFileSync(nativeOracle), originalNative);
    const importing = { operation: "cut-import", source: spawn.id, destination: fields.session_id,
      paths: ["child-b.txt"], confirm: true };
    await control("TaskImport", importing);
    const destination = await control("TaskInspect", { operation: "cut-destination-before", id: fields.session_id });
    const source = await control("TaskInspect", { operation: "cut-source-before", id: spawn.id });
    assert.ok(destination.snapshot.authored_generation && source.snapshot.authored_generation);
    await restart();
    await control("TaskImport", importing);
    assert.deepEqual((await control("TaskInspect", { operation: "cut-destination-after", id: fields.session_id })).snapshot.authored_generation, destination.snapshot.authored_generation,
      "lost import reply must not publish a second destination generation");
    assert.deepEqual((await control("TaskInspect", { operation: "cut-source-after", id: spawn.id })).snapshot.authored_generation, source.snapshot.authored_generation);
    assert.deepEqual(readFileSync(started.child.session_file), childBytes);
    const cancelling = { ...spawn, operation: "cut-cancel-spawn", id: "cancel-child" };
    const cancelPrepared = await control("SpawnPrepare", cancelling);
    const cancelCommit = { ...cancelling, ...nativeChildSession(fixture.manager, cancelPrepared.child.cwd) };
    const pending = await control("SpawnCommit", cancelCommit);
    await observePiOperation("cut-cancel-start", async () => ({
      state: existsSync(join(oracleDirectory, `${pending.child.native_session_id}.CHILD_B.json`)) ? "completed" : "pending", outcome: {},
    }));
    await restart();
    const interrupted = await control("SpawnCommit", cancelCommit);
    assert.deepEqual(interrupted.child, pending.child);
    assert.deepEqual(interrupted.native_effect, pending.native_effect);
    assert.equal(interrupted.outcome?.Succeeded, undefined, "unreleased original native work must not be reported successful after death");
    await control("TaskCancel", { task: pending.child.task });
    await restart();
    const cancelled = await control("SpawnCommit", cancelCommit);
    assert.equal(cancelled.child.task, pending.child.task);
    assert.deepEqual(cancelled.native_effect, pending.native_effect);
    assert.ok(cancelled.uncertain || cancelled.outcome, "cancelled dispatch must retain its original uncertainty or terminal outcome");
    assert.equal(existsSync(join(pending.child.cwd, "child-b.txt")), false);
    assert.deepEqual(readFileSync(join(fixture.project, "seed.txt")), sourceBytes);
    assert.deepEqual(readFileSync(fixture.session).subarray(0, rootBytes.length), rootBytes);
    const unstarted = { ...spawn, operation: "cancel-preparation", id: "never-started" };
    const unstartedPreparation = await control("SpawnPrepare", unstarted);
    const unstartedCommit = { ...unstarted, ...nativeChildSession(fixture.manager, unstartedPreparation.child.cwd) };
    const rootTask = (await control("TaskStatus", {})).caller_task;
    assert.equal(typeof rootTask, "string");
    const filesystem = await openNativeFs({
      root: join(directory, process.platform === "win32" ? "local/Acyclic/state-v5/filesystem" : "state/acyclic/state-v5/filesystem"),
      objectCache: DEFAULT_OBJECT_CACHE_OPTIONS,
    });
    try {
      const importedSource = await filesystem.openWorkspace(volumeName(source.volume));
      const importedDestination = await filesystem.openWorkspace(volumeName(destination.volume));
      const sourceHead = await importedSource.head();
      const racingImport = { ...importing, operation: "cancel-import" };
      const raced = await Promise.allSettled([control("TaskImport", racingImport), control("TaskCancel", { task: rootTask })]);
      assert.equal(raced[1].status, "fulfilled");
      const destinationHead = await importedDestination.head();
      await restart();
      assert.equal((await control("TaskStatus", {})).tasks.find((task: {task:string}) => task.task === rootTask).outcome, "Cancelled");
      const replayed = await control("TaskImport", racingImport).catch(() => undefined);
      if (replayed) {
        assert.equal(raced[0].status, "fulfilled", "cancelled unknown import must not become a new successful dispatch");
        if (raced[0].status === "fulfilled") assert.deepEqual(replayed, raced[0].value);
      }
      await assert.rejects(() => control("SpawnCommit", unstartedCommit));
      assert.equal(existsSync(unstartedPreparation.child.session_file), false);
      assert.deepEqual(await importedSource.head(), sourceHead);
      assert.deepEqual(await importedDestination.head(), destinationHead);
    } finally { filesystem.cancel(); }
    await fixture.close();
    for (const name of readdirSync(oracleDirectory).filter(name => name.endsWith(".json"))) {
      assertNativePidExited(JSON.parse(readFileSync(join(oracleDirectory, name), "utf8")).pid);
    }
  } finally {
    for (const name of readdirSync(oracleDirectory).filter(name => name.endsWith(".json"))) {
      const { pid } = JSON.parse(readFileSync(join(oracleDirectory, name), "utf8"));
      try { process.kill(pid, "SIGKILL"); }
      catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; }
    }
    try { await fixture?.close(); }
    finally { await fixture?.kill(); rmSync(directory, { recursive: true }); }
  }
}, { timeout: 180_000 });

function assertNativePidExited(pid: number): void {
  assert.ok(Number.isSafeInteger(pid) && pid > 0, "native process oracle did not record an actual PID");
  try { process.kill(pid, 0); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return;
    throw error;
  }
  assert.fail(`original owned native process ${pid} survived recorded completion and service cleanup`);
}
