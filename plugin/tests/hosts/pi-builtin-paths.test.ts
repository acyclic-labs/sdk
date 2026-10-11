import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { SessionManager, VERSION } from "@earendil-works/pi-coding-agent";
import { installedNativeFixture, pinnedPiCli, type InstalledNativeFixture } from "./pi-native-fixture.ts";

const execute = promisify(execFile);

// The provider drives real model calls, not replacement production IO. Its one
// hostile foreign reader is a genuine registered-tool negative control.
const builtinProvider = String.raw`
import { VERSION } from "@earendil-works/pi-coding-agent";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import { existsSync, readFileSync, renameSync, symlinkSync, watch, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { Type } from "typebox";
export default function(pi) {
  if (VERSION !== "1.1.0") throw new Error("unpinned native Pi");
  const outside = process.env.ACYCLIC_BUILTIN_OUTSIDE;
  const original = process.env.ACYCLIC_BUILTIN_PROJECT;
  const oracle = process.env.ACYCLIC_BUILTIN_ORACLE;
  if (!outside || !original || !oracle) throw new Error("missing original builtin fixture artifacts");
  pi.registerTool({
    name:"acyclic_foreign_read", label:"Foreign native reader",
    description:"Read a host file through an unscoped foreign tool",
    parameters:Type.Object({path:Type.String()}),
    async execute(_id,input) {
      writeFileSync(join(oracle,"foreign-reader-executed"),"executed\n");
      return {content:[{type:"text",text:readFileSync(input.path,"utf8")}],details:undefined};
    },
  });
  let role, session, cwd, scratch, cancelNative;
  pi.on("session_start", (_event, ctx) => {
    session = ctx.sessionManager.getSessionId();
    cwd = ctx.cwd;
    scratch = dirname(ctx.sessionManager.getSessionFile());
  });
  pi.on("before_agent_start", event => {
    const selected = event.prompt.match(/^ACYCLIC_BUILTIN_(ROOT|CHILD|ABORT|CANCEL)(?:\s|$)/);
    if (!selected) throw new Error("missing current native builtin instruction");
    role = selected[1];
  });
  pi.on("tool_call", async (event, ctx) => {
    if (event.toolCallId === "cancel-before-write") {
      if (!ctx.signal) throw new Error("original native builtin has no actual Pi cancellation signal");
      const pending = join(oracle,session + ".cancel.pending");
      writeFileSync(pending,JSON.stringify({session,cwd,scratch,pid:process.pid}),{flag:"wx"});
      renameSync(pending,join(oracle,session + ".cancel.json"));
      // The real native tool is awaiting its original signal when Root orders
      // actual owning-task cancellation. No production IO is simulated here.
      const signal = ctx.signal;
      signal.throwIfAborted();
      const {promise,reject} = Promise.withResolvers();
      const stopped = () => reject(signal.reason);
      signal.addEventListener("abort",stopped,{once:true});
      try { await promise; }
      finally { signal.removeEventListener("abort",stopped); }
    }
    if (event.toolCallId === "root-cancel") {
      if (!cancelNative) throw new Error("Root cancellation lost the actual admitted native session");
      const filename = cancelNative + ".cancel.json";
      const ready = join(oracle,filename);
      const {promise,resolve,reject} = Promise.withResolvers();
      // Separate real OS processes cannot share fake time. This deadline only
      // bounds a missing required artifact; readiness is the actual fs event.
      const deadline = AbortSignal.timeout(30000);
      const missing = () => reject(new Error("actual native builtin never reached its cancellation boundary"));
      deadline.addEventListener("abort",missing,{once:true});
      const watcher = watch(oracle,(_event,name) => {
        if (String(name) === filename && existsSync(ready)) resolve();
      });
      try {
        if (existsSync(ready)) resolve();
        await promise;
      } finally {
        watcher.close();
        deadline.removeEventListener("abort",missing);
      }
    }
    if (event.toolCallId === "deny-symlink-read") {
      const link = join(original, "outside-link");
      if (!existsSync(link)) symlinkSync(outside, link, process.platform === "win32" ? "junction" : "dir");
    }
    if (event.toolCallId === "abort-write") {
      // Abort the original Pi operation, not a simulated adapter signal.
      writeFileSync(join(oracle, session + ".abort.json"), JSON.stringify({session,cwd,scratch,pid:process.pid}));
      ctx.abort();
    }
  });
  const provider = fauxProvider({provider:"acyclic-native-qualification",models:[{id:"probe"}]});
  const response = context => {
    const results = context.messages.filter(message => message.role === "toolResult");
    const done = id => results.some(message => message.toolCallId === id);
    const call = (id,name,args) => fauxAssistantMessage(fauxToolCall(name,args,{id}),{stopReason:"toolUse"});
    const marker = join(outside, "hostile-marker.txt");
    for (const result of results) {
      if (result.toolCallId.startsWith("deny-") && !result.isError) throw new Error("outside or unsupported builtin unexpectedly succeeded: " + result.toolCallId);
      if (!result.toolCallId.startsWith("deny-") && result.toolCallId !== "abort-write" && result.isError) {
        throw new Error("actual original builtin failed: " + JSON.stringify(result));
      }
      const text = result.content.filter(part => part.type === "text").map(part => part.text).join("");
      if (text.includes("HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL")) throw new Error("model received bytes outside its declared native view");
    }
    let steps;
    if (role === "ROOT") {
      const admitted = results.find(message => message.toolCallId === "root-spawn-cancel");
      const cancelled = admitted ? JSON.parse(admitted.content.filter(part=>part.type==="text").map(part=>part.text).join("")).child : undefined;
      cancelNative = cancelled?.native_session_id;
      steps = [
        ["root-read","read",{path:"@seed.txt"}],
        ["root-unicode","read",{path:"@space\u00a0file.txt"}],
        ["root-nfd","read",{path:"caf\u00e9.txt"}],
        ["root-ampm","read",{path:"capture 1 PM.txt"}],
        ["root-curly","read",{path:"Capture d'ecran.txt"}],
        ["root-lines","read",{path:"lines.txt",offset:2,limit:1}],
        ["root-spawn","acyclic_spawn",{id:"builtin-child",prompt:"ACYCLIC_BUILTIN_CHILD"}],
        ["root-wait","acyclic_wait",{id:"builtin-child"}],
        ["root-spawn-cancel","acyclic_spawn",{id:"builtin-cancel",prompt:"ACYCLIC_BUILTIN_CANCEL"}],
        ["root-cancel","acyclic_cancel",{task:cancelled?.task}],
        ["root-wait-cancel","acyclic_wait",{id:"builtin-cancel"}],
        ["root-spawn-abort","acyclic_spawn",{id:"builtin-abort",prompt:"ACYCLIC_BUILTIN_ABORT"}],
        ["root-wait-abort","acyclic_wait",{id:"builtin-abort"}],
        ["deny-root-write","write",{path:"seed.txt",content:"model must not publish physical root\n"}],
        ["deny-root-edit","edit",{path:"seed.txt",edits:[{oldText:"original source",newText:"model must not publish physical root"}]}],
        ["deny-outside-read","read",{path:marker}],
        ["deny-outside-write","write",{path:marker,content:"outside overwritten\n"}],
        ["deny-outside-edit","edit",{path:marker,edits:[{oldText:"HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL",newText:"outside overwritten"}]}],
        ["deny-parent-read","read",{path:"../outside/hostile-marker.txt"}],
        ["deny-parent-write","write",{path:"safe/../../outside/hostile-marker.txt",content:"outside overwritten\n"}],
        ["deny-tilde-read","read",{path:"~/outside/hostile-marker.txt"}],
        ["deny-unc-read","read",{path:"\\\\localhost\\acyclic-outside\\hostile-marker.txt"}],
        ["deny-drive-read","read",{path:"Z:\\acyclic-outside\\hostile-marker.txt"}],
        ["deny-drive-relative","read",{path:"Z:hostile-marker.txt"}],
        ["deny-backslash-parent","write",{path:"safe\\..\\..\\outside\\hostile-marker.txt",content:"outside overwritten\n"}],
        ["deny-symlink-read","read",{path:"outside-link/hostile-marker.txt"}],
        ["deny-symlink-write","write",{path:"outside-link/hostile-marker.txt",content:"outside overwritten\n"}],
        ["deny-symlink-edit","edit",{path:"outside-link/hostile-marker.txt",edits:[{oldText:"HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL",newText:"outside overwritten"}]}],
        ["deny-read-quota","read",{path:"over-cap.txt",offset:1,limit:1}],
        ["deny-bash","bash",{command:"cat " + JSON.stringify(marker)}],
        ["deny-powershell","powershell",{command:"Get-Content -LiteralPath " + JSON.stringify(marker)}],
        ["deny-grep","grep",{pattern:"HOSTILE",path:outside}],
        ["deny-find","find",{pattern:"*",path:outside}],
        ["deny-ls","ls",{path:outside}],
        ["deny-foreign-reader","acyclic_foreign_read",{path:marker}],
        ["root-status","acyclic_agents",{}],
      ];
    } else if (role === "CHILD") {
      const scratchFile = join(scratch,"builtin-scratch.txt");
      steps = [
        ["child-read","read",{path:"seed.txt"}],
        ["child-write","write",{path:"nested/child.txt",content:"alpha\nbeta\ngamma\n"}],
        ["child-edit","edit",{path:"nested/child.txt",edits:[{oldText:"alpha",newText:"ALPHA"},{oldText:"gamma",newText:"GAMMA"}]}],
        ["child-edited-read","read",{path:"nested/child.txt"}],
        ["child-bom-edit","edit",{path:"bom-crlf.txt",edits:[{oldText:"first\nsecond",newText:"FIRST\nSECOND"}]}],
        ["child-bom-read","read",{path:"bom-crlf.txt"}],
        ["scratch-write","write",{path:scratchFile,content:"private scratch original\n"}],
        ["scratch-edit","edit",{path:scratchFile,edits:[{oldText:"private scratch original",newText:"private scratch edited"}]}],
        ["scratch-read","read",{path:scratchFile}],
        ["deny-child-outside-read","read",{path:marker}],
        ["deny-child-outside-write","write",{path:marker,content:"outside overwritten\n"}],
        ["deny-child-outside-edit","edit",{path:marker,edits:[{oldText:"HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL",newText:"outside overwritten"}]}],
        ["deny-child-physical-root","write",{path:join(original,"seed.txt"),content:"outside child view\n"}],
        ["deny-child-result","write",{path:join(dirname(scratch),"result","model-file.txt"),content:"unmounted result\n"}],
        ["deny-child-parent","read",{path:"../scratch/session.jsonl"}],
        ["deny-child-write-quota","write",{path:"quota-write.txt",content:"q".repeat(262145)}],
        ["deny-child-edit-quota","edit",{path:"nested/child.txt",edits:[{oldText:"ALPHA",newText:"q".repeat(262145)}]}],
        ["child-after-quota","read",{path:"nested/child.txt"}],
      ];
    } else if (role === "ABORT") {
      steps = [["abort-write","write",{path:"must-not-write-after-abort.txt",content:"must not be committed\n"}]];
    } else if (role === "CANCEL") {
      steps = [["cancel-before-write","write",{path:"must-not-write-after-cancel.txt",content:"must not be committed\n"}]];
    } else throw new Error("no actual builtin role");
    for (const [id,name,args] of steps) if (!done(id)) return call(id,name,args);
    return fauxAssistantMessage(JSON.stringify({role,session,cwd,scratch,pid:process.pid,
      results:results.map(message=>({id:message.toolCallId,isError:message.isError,
        text:message.content.filter(part=>part.type==="text").map(part=>part.text).join("")}))}));
  };
  provider.setResponses(Array.from({length:64},()=>response));
  pi.registerProvider(provider.provider);
}
`;

interface BuiltinResult {
  id: string;
  isError: boolean;
  text: string;
}

interface BuiltinReport {
  role: string;
  session: string;
  cwd: string;
  scratch: string;
  pid: number;
  results: BuiltinResult[];
}

test("required pinned CLI model builtins use original held view IO before reading or mutating host paths", { timeout: 240_000 }, async () => {
  assert.equal(VERSION, "1.1.0");
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-builtin-paths-", import.meta.url)));
  let fixture: InstalledNativeFixture | undefined;
  let originalFailure: unknown;
  try {
    const outside = join(directory, "outside");
    const oracle = join(directory, "oracles");
    mkdirSync(outside);
    mkdirSync(oracle);
    const marker = join(outside, "hostile-marker.txt");
    const markerBytes = "HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL\n";
    writeFileSync(marker, markerBytes);
    fixture = await installedNativeFixture(directory, builtinProvider, {
      ACYCLIC_BUILTIN_OUTSIDE: outside, ACYCLIC_BUILTIN_ORACLE: oracle,
    });
    fixture.environment.ACYCLIC_BUILTIN_PROJECT = fixture.project;
    writeFileSync(join(fixture.project,"space file.txt"), "unicode space original\n");
    writeFileSync(join(fixture.project,"cafe\u0301.txt"), "NFD original\n");
    writeFileSync(join(fixture.project,"capture 1\u202fPM.txt"), "AM PM original\n");
    writeFileSync(join(fixture.project,"Capture d\u2019ecran.txt"), "curly quote original\n");
    writeFileSync(join(fixture.project,"lines.txt"), "first\nsecond\nthird\n");
    writeFileSync(join(fixture.project,"bom-crlf.txt"), "\ufefffirst\r\nsecond\r\nthird\r\n");
    writeFileSync(join(fixture.project,"over-cap.txt"), "q".repeat(262145));
    await fixture.start();
    const rootTools = ["read","write","edit","bash","powershell","grep","find","ls",
      "acyclic_spawn","acyclic_wait","acyclic_cancel","acyclic_agents","acyclic_foreign_read"];
    const invocation = execute(process.execPath, [pinnedPiCli,
      "--print","--mode","text","--session",fixture.session,
      "--provider","acyclic-native-qualification","--model","probe",
      "--no-extensions","--no-mcp","--tools",rootTools.join(","),
      "-e",fixture.extension,"-e",fixture.provider,"--","ACYCLIC_BUILTIN_ROOT",
    ], {cwd:fixture.project,env:fixture.environment,windowsHide:true,timeout:180_000,maxBuffer:4*1024*1024,encoding:"utf8"});
    invocation.child.stdin?.end();
    const output = await invocation;
    assert.doesNotMatch(output.stdout, /HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL/);
    const root: BuiltinReport = JSON.parse(output.stdout.trim());
    assert.equal(root.role,"ROOT");
    assert.equal(root.session,fixture.manager.getSessionId());
    const rootResults = new Map(root.results.map(result => [result.id,result]));
    const expectedRootReads: Record<string,string> = {
      "root-read":"original source\n", "root-unicode":"unicode space original\n",
      "root-nfd":"NFD original\n", "root-ampm":"AM PM original\n", "root-curly":"curly quote original\n",
      "root-lines":"second\n\n[2 more lines in file. Use offset=3 to continue.]",
    };
    for (const [id,text] of Object.entries(expectedRootReads)) {
      assert.equal(rootResults.get(id)?.isError,false,id);
      assert.equal(rootResults.get(id)?.text,text,id);
    }
    const expectedRootDenials = ["deny-root-write","deny-root-edit","deny-outside-read","deny-outside-write","deny-outside-edit",
      "deny-parent-read","deny-parent-write","deny-tilde-read","deny-unc-read","deny-drive-read","deny-drive-relative",
      "deny-backslash-parent","deny-symlink-read","deny-symlink-write","deny-symlink-edit","deny-read-quota",
      "deny-bash","deny-powershell","deny-grep","deny-find","deny-ls","deny-foreign-reader"];
    for (const id of expectedRootDenials) assert.equal(rootResults.get(id)?.isError,true,id);
    for (const id of ["deny-bash","deny-powershell","deny-grep","deny-find","deny-ls","deny-foreign-reader"]) {
      assert.match(rootResults.get(id)!.text,/no original SDK-scoped IO adapter/,id);
    }
    assert.equal(existsSync(join(oracle,"foreign-reader-executed")),false,"a foreign acyclic_ prefix must not confer tool IO authority");
    const child = JSON.parse(rootResults.get("root-spawn")!.text).child;
    const childSession = SessionManager.open(child.session_file,directory);
    const childMessages = childSession.getEntries().filter(entry => entry.type === "message").map(entry => entry.message);
    const childFinal = childMessages.filter(message => message.role === "assistant")
      .flatMap(message => message.content.filter(part => part.type === "text").map(part => part.text));
    const childReport: BuiltinReport = JSON.parse(childFinal.at(-1)!);
    assert.equal(childReport.role,"CHILD");
    assert.equal(childReport.session,child.native_session_id);
    assert.notEqual(childReport.pid,root.pid,"builtin controls must execute in the actual admitted native child");
    const childResults = new Map(childReport.results.map(result => [result.id,result]));
    for (const id of ["child-read","child-write","child-edit","child-edited-read","child-bom-edit","child-bom-read",
      "scratch-write","scratch-edit","scratch-read","child-after-quota"]) assert.equal(childResults.get(id)?.isError,false,id);
    assert.equal(childResults.get("child-read")!.text,"original source\n");
    assert.equal(childResults.get("child-edited-read")!.text,"ALPHA\nbeta\nGAMMA\n");
    assert.equal(childResults.get("child-bom-read")!.text,"\ufeffFIRST\r\nSECOND\r\nthird\r\n");
    assert.equal(childResults.get("scratch-read")!.text,"private scratch edited\n");
    assert.equal(childResults.get("child-after-quota")!.text,"ALPHA\nbeta\nGAMMA\n");
    for (const id of ["deny-child-outside-read","deny-child-outside-write","deny-child-outside-edit",
      "deny-child-physical-root","deny-child-result","deny-child-parent","deny-child-write-quota","deny-child-edit-quota"]) {
      assert.equal(childResults.get(id)?.isError,true,id);
    }
    assert.equal(readFileSync(join(childReport.scratch,"builtin-scratch.txt"),"utf8"),"private scratch edited\n");
    assert.equal(existsSync(join(childReport.cwd,"quota-write.txt")),false);
    const aborted = JSON.parse(rootResults.get("root-spawn-abort")!.text).child;
    const abortOracle = JSON.parse(readFileSync(join(oracle,aborted.native_session_id + ".abort.json"),"utf8"));
    assert.equal(abortOracle.session,aborted.native_session_id);
    assert.notEqual(abortOracle.pid,root.pid);
    assert.equal(existsSync(join(abortOracle.cwd,"must-not-write-after-abort.txt")),false);
    const abortedSession = SessionManager.open(aborted.session_file,directory);
    const abortResult = abortedSession.getEntries().find(entry => entry.type === "message" &&
      entry.message.role === "toolResult" && entry.message.toolCallId === "abort-write");
    assert.ok(abortedSession.getEntries().some(entry => entry.type === "message" && entry.message.role === "assistant" &&
      entry.message.content.some(part => part.type === "toolCall" && part.id === "abort-write")));
    if (abortResult && abortResult.type === "message" && abortResult.message.role === "toolResult") {
      assert.equal(abortResult.message.isError,true,"a persisted native abort result must not claim a successful write");
    }
    const cancelled = JSON.parse(rootResults.get("root-spawn-cancel")!.text).child;
    const cancelOracle = JSON.parse(readFileSync(join(oracle,cancelled.native_session_id + ".cancel.json"),"utf8"));
    assert.equal(cancelOracle.session,cancelled.native_session_id);
    assert.notEqual(cancelOracle.pid,root.pid);
    const cancellation = JSON.parse(rootResults.get("root-cancel")!.text);
    const cancelledTask = cancellation.tasks.find((task: {task:string}) => task.task === cancelled.task);
    assert.equal(cancelledTask?.state,"outcome");
    assert.equal(cancelledTask?.outcome,"Cancelled","the actual original TaskHost, not SDK abort or cwd, must record cancellation");
    assert.throws(() => process.kill(cancelOracle.pid,0), {code:"ESRCH"},"the actual owned native process must be observed stopped");
    assert.equal(existsSync(join(cancelOracle.cwd,"must-not-write-after-cancel.txt")),false);
    assert.equal(existsSync(join(fixture.project,"must-not-write-after-cancel.txt")),false);
    assert.equal(readFileSync(marker,"utf8"),markerBytes,"outside write/edit/symlink controls must leave actual host bytes unchanged");
    assert.equal(readFileSync(join(fixture.project,"seed.txt"),"utf8"),"original source\n","model builtins must not publish physical root");
    assert.equal(readFileSync(join(fixture.project,"bom-crlf.txt"),"utf8"),"\ufefffirst\r\nsecond\r\nthird\r\n");
    assert.equal(existsSync(join(fixture.project,"nested","child.txt")),false,"child native writes must stay in its actual private project");
    assert.doesNotMatch(JSON.stringify(childReport),/HOSTILE_OUTSIDE_BYTES_NOT_A_CREDENTIAL/);
  } catch (error) { originalFailure = error; }
  finally {
    const failures: unknown[] = [];
    try { await fixture?.close(); } catch (error) { failures.push(error); }
    try { rmSync(directory,{recursive:true,force:true}); } catch (error) { failures.push(error); }
    if (originalFailure !== undefined && failures.length) {
      throw new AggregateError([originalFailure,...failures],"original builtin qualification and owned fixture cleanup failed",{cause:originalFailure});
    }
    if (originalFailure !== undefined) throw originalFailure;
    if (failures.length) throw new AggregateError(failures,"owned builtin qualification cleanup failed");
  }
});
