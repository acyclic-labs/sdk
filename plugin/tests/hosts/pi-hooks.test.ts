import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { test } from "bun:test";
import fc from "fast-check";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import type { Invoke } from "../../pi/index.ts";
import { createHash } from "node:crypto";
import { loadPiExtension } from "./pi-fixture.ts";

const { default: extension } = await loadPiExtension(fileURLToPath(new URL("../../pi/index.ts", import.meta.url)));

type Handler = (event: Record<string, unknown>, context: ExtensionContext) => Promise<unknown>;
const definitions = ["bash", "fs", "read", "write", "edit"].map(name => ({
  name, description: `Harness ${name}`, input_schema: { type: "object", properties: {} },
}));
function setup(send: Invoke, initial: string[] = ["foreign-mcp"], write?: Parameters<typeof extension>[2]) {
  const handlers = new Map<string, Handler>();
  const tools = new Map<string, ToolDefinition>();
  let active = [...initial];
  const api = {
    on(event: string, handler: Handler) { assert.ok(!handlers.has(event)); handlers.set(event, handler); },
    registerTool(tool: ToolDefinition) { tools.set(tool.name, tool); },
    getActiveTools() { return [...active]; },
    setActiveTools(names: string[]) { active = [...names]; },
  } as unknown as ExtensionAPI;
  extension(api, send, write);
  const context = {
    cwd: "/selected-project", sessionManager: { getSessionId: () => "native-session" },
  } as unknown as ExtensionContext;
  return { handlers, tools, context, active: () => active };
}

test("Pi executes every registered tool exactly once through the harness", async () => {
  const calls: { event: string; input: Record<string, unknown> }[] = [];
  const result = { content: [{type: "text", text: "harness result"}], details: {published:true}, isError:false };
  const {handlers,tools,context,active} = setup(async (event,input) => {
    calls.push({event,input});
    return event === "catalog" ? {tools:definitions} : event === "tool_call" ? input.hostExecuted ? {block:false} : result : {};
  });
  await handlers.get("session_start")!({},context);
  assert.deepEqual([...tools.keys()],definitions.map(tool=>tool.name));
  assert.deepEqual(active(),["foreign-mcp",...definitions.map(tool=>tool.name)]);
  for (const [name,tool] of tools) {
    assert.deepEqual(tool.parameters,definitions.find(definition=>definition.name===name)!.input_schema);
    assert.equal(typeof tool.renderCall,"function");
    assert.equal(typeof tool.renderResult,"function");
    const count = calls.length;
    assert.equal(await handlers.get("tool_call")!({toolName:name},context),undefined);
    assert.equal(calls.length,count,"permission hook must not execute a tool");
    const answer = await tool.execute("call",{},undefined,undefined,context);
    assert.deepEqual(answer,{content:result.content,details:result.details});
    assert.deepEqual(calls.at(-1),{event:"tool_call",input:{toolCallId:"call",toolName:name,input:{},sessionId:"native-session",cwd:"/selected-project"}});
    assert.equal(calls.length,count+1);
  }
  const foreign = {toolName:"foreign-mcp",toolCallId:"host-call",input:{query:"native"}};
  assert.equal(await handlers.get("tool_call")!(foreign,context),undefined);
  assert.deepEqual(calls.at(-1),{event:"tool_call",input:{...foreign,sessionId:"native-session",cwd:"/selected-project",hostExecuted:true}});
  const completion = {...foreign,content:[{type:"text",text:"native result"}],isError:false};
  await handlers.get("tool_result")!(completion,context);
  assert.deepEqual(calls.at(-1),{event:"tool_result",input:{...completion,sessionId:"native-session",cwd:"/selected-project"}});
  await handlers.get("session_shutdown")!({},context);
  assert.equal(calls.at(-1)!.event,"session_shutdown");
});

test("native launch identity and seed survive exact retries without a memory cache",async () => {
  await fc.assert(fc.asyncProperty(fc.uuid(),fc.stringMatching(/^[a-z]{1,16}$/),async (parent,callId) => {
    const calls: Record<string,unknown>[]=[];
    const make=async () => {
      const fixture=setup(async (event,input)=> {
        if(event === "catalog") return {tools:[{name:"acyclic_spawn",description:"spawn",input_schema:{type:"object"}}]};
        if(event === "tool_call") calls.push(input);
        return {content:[{type:"text",text:"child"}],isError:false};
      });
      const manager=SessionManager.inMemory("/selected-project",{id:parent});
      manager.appendMessage(fauxAssistantMessage(fauxToolCall("acyclic_spawn",{id:"label",prompt:"literal $(prompt)"},{id:callId}),
        {stopReason:"toolUse",timestamp:1}));
      // Native entry timestamps are retained in the source session file.
      const entries=manager.getEntries().map(entry=>({...entry,timestamp:"2026-10-17T00:00:00Z"}));
      const restored=SessionManager.inMemory("/selected-project",{id:parent},JSON.parse(JSON.stringify(entries)));
      const context={...fixture.context,sessionManager:restored,model:{id:"probe",provider:"test"}} as unknown as ExtensionContext;
      await fixture.handlers.get("session_start")!({},context);
      await fixture.tools.get("acyclic_spawn")!.execute(callId,{id:"label",prompt:"literal $(prompt)"},undefined,undefined,context);
    };
    await make();await make();
    assert.deepEqual(calls[0],calls[1],"a fresh shim instance changed the immutable native invocation");
    const declaration=(calls[0]!.input as {executor:{session:{session_id:string},session_bytes:string}}).executor;
    assert.equal(JSON.parse(declaration.session_bytes).id,declaration.session.session_id);
    assert.notEqual(declaration.session.session_id,parent);
    assert.match(declaration.session.session_id,/^[0-9a-f]{8}-[0-9a-f]{4}-8[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/);
    assert.ok(!("environment" in declaration));
  }),{numRuns:32});
});

test("selected native source uses task files and revalidates the SDK before admission",async () => {
  for (const mutation of ["unchanged","entry","branch"] as const) {
    const manager=SessionManager.inMemory("/selected-project");
    const root=manager.appendMessage({role:"user",content:"selected ".repeat(1800),timestamp:1});
    manager.appendMessage(fauxAssistantMessage(fauxToolCall("acyclic_spawn",{id:"child",prompt:"instruction",context:"native_cut"},{id:"spawn"}),{stopReason:"toolUse",timestamp:2}));
    const before=[manager.getHeader(),...manager.getEntries()].map(entry=>JSON.stringify(entry)).join("\n")+"\n";
    const files:Array<{key:{Input:string},channel:null,bytes:number[]}>=[];
    const calls:Record<string,unknown>[]=[];
    const task="00000000-0000-0000-0000-000000000001";
    const fixture=setup(async (event,input)=> {
      if(event==="session_start") return {task};
      if(event==="catalog") return {tools:[{name:"acyclic_spawn",description:"spawn",input_schema:{type:"object"}}]};
      if(event==="tool_call") calls.push(input);
      return {content:[{type:"text",text:"child"}],isError:false};
    },[],async (bound,path,source)=> {
      assert.equal(bound,task);files.push(...source);
      assert.equal(files.at(-1)!.key.Input,path);
      if(mutation==="entry") manager.appendCustomEntry("changed",{});
      if(mutation==="branch") manager.branch(root);
    });
    const context={...fixture.context,sessionManager:manager,model:{id:"probe",provider:"test"}} as unknown as ExtensionContext;
    await fixture.handlers.get("session_start")!({},context);
    const run=()=>fixture.tools.get("acyclic_spawn")!.execute("spawn",{id:"child",prompt:"instruction",context:"native_cut"},undefined,undefined,context);
    if(mutation==="unchanged") await run();
    else await assert.rejects(run,/Native source changed before admission/);
    const manifest=JSON.parse(Buffer.from(files.at(-1)!.bytes).toString());
    assert.ok(manifest.chunks>1);
    const source=Buffer.concat(files.slice(0,-1).map(file=>Buffer.from(file.bytes)));
    assert.equal(source.toString(),before);
    assert.equal(source.length,manifest.length);
    assert.equal(calls.length,mutation==="unchanged"?1:0);
    if(mutation==="unchanged") {
      const declaration=(calls[0]!.input as {executor:{context:{manifest:string,head:string,sha256:number[]}}}).executor.context;
      assert.equal(declaration.head,manager.getLeafId());
      assert.equal(declaration.manifest,files.at(-1)!.key.Input);
      assert.deepEqual(declaration.sha256,Array.from(createHash("sha256").update(source).digest()));
    }
  }
});

test("session activation preserves every additive tool and enables every harness tool",async () => {
  await fc.assert(fc.asyncProperty(fc.array(fc.string({maxLength:16}),{maxLength:16}),async initial => {
    const fixture=setup(async event=>event === "catalog" ? {tools:definitions} : {},initial);
    await fixture.handlers.get("session_start")!({},fixture.context);
    const active=fixture.active();
    for (const name of [...initial,...definitions.map(tool=>tool.name)]) assert.ok(active.includes(name));
    assert.equal(new Set(active).size,active.length);
    for (const name of active) assert.ok(initial.includes(name) || definitions.some(tool=>tool.name === name));
  }),{numRuns:128});
});

test("Pi cannot fall back to a built-in after a missing or failed harness result", async () => {
  for (const answer of [{block:true,reason:"rejected"},{content:[],isError:true},{}]) {
    const {handlers,tools,context} = setup(async event => event === "catalog" ? {tools:definitions} : answer);
    await handlers.get("session_start")!({},context);
    await assert.rejects(()=>tools.get("bash")!.execute("call",{},undefined,undefined,context));
  }
});

test("Pi preserves foreign-tool refusals and rejects malformed decisions", async () => {
  const foreign = {toolName:"foreign-mcp",toolCallId:"host-call",input:{}};
  const denied = setup(async () => ({block:true,reason:"call was not recorded"}));
  assert.deepEqual(await denied.handlers.get("tool_call")!(foreign,denied.context),
    {block:true,reason:"call was not recorded"});
  for (const answer of [null,[],{},false,{block:"false"},{block:false,reason:1},{isError:true}]) {
    const fixture = setup(async () => answer as never);
    await assert.rejects(() => fixture.handlers.get("tool_call")!(foreign,fixture.context),
      /Invalid harness native-tool decision/);
  }
});

test("only an explicit permission can release a foreign tool after a JSON round trip", async () => {
  await fc.assert(fc.asyncProperty(fc.jsonValue(),async value => {
    const answer = JSON.parse(JSON.stringify(value));
    const fixture = setup(async () => answer);
    let result: unknown;
    try { result = await fixture.handlers.get("tool_call")!({toolName:"foreign-mcp"},fixture.context); }
    catch { return; }
    if (result === undefined) assert.equal(answer.block,false);
    else assert.equal((result as {block:unknown}).block,true);
  }),{numRuns:256});
  await fc.assert(fc.asyncProperty(fc.boolean(),fc.string(),async (block,reason) => {
    const fixture = setup(async () => JSON.parse(JSON.stringify({block,reason})));
    assert.deepEqual(await fixture.handlers.get("tool_call")!({toolName:"foreign-mcp"},fixture.context),
      block ? {block:true,reason} : undefined);
  }),{numRuns:256});
});
