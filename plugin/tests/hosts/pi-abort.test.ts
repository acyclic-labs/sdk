import assert from "node:assert/strict";
import { createServer, type Socket } from "node:net";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn } from "node:child_process";
import { test } from "bun:test";
import type { ExtensionAPI, ExtensionContext, ToolDefinition } from "@earendil-works/pi-coding-agent";
import { loadPiExtension } from "./pi-fixture.ts";

const executable = process.env.ACYCLIC_TEST_EXECUTABLE;
if (!executable) throw new Error("ACYCLIC_TEST_EXECUTABLE must name the built native hook executable");

test("Pi abort waits for native process cleanup, records cancellation, and leaves the session usable", async () => {
  const root = mkdtempSync(join(tmpdir(),"acyclic-pi-abort-"));
  const project = join(root,"project");
  mkdirSync(project);
  const previous = process.env.ACYCLIC_LOCAL_OWNER_DIR;
  process.env.ACYCLIC_LOCAL_OWNER_DIR = join(root,"owner");
  const source = readFileSync(new URL("../../pi/index.ts",import.meta.url),"utf8");
  const module = join(root,"shim.ts");
  writeFileSync(module,source.replace(JSON.stringify("__ACYCLIC_EXECUTABLE__"),JSON.stringify(executable)));
  const { default: extension } = await loadPiExtension(module);
  const handlers = new Map<string,(event:unknown,ctx:ExtensionContext)=>Promise<unknown>>();
  const tools = new Map<string,ToolDefinition>();
  extension({
    on(event:string,handler:(event:unknown,ctx:ExtensionContext)=>Promise<unknown>) {handlers.set(event,handler);},
    registerTool(tool:ToolDefinition) {tools.set(tool.name,tool);},
    getActiveTools() {return [];},
    setActiveTools() {},
  } as unknown as ExtensionAPI);
  const context = {cwd:project,sessionManager:{getSessionId:()=>"abort-session"}} as unknown as ExtensionContext;
  const connections: Socket[] = [];
  let connected!: (socket:Socket)=>void;
  const ready = new Promise<Socket>(resolve=>{connected=resolve;});
  const server = createServer(socket=>{
    connections.push(socket);
    socket.on("error",()=>{}); // A reaped native process may reset its test pipe.
    socket.once("data",chunk=>{assert.equal(chunk.toString(),"R");connected(socket);});
  });
  let running: Promise<unknown> | undefined;
  try {
    await new Promise<void>(resolve=>server.listen(0,"127.0.0.1",resolve));
    const address = server.address();
    assert.ok(address && typeof address !== "string");
    await handlers.get("session_start")!({},context);
    const code = `import socket; s=socket.create_connection(("127.0.0.1",${address.port})); s.sendall(b"R"); s.recv(1); open("late.txt","w").write("late"); s.sendall(b"W")`;
    const controller = new AbortController();
    running = tools.get("bash")!.execute("abort-call",{command:`python3 -c '${code}'`},controller.signal,undefined,context);
    // Attach the rejection handler before the external process reaches its cut.
    const settled = assert.rejects(running,{name:"AbortError"});
    const socket = await ready;
    controller.abort();
    await settled;
    // Releasing a still-live process here would cause an effect after settle.
    await new Promise<void>((resolve,reject)=>{
      if (socket.destroyed || socket.readableEnded) {resolve();return;}
      socket.once("data",()=>reject(new Error("native effect landed after abort settlement")));
      socket.once("end",resolve);
      socket.once("close",resolve);
      socket.once("error",()=>resolve());
      socket.write("go");
    });
    assert.equal(existsSync(join(project,"late.txt")),false);
    const written = await tools.get("write")!.execute("after-abort",{path:"after.txt",text:"still attached",media_type:"text/plain",display_name:"after.txt"},undefined,undefined,context);
    assert.ok(written.content.length);
    assert.equal(readFileSync(join(project,"after.txt"),"utf8"),"still attached");
    await handlers.get("session_shutdown")!({},context);
  } finally {
    for (const socket of connections) {socket.write("go");socket.destroy();}
    await running?.catch(()=>{});
    await new Promise<void>(resolve=>server.close(()=>resolve()));
    const stop = spawn(executable,["__owner-stop"],{cwd:project,windowsHide:true,stdio:"pipe"});
    stop.stdin.end();
    await new Promise<void>((resolve,reject)=>{stop.once("error",reject);stop.once("close",()=>resolve());});
    if (previous === undefined) delete process.env.ACYCLIC_LOCAL_OWNER_DIR;
    else process.env.ACYCLIC_LOCAL_OWNER_DIR = previous;
    rmSync(root,{recursive:true,force:true});
  }
});
