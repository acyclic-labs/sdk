#!/usr/bin/env node
// Shipping GraphCoder, a real shell and stock Git; no Git metadata workaround.
import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { chmodSync, lstatSync, mkdirSync, readFileSync, readdirSync, readlinkSync, rmSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { isAbsolute, join, resolve } from "node:path";
import { Processes } from "./graphcoder-e2e/process.mjs";
import { environment, scratchDirectory } from "./graphcoder-e2e/scratch.mjs";

const [binary] = process.argv.slice(2);
assert.ok(binary && isAbsolute(binary), "absolute shipping GraphCoder required");
assert.notEqual(process.platform, "win32", "read-only directory proof requires POSIX permissions");
const root = scratchDirectory("graphcoder-workspace-state-");
const processes = new Processes(), settings = join(root, "settings");
const env = environment(settings);
const captures = [];
let passed = false, active, rejectFailure;
const failed = new Promise((_, reject) => { rejectFailure = reject; });
failed.catch(() => {});
const sse = value => `data: ${JSON.stringify(value)}\n\n`;

// Record all Git metadata bytes, links and permissions. Reading it belongs to
// this independent oracle, never to the harness implementation.
function metadata(path, writable) {
  const stat = lstatSync(path);
  if (writable !== undefined && !stat.isSymbolicLink()) chmodSync(path, writable ? stat.mode | 0o700 : stat.mode & ~0o222);
  const mode = lstatSync(path).mode & 0o777;
  if (stat.isSymbolicLink()) return { mode, link: readlinkSync(path) };
  if (stat.isDirectory()) return { mode, entries: Object.fromEntries(readdirSync(path).sort().map(name => [name, metadata(join(path, name), writable)])) };
  return { mode, sha256: createHash("sha256").update(readFileSync(path)).digest("hex") };
}

async function git(cwd, args) {
  const result = await processes.run("git", args, { env, cwd });
  assert.equal(result.code, 0, result.stderr);
  return result.stdout;
}

const server = createServer(async (request, response) => {
  try {
    let bytes = ""; for await (const chunk of request) bytes += chunk;
    const body = JSON.parse(bytes);
    if (request.url === "/count") { response.end(JSON.stringify({ input_tokens: 64 })); return; }
    assert.equal(request.url, "/responses");
    const step = active.requests++;
    response.setHeader("content-type", "text/event-stream");
    if (step === 0) {
      const expected = active.git;
      const code = `
const edited = await tools.shell({command:"printf 'model edit\\n' > tracked.txt; printf '%040000d' 0"});
if (!edited.success || !edited.spill) throw Error(JSON.stringify(edited));
async function readAll(path) { let page=await tools.read({path}); const source=page.file.authored??page.file.content; const version=page.file.authored?'authored-path-v2:'+JSON.stringify({volume:source.volume,path:source.path,generation:source.generation}):JSON.stringify(source); const length=page.file.authored?source.byte_length:source.descriptor.byte_length; let text=page.text; while(page.page?.next!=null) { const start=page.page.next; page=await tools.read({path,version,range:{start,end:length}}); if(page.page?.next!=null && page.page.next<=start) throw Error('private file page did not advance'); text+=page.text; } return text; }
if ((await readAll(edited.spill)) !== '0'.repeat(40000)) throw Error('overflow not readable through filesystem tools');
const status = await tools.shell({command:"git status --porcelain=v1 --untracked-files=all"});
const diff = await tools.shell({command:"git diff -- tracked.txt"});
if (${expected}) {
  if (!status.success || status.stdout.trim() !== 'M tracked.txt') throw Error('unclean status: '+JSON.stringify(status));
  if (!diff.success || !diff.stdout.includes('-original') || !diff.stdout.includes('+model edit')) throw Error('missing real diff: '+JSON.stringify(diff));
} else {
  if (status.success || diff.success || !status.stderr.includes('not a git repository')) throw Error('plain directory Git boundary: '+JSON.stringify({status,diff}));
}
export default {proof:'WORKSPACE_STATE_OK',status,diff};`;
      response.write(sse({ type: "response.output_item.done", output_index: 0,
        item: { type: "function_call", call_id: `workspace-${active.git}`, name: "code", arguments: JSON.stringify({ code }) } }));
    } else {
      assert.equal(step, 1, "one model shell sequence before completion");
      const outputs = body.input.filter(item => item.type === "function_call_output");
      assert.ok(JSON.stringify(outputs).includes("WORKSPACE_STATE_OK"), JSON.stringify(outputs));
      response.write(sse({ type: "response.output_item.done", output_index: 0,
        item: { type: "message", role: "assistant", content: [{ type: "output_text", text: "WORKSPACE_STATE_DONE" }] } }));
    }
    response.end(sse({ type: "response.completed", response: { status: "completed", usage: { input_tokens: 64, output_tokens: 8 } } }));
  } catch (error) { rejectFailure(error); response.end(); }
});
await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
const catalog = join(root, "catalog.json");
writeFileSync(catalog, JSON.stringify({ fixture: { adapter: "responses", authentication: "optional", base_url: `http://127.0.0.1:${server.address().port}`,
  models: { tiny: { capacity: { context_tokens: 32768, output_tokens: 1024 }, features: { tools: true, images: false } } }, max_event_bytes: 65536,
  token_counter: { path: "count", adapter: "responses", response_field: "input_tokens", framing_tokens: 0 } } }));
let gitDirectory;
try {
  const source = join(root, "source"); mkdirSync(source);
  await git(source, ["init", "-q"]);
  writeFileSync(join(source, "tracked.txt"), "original\n");
  await git(source, ["add", "tracked.txt"]);
  await git(source, ["-c", "user.name=Fixture", "-c", "user.email=fixture@example.invalid", "-c", "commit.gpgsign=false", "commit", "-qm", "original"]);
  for (const repository of [true, false]) {
    const workspace = join(root, repository ? "clone" : "plain");
    if (repository) await git(root, ["clone", source, workspace]);
    else { mkdirSync(workspace); writeFileSync(join(workspace, "tracked.txt"), "original\n"); }
    let before;
    if (repository) {
      gitDirectory = join(workspace, ".git"); before = metadata(gitDirectory, false);
      assert.throws(() => writeFileSync(join(gitDirectory, "info/exclude"), "unexpected", { flag: "a" }), { code: "EACCES" });
    }
    active = { git: repository, requests: 0 };
    const result = await Promise.race([failed, processes.run(binary, ["--default-config", "--workspace", workspace,
      "--workspace-mounting", "physical", "--host-state-directory", join(root, repository ? "clone-state" : "plain-state"),
      "--settings-root", settings, "--catalog", catalog, "--model", "fixture/tiny", "--no-compaction", "--operation", randomUUID(),
      "-p", "Edit tracked.txt, then inspect the workspace with real git through shell."], { env, cwd: workspace })]);
    captures.push({ repository, ...result });
    assert.equal(result.code, 0, result.stderr + result.stdout.slice(-4000));
    assert.equal(active.requests, 2);
    assert.ok(result.stdout.includes("WORKSPACE_STATE_DONE"));
    assert.equal(readFileSync(join(workspace, "tracked.txt"), "utf8"), "model edit\n");
    assert.deepEqual(readdirSync(workspace).sort(), repository ? [".git", "tracked.txt"] : ["tracked.txt"]);
    if (repository) {
      assert.equal((await git(workspace, ["status", "--porcelain=v1", "--untracked-files=all"])).trim(), "M tracked.txt");
      assert.deepEqual(metadata(gitDirectory), before);
    }
  }
  passed = true;
  console.log("PASS read-only stock Git clone and non-Git workspace: edits only, external harness state");
} finally {
  await processes.close(); server.closeAllConnections(); await new Promise(resolve => server.close(resolve));
  if (gitDirectory) metadata(gitDirectory, true);
  if (process.env.E2E_OUTPUT) {
    const output = resolve(process.env.E2E_OUTPUT); mkdirSync(output, { recursive: true });
    writeFileSync(join(output, "workspace-state.json"), JSON.stringify({ passed, captures, fixture: passed ? null : root }));
  }
  if (passed) rmSync(root, { recursive: true, force: true });
  else console.error(`WORKSPACE_STATE_FAILURE_FIXTURE ${root}`);
}
