import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { test } from "bun:test";
import { fileURLToPath } from "node:url";
import { installedNativeFixture, type InstalledNativeFixture } from "./pi-native-fixture.ts";
const provider = `
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai";
export default function directoryPages(pi) {
  let session;
  pi.on("session_start", (_event, ctx) => { session = ctx.sessionManager.getSessionId(); });
  const model = fauxProvider({provider:"acyclic-native-qualification", models:[{id:"probe"}]});
  const response = context => {
    const results = context.messages.filter(message => message.role === "toolResult");
    const get = id => results.find(result => result.toolCallId === id);
    const read = id => {
      const result = get(id);
      if (!result || result.isError) throw new Error("actual selected page missing: " + id);
      return JSON.parse(result.content.filter(part=>part.type==="text").map(part=>part.text).join(""));
    };
    const call = (id, name, args) => fauxAssistantMessage(fauxToolCall(name, args, {id}), {stopReason:"toolUse"});
    for (const result of results) if (result.isError && !result.toolCallId.startsWith("deny-")) {
      throw new Error("actual production page failed: " + JSON.stringify(result));
    }
    if (!get("inspect-first")) return call("inspect-first", "acyclic_inspect", {id:session,directory:"nested",maximum_entries:1});
    const inspected = read("inspect-first");
    if (inspected.directory !== "nested" || typeof inspected.cursor !== "string") throw new Error("selected directory/opaque cursor contract changed");
    if (!inspected.volume || !inspected.volume.owner || inspected.volume.owner.kind !== "project" ||
        typeof inspected.volume.owner.id !== "string") throw new Error("actual selected root has no project RootID metadata");
    const root = inspected.volume.owner.id;
    if (!get("inspect-explicit-root")) return call("inspect-explicit-root", "acyclic_inspect", {id:session,root,directory:"nested",cursor:inspected.cursor,maximum_entries:1});
    if (!get("deny-inspect-root-alias")) return call("deny-inspect-root-alias", "acyclic_inspect", {id:session,root:".",directory:"nested",maximum_entries:1});
    if (!get("inspect-next")) return call("inspect-next", "acyclic_inspect", {id:session,directory:"nested",cursor:inspected.cursor,maximum_entries:1});
    if (!get("deny-inspect-directory")) return call("deny-inspect-directory", "acyclic_inspect", {id:session,directory:"other",cursor:inspected.cursor,maximum_entries:1});
    if (!get("inspect-repeat")) return call("inspect-repeat", "acyclic_inspect", {id:session,directory:"nested",cursor:inspected.cursor,maximum_entries:1});
    if (!get("deny-inspect-path")) return call("deny-inspect-path", "acyclic_inspect", {id:session,directory:"../private",maximum_entries:1});
    if (!get("diff-first")) return call("diff-first", "acyclic_diff", {id:session,directory:"nested",maximum_entries:1});
    const diff = read("diff-first");
    if (typeof diff.cursor !== "string") throw new Error("bounded native diff lost its original directory continuation");
    if (!get("diff-explicit-root")) return call("diff-explicit-root", "acyclic_diff", {id:session,root,directory:"nested",cursor:diff.cursor,maximum_entries:1});
    if (!get("deny-diff-root-alias")) return call("deny-diff-root-alias", "acyclic_diff", {id:session,root:".",directory:"nested",maximum_entries:1});
    if (!get("diff-next")) return call("diff-next", "acyclic_diff", {id:session,directory:"nested",cursor:diff.cursor,maximum_entries:1});
    if (!get("deny-diff-directory")) return call("deny-diff-directory", "acyclic_diff", {id:session,directory:"other",cursor:diff.cursor,maximum_entries:1});
    if (!get("deny-diff-inspection-cursor")) return call("deny-diff-inspection-cursor", "acyclic_diff", {id:session,directory:"nested",cursor:inspected.cursor,maximum_entries:1});
    if (!get("diff-repeat")) return call("diff-repeat", "acyclic_diff", {id:session,directory:"nested",cursor:diff.cursor,maximum_entries:1});
    if (!get("deny-diff-path")) return call("deny-diff-path", "acyclic_diff", {id:session,directory:"nested/..",maximum_entries:1});
    return fauxAssistantMessage(JSON.stringify({session,results:results.map(result=>({id:result.toolCallId,isError:result.isError,
      text:result.content.filter(part=>part.type==="text").map(part=>part.text).join("")}))}));
  };
  model.setResponses(Array.from({length:32},()=>response));
  pi.registerProvider(model.provider);
}
`;

test("native directory selection preserves opaque continuations and rejects cross-directory or cross-tool cursors", async () => {
  const directory = mkdtempSync(fileURLToPath(new URL(".pi-context-pages-", import.meta.url)));
  let fixture: InstalledNativeFixture | undefined;
  try {
    fixture = await installedNativeFixture(directory, provider, {});
    for (const name of ["nested", "other"]) {
      mkdirSync(join(fixture.project, name));
      writeFileSync(join(fixture.project, name, "one.txt"), `${name} original one\n`);
      writeFileSync(join(fixture.project, name, "two.txt"), `${name} original two\n`);
    }
    await fixture.start();
    const execution = await fixture.run("Inspect the exact selected native nested directory; do not reinterpret service cursors.");
    const report = JSON.parse(execution.stdout.trim());
    assert.equal(report.session, fixture.manager.getSessionId());
    const results = new Map<string, { id: string; isError: boolean; text: string }>(
      report.results.map((result: { id: string; isError: boolean; text: string }) => [result.id, result]),
    );
    for (const id of ["inspect-first", "inspect-next", "inspect-repeat", "inspect-explicit-root", "diff-first", "diff-next", "diff-repeat", "diff-explicit-root"]) {
      assert.equal(results.get(id)?.isError, false, `${id} must use the actual selected directory and original cursor`);
    }
    for (const id of ["deny-inspect-directory", "deny-inspect-path", "deny-inspect-root-alias", "deny-diff-directory", "deny-diff-inspection-cursor", "deny-diff-path", "deny-diff-root-alias"]) {
      assert.equal(results.get(id)?.isError, true, `${id} must fail at the canonical service selector boundary`);
    }
    const first = JSON.parse(results.get("inspect-first")!.text);
    const next = JSON.parse(results.get("inspect-next")!.text);
    const repeated = JSON.parse(results.get("inspect-repeat")!.text);
    assert.equal(first.directory, "nested");
    assert.equal(next.directory, "nested");
    assert.equal(first.entries.length, 1);
    assert.equal(next.entries.length, 1);
    assert.deepEqual(new Set([...first.entries, ...next.entries].map(entry => entry.name)), new Set(["one.txt", "two.txt"]));
    for (const field of ["source", "volume", "snapshot", "directory", "entries", "cursor"]) {
      assert.deepEqual(repeated[field], next[field], `failed foreign-directory selection must preserve the original ${field}`);
      assert.deepEqual(JSON.parse(results.get("inspect-explicit-root")!.text)[field], next[field], `explicit RootID must preserve the selected ${field}`);
    }
    const diffNext = JSON.parse(results.get("diff-next")!.text);
    const diffRepeated = JSON.parse(results.get("diff-repeat")!.text);
    assert.ok(Array.isArray(diffNext.entries));
    assert.ok(diffNext.entries.length <= 1);
    assert.deepEqual(diffRepeated.entries, diffNext.entries);
    assert.deepEqual(diffRepeated.cursor, diffNext.cursor);
    const explicitDiff = JSON.parse(results.get("diff-explicit-root")!.text);
    assert.deepEqual(explicitDiff.entries, diffNext.entries);
    assert.deepEqual(explicitDiff.cursor, diffNext.cursor);
  } finally {
    if (fixture) await fixture.close();
    rmSync(directory, { recursive: true });
  }
}, { timeout: 240_000 });
