import assert from "node:assert/strict";
import { join } from "node:path";
import { writeFileSync } from "node:fs";
import test from "node:test";
import { gzipSync } from "node:zlib";
import { readDependencies, readTools, verifyInstalledDependencies, verifyLock } from "../src/provenance.mjs";
import { dependencyFixture } from "./fixtures/dependencies.mjs";
import { tar } from "./fixtures/package.mjs";

test("all locked Hex archive source bytes and installed tool files are admitted", t => {
  const f = dependencyFixture(t), admitted = readDependencies(f.cache, f.pins);
  verifyLock(f.lock, admitted, f.pins);
  assert.equal(Object.keys(verifyInstalledDependencies(f.deps, admitted)).length, 6);
  assert.equal(readTools(f.toolHome, f.toolIndex, f.pins).size, 3);
});

test("dependency archive, lock and coordinate substitutions reject", t => {
  for (const mutate of [
    f => { f.pins.dependencies[0].archive_sha256 = "0".repeat(64); },
    f => { f.pins.dependencies[0].name = "../escape"; },
    f => { f.pins.dependencies.push(f.pins.dependencies[0]); },
  ]) { const f = dependencyFixture(t); mutate(f); assert.throws(() => readDependencies(f.cache, f.pins)); }
  const f = dependencyFixture(t); assert.throws(() => verifyLock(Buffer.from("substituted"), readDependencies(f.cache, f.pins), f.pins), /lock differs/);
});

test("unsafe source envelopes, links, traversal and case aliases reject", t => {
  for (const mutate of [
    members => { members[0].type = "2"; },
    members => { members.push(members[0]); },
    members => { members[2].bytes = gzipSync(tar([{ name: "../escape", bytes: Buffer.from("bad") }])); },
    members => { members[2].bytes = gzipSync(tar([{ name: "lib/a.ex", bytes: Buffer.from("a"), type: "2" }])); },
    members => { members[2].bytes = gzipSync(tar([{ name: "lib/a.ex", bytes: Buffer.from("a") }, { name: "lib/A.ex", bytes: Buffer.from("b") }])); },
  ]) { const f = dependencyFixture(t), members = f.entries.protobuf.members; mutate(members); f.replaceArchive("protobuf", members); assert.throws(() => readDependencies(f.cache, f.pins)); }
});

test("changed compiled sources, extra files and substituted Hex metadata reject", t => {
  for (const [name, content, error] of [["lib/source.ex", "changed", /compiled dependency source differs/], ["extra.ex", "extra", /unexpected compiled source inventory/], ["hex_metadata.config", "substituted", /Hex metadata differs/]]) {
    const f = dependencyFixture(t), admitted = readDependencies(f.cache, f.pins);
    writeFileSync(join(f.deps, "protobuf", name), content);
    assert.throws(() => verifyInstalledDependencies(f.deps, admitted), error);
  }
});

test("qualification tool changes and unlisted Hex code reject", t => {
  const changed = dependencyFixture(t); writeFileSync(join(changed.toolHome, ".mix/archives/hex-fixture/ebin/hex.beam"), "changed");
  assert.throws(() => readTools(changed.toolHome, changed.toolIndex, changed.pins), /tool payload differs/);
  const extra = dependencyFixture(t); writeFileSync(join(extra.toolHome, ".mix/archives/hex-fixture/ebin/extra.beam"), "extra");
  assert.throws(() => readTools(extra.toolHome, extra.toolIndex, extra.pins), /Hex installation inventory differs/);
});
