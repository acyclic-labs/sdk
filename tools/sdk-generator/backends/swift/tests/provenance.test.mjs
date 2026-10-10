import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { verifySource } from "../src/provenance.mjs";

function source(t, target = "Package.swift") {
  const root = realpathSync(mkdtempSync(join(tmpdir(),"swift-source-proof-")));
  t.after(() => rmSync(root,{recursive:true}));
  writeFileSync(join(root,"Package.swift"),"package"); writeFileSync(join(root,"Link.swift"),target);
  const blob = bytes => createHash("sha1").update(`blob ${Buffer.byteLength(bytes)}\0`).update(bytes).digest("hex");
  const pin = { name:"fixture",version:"1.0.0",revision:"a".repeat(40) };
  const git = (_root,argv) => argv[0]==="rev-parse" ? pin.revision :
    `100644 blob ${blob("package")}\tPackage.swift\0`+`120000 blob ${blob(target)}\tLink.swift\0`;
  return {root,pin,git};
}

test("Windows symlink text is permitted only in Git clone inputs", t => {
  const f=source(t);
  const admitted=verifySource(f.root,f.pin,f.git,{cloneInput:true});
  assert.equal(Object.keys(admitted.files_sha256).length,2);
  assert.throws(()=>verifySource(f.root,f.pin,f.git),/file kind differs/);
});

test("modified or escaping symlink text cannot enter dependency clones", t => {
  const f=source(t); writeFileSync(join(f.root,"Link.swift"),"other");
  mkdirSync(join(f.root,"other"));
  assert.throws(()=>verifySource(f.root,f.pin,f.git,{cloneInput:true}),/pinned Git object/);
  const escaping=source(t,"../outside");
  assert.throws(()=>verifySource(escaping.root,escaping.pin,escaping.git,{cloneInput:true}),/escapes source root/);
});
