import assert from "node:assert/strict";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { inventorySDK } from "../src/inventory-sdk.mjs";

function fixture(t) {
  const root=mkdtempSync(join(tmpdir(),"swift-inventory-"));
  t.after(()=>rmSync(root,{recursive:true}));
  const sdk=join(root,"sdk");mkdirSync(join(sdk,"usr/bin"),{recursive:true});mkdirSync(join(sdk,"usr/lib"));
  writeFileSync(join(sdk,"usr/bin/swift"),"driver");writeFileSync(join(sdk,"usr/lib/runtime"),"library");
  const inventory={archive_sha256:"a".repeat(64),platform:"Ubuntu 22.04 x86_64",swift_version:"Swift fixture",files:{"usr/bin/swift":sha256("driver"),"usr/lib/runtime":sha256("library")},links:{}};
  const bytes=JSON.stringify(inventory,null,2)+"\n";
  const options={toolchain:{sdk_files:2,sdk_links:0,sdk_archive_sha256:inventory.archive_sha256,sdk_inventory_sha256:sha256(bytes)},swiftVersion:inventory.swift_version};
  return {sdk,args:{"swift-home":sdk,output:join(root,"inventory.json")},options,bytes};
}

test("SDK inventory generation reproduces exact canonical artifact bytes",t=>{
  const f=fixture(t),result=inventorySDK(f.args,f.options);
  assert.equal(readFileSync(f.args.output,"utf8"),f.bytes);assert.equal(result.sha256,sha256(f.bytes));assert.equal(result.files,2);
});

test("changed, extra and missing SDK inputs cannot produce an admitted inventory",t=>{
  for(const mutate of [
    f=>writeFileSync(join(f.sdk,"usr/bin/swift"),"drift"),
    f=>writeFileSync(join(f.sdk,"extra"),"extra"),
    f=>rmSync(join(f.sdk,"usr/lib/runtime")),
  ]){const f=fixture(t);mutate(f);assert.throws(()=>inventorySDK(f.args,f.options));assert.equal(existsSync(f.args.output),false);}
});

test("existing and overlapping inventory output is preserved",t=>{
  const f=fixture(t);writeFileSync(f.args.output,"caller bytes");
  assert.throws(()=>inventorySDK(f.args,f.options),/absent/);assert.equal(readFileSync(f.args.output,"utf8"),"caller bytes");
  f.args.output=join(f.sdk,"new-inventory.json");assert.throws(()=>inventorySDK(f.args,f.options),/overlaps/);
});
