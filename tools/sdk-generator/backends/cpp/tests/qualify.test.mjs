import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import test from "node:test";
import { qualify } from "../src/qualify.mjs";
import { fixture } from "./fixtures/qualification.mjs";

test("fresh archive-installed qualifier verifies compiler, runtime, native controls and receipt", t => {
  const f = fixture(t), result = qualify(f.args, f.options);
  assert.equal(result.native_rpc_method_calls, 25); assert.equal(result.independent_negative_compiles, 3);
  assert.equal(result.host_sdk_files, 9); assert.equal(result.runtime_files, 1); assert.equal(result.rust_backed_rpc_qualified, false);
  assert.equal(f.calls.length, 12);
  for (const [name, bytes] of f.pkg.payload) assert.deepEqual(readFileSync(join(f.args.output, "sdk-source", name)), bytes);
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), result);
});

test("changed compiler SDK, runtime and external inventory reject before commands", t => {
  for (const mutate of [
    f => writeFileSync(join(f.hostRoots[0].root, "header.hpp"), "changed"),
    f => writeFileSync(join(f.hostRoots[0].root, "extra.hpp"), "extra"),
    f => writeFileSync(join(f.runtimeRoot, "include/runtime.hpp"), "changed"),
    f => writeFileSync(f.args["host-inventory"], "changed"),
  ]) { const f = fixture(t); mutate(f); assert.throws(() => qualify(f.args, f.options)); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false); }
});

test("failed native commands, markers and unrelated negative errors cannot qualify", t => {
  for (const [phase, mutate] of [
    ["cmake-version", r => { r.stdout = "wrong"; }], ["compiler-version", r => { r.stdout = "wrong"; }],
    ...["configure-sdk", "build-sdk", "install-sdk", "configure-consumer", "build-consumer", "consumer"].map(name => [name, r => { r.status = 1; }]),
    ["consumer", r => { r.stdout = "no completion marker"; }],
    ["negative-InvalidActorBytes", r => { r.status = 0; }],
    ["negative-InvalidWorkerBytes", r => { r.stdout = "unrelated compiler failure"; }],
    ["negative-InvalidOptionalInteger", r => { r.stdout = "fatal error C1083: missing file"; }],
  ]) { const f = fixture(t); f.hooks[phase] = mutate; assert.throws(() => qualify(f.args, f.options)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false); }
});

test("post-execution source, compiler, graph, prefix and private runtime drift reject", t => {
  for (const mutate of [
    f => writeFileSync(join(f.args.output, "sdk-source/src/actors/v1/actors.pb.cc"), "changed"),
    f => writeFileSync(join(f.args.output, "sdk-source/extra.cc"), "extra"),
    f => writeFileSync(join(f.args.output, "consumer/rpc_consumer.cc"), "changed"),
    f => writeFileSync(join(f.args.output, "runtime/include/runtime.hpp"), "changed"),
    f => writeFileSync(join(f.hostRoots[0].root, "header.hpp"), "changed"),
    f => writeFileSync(join(f.args.output, "sdk-build/compile_commands.json"), "[]"),
    f => writeFileSync(join(f.args.output, "consumer-build/CMakeCache.txt"), "Protobuf_DIR:PATH=outside"),
  ]) { const f = fixture(t); f.hooks["negative-InvalidOptionalInteger"] = () => mutate(f); assert.throws(() => qualify(f.args, f.options)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false); }
});

test("existing and input-overlapping outputs preserve state", t => {
  const existing = fixture(t); mkdirSync(existing.args.output); writeFileSync(join(existing.args.output, "keep"), "keep");
  assert.throws(() => qualify(existing.args, existing.options), /must be absent/); assert.equal(readFileSync(join(existing.args.output, "keep"), "utf8"), "keep");
  const overlap = fixture(t); overlap.args.output = join(overlap.runtimeRoot, "new");
  assert.throws(() => qualify(overlap.args, overlap.options), /overlaps/); assert.equal(overlap.calls.length, 0);
});
