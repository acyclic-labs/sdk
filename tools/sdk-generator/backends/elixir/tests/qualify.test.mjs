import assert from "node:assert/strict";
import { cpSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import test from "node:test";
import { qualify } from "../src/qualify.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture as packageFixture } from "./fixtures/package.mjs";
import { dependencyFixture } from "./fixtures/dependencies.mjs";

const messageMarker = "PASS Elixir Rust message/enum/service descriptors, 25 populated RPC message pairs, bytes, unsigned bounds, optional presence, oneofs and 3 runtime type rejections";
const sourceMarker = "PASS Elixir exact compiled SDK module inventory, source paths and loaded BEAM paths";
function fixture(t) {
  const pkg = packageFixture(t), deps = dependencyFixture(t), root = dirname(pkg.args.package);
  const runtime = join(root, "runtime"), runtimePin = { otp_version: "29.1.1", otp_release: "29", elixir_version: "1.20.4",
    roots: ["OTP-fixture", "elixir-fixture"], files: 3, links: 0, otp_archive_sha256: "a".repeat(64), elixir_archive_sha256: "b".repeat(64) };
  const sdkFiles = { "OTP-fixture/bin/erl": Buffer.from("OTP fixture"), "elixir-fixture/bin/elixir": Buffer.from("Elixir fixture"), "elixir-fixture/bin/mix": Buffer.from("Mix fixture") };
  for (const [name, bytes] of Object.entries(sdkFiles)) { const target = join(runtime, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes); }
  const index = Buffer.from(JSON.stringify({ ...runtimePin, files_sha256: Object.fromEntries(Object.entries(sdkFiles).map(([name, bytes]) => [name, sha256(bytes)])), links: {} }));
  runtimePin.inventory_sha256 = sha256(index);
  const runtimeIndex = join(root, "runtime.json"), toolIndex = join(root, "tools.json"); writeFileSync(runtimeIndex, index); writeFileSync(toolIndex, deps.toolIndex);
  const args = { ...pkg.args, "runtime-root": runtime, "runtime-inventory": runtimeIndex, "tool-home": deps.toolHome, "tool-inventory": toolIndex, cache: deps.cache, output: join(root, "qualified") };
  const calls = [], hooks = {};
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    assert.equal(options.env.HEX_OFFLINE, "1"); assert.equal(options.env.HEX_API_URL, undefined); assert.equal(options.env.HEX_MIRROR, undefined);
    assert.equal(options.env.ERL_FLAGS, "+S 1:1"); assert.equal(options.env.MIX_ENV, "prod");
    assert.equal(options.env.MIX_BUILD_PATH, join(args.output, "build"));
    let phase, stdout = "", status = 0;
    if (argv[0] === "-e") { phase = "version"; stdout = "1.20.4\n29\n"; }
    else if (argv[0] === "hex.info") { phase = "hex"; stdout = "Hex:    fixture\n"; }
    else if (argv[0] === "deps.get") { phase = "deps"; cpSync(deps.deps, join(args.output, "consumer/deps"), { recursive: true }); }
    else if (argv[0] === "compile") { phase = "compile"; mkdirSync(join(args.output, "build/lib/acyclic_sdk_transport/ebin"), { recursive: true }); writeFileSync(join(args.output, "build/lib/acyclic_sdk_transport/ebin/Test.beam"), "compiled fixture"); }
    else if (argv.includes("Provenance.exs")) { phase = "provenance"; stdout = sourceMarker + "\n"; }
    else if (argv.includes("Consumer.exs")) { phase = "consumer"; stdout = messageMarker + "\n"; }
    else throw new Error("unexpected command");
    const result = { status, stdout, stderr: "" }; hooks[phase]?.(result, options);
    return result;
  };
  const options = { command, toolchain: deps.pins, runtimePin, lockBytes: deps.lock };
  return { args, options, pkg, deps, hooks, calls, runtime };
}

test("qualifier stages only admitted archives, pins offline tools and writes success after controls", t => {
  const f = fixture(t), receipt = qualify(f.args, f.options);
  assert.equal(receipt.populated_rpc_message_pairs, 25); assert.equal(receipt.runtime_type_rejections, 3);
  assert.equal(receipt.native_rpc_calls_executed, false); assert.equal(receipt.compiled_sdk_provenance, true);
  assert.equal(Object.keys(receipt.installed_dependencies).length, 6);
  assert.equal(f.calls.length, 6);
  for (const [name, bytes] of f.pkg.payload) assert.deepEqual(readFileSync(join(f.args.output, "sdk", name)), bytes);
  assert.deepEqual(JSON.parse(readFileSync(join(f.args.output, "qualification.json"))), receipt);
});

test("changed tools, runtime and missing dependency archives reject before native commands", t => {
  for (const mutate of [
    f => writeFileSync(join(f.runtime, "OTP-fixture/bin/erl"), "changed"),
    f => writeFileSync(join(f.deps.toolHome, ".mix/archives/hex-fixture/ebin/hex.beam"), "changed"),
    f => rmSync(join(f.deps.cache, "protobuf-1.0.0.tar")),
    f => { f.options.toolchain.lock_sha256 = "0".repeat(64); },
  ]) { const f = fixture(t); mutate(f); assert.throws(() => qualify(f.args, f.options)); assert.equal(f.calls.length, 0); assert.equal(existsSync(f.args.output), false); }
});

test("native command failures, wrong versions and missing completion markers cannot qualify", t => {
  for (const [phase, mutate] of [
    ["version", r => { r.stdout = "wrong version"; }], ["hex", r => { r.stdout = "Hex: other"; }],
    ...["deps", "compile", "provenance", "consumer"].map(phase => [phase, r => { r.status = 1; r.stderr = "intended command failure"; }]),
    ["provenance", r => { r.stdout = "no marker"; }], ["consumer", r => { r.stdout = "no marker"; }],
  ]) { const f = fixture(t); f.hooks[phase] = mutate; assert.throws(() => qualify(f.args, f.options)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false); }
});

test("source, dependency, lock and copied tool mutation after execution cannot qualify", t => {
  for (const mutate of [
    f => writeFileSync(join(f.args.output, "sdk/lib/acyclic/actors/v1/actors.pb.ex"), "changed"),
    f => writeFileSync(join(f.args.output, "sdk/extra.ex"), "extra"),
    f => writeFileSync(join(f.args.output, "consumer/deps/protobuf/lib/source.ex"), "changed"),
    f => writeFileSync(join(f.args.output, "consumer/mix.lock"), "changed"),
    f => writeFileSync(join(f.args.output, "home/.mix/archives/hex-fixture/ebin/hex.beam"), "changed"),
    f => writeFileSync(join(f.args.output, "home/.mix/archives/hex-fixture/ebin/extra.beam"), "extra"),
    f => writeFileSync(join(f.runtime, "OTP-fixture/bin/erl"), "changed"),
  ]) { const f = fixture(t); f.hooks.consumer = () => mutate(f); assert.throws(() => qualify(f.args, f.options)); assert.equal(existsSync(join(f.args.output, "qualification.json")), false); }
});

test("existing and overlapping outputs preserve inputs without native execution", t => {
  const existing = fixture(t); mkdirSync(existing.args.output); writeFileSync(join(existing.args.output, "keep"), "keep");
  assert.throws(() => qualify(existing.args, existing.options), /must be absent/); assert.equal(readFileSync(join(existing.args.output, "keep"), "utf8"), "keep");
  const overlap = fixture(t); overlap.args.output = join(overlap.runtime, "new");
  assert.throws(() => qualify(overlap.args, overlap.options), /overlaps/); assert.equal(overlap.calls.length, 0);
});
