import assert from "node:assert/strict";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { basename, dirname, join } from "node:path";
import test from "node:test";
import { pins } from "../src/generate.mjs";
import { qualify } from "../src/qualify.mjs";
import { sha256 } from "../../../shared/authority.mjs";
import { fixture as packageFixture } from "./fixtures/package.mjs";

function fixture(t, fault) {
  const f = packageFixture(t), root = dirname(f.args.package);
  const put = (name, bytes) => { const path = join(root, name); mkdirSync(dirname(path), { recursive: true }); writeFileSync(path, bytes); return path; };
  put("runtime/OTP-29.1.1/bin/erl", "admitted OTP");
  const rt = { schema: "acyclic.sdk.erlang-runtime-inventory.v1", otp_version: "29.1.1", otp_archive_sha256: "a".repeat(64), files_sha256: { "OTP-29.1.1/bin/erl": sha256("admitted OTP") }, links: {} };
  const runtimeBytes = Buffer.from(JSON.stringify(rt)); put("runtime-index.json", runtimeBytes);
  put("tools/tool.beam", "admitted generator");
  const toolIndexBytes = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.erlang-generation-tools.v1", gpb_version: pins.gpb_version, grpcbox_plugin_version: pins.grpcbox_plugin_version, rebar_version: pins.rebar_version, files_sha256: { "tool.beam": sha256("admitted generator") } }));
  put("dependencies/grpcbox-0.18.0/ebin/grpcbox.beam", "admitted runtime dependency");
  const deps = { schema: "acyclic.sdk.erlang-runtime-dependencies.v1", upstream_lock_sha256: "b".repeat(64), packages: { grpcbox: { directory: "grpcbox-0.18.0", application: "grpcbox", version: "0.18.0", archive_sha256: "c".repeat(64) } }, files_sha256: { "grpcbox-0.18.0/ebin/grpcbox.beam": sha256("admitted runtime dependency") } };
  const dependencyIndexBytes = Buffer.from(JSON.stringify(deps));
  Object.assign(f.args, { "runtime-root": join(root, "runtime"), "runtime-inventory": join(root, "runtime-index.json"), "tool-home": join(root, "tools"), dependencies: join(root, "dependencies"), output: join(root, "installed") });
  const calls = [], sdk = join(f.args.output, "libs/acyclic_sdk_transport-0.1.0"), consumer = join(f.args.output, "consumer"), libs = join(f.args.output, "libs");
  const command = (exe, argv, opts) => {
    calls.push({ exe, argv, opts });
    if (exe.endsWith("erlc")) {
      const output = argv[argv.indexOf("-o") + 1];
      for (const source of argv.filter(name => name.endsWith(".erl"))) writeFileSync(join(output, basename(source, ".erl") + ".beam"), "compiled " + basename(source));
      return { status: 0, stdout: "compiled offline fixture\n" };
    }
    for (const name of ["actors", "workers", "stream"]) writeFileSync(join(consumer, name + "_pb_route_fixture.erl"), "dynamic fixture " + name);
    fault?.({ ...f, root, sdk, consumer, libs });
    return { status: 0, stdout: ["PASS installed SDK and loaded-module provenance", "PASS archive-installed Erlang clients: 26 populated native TCP RPC pairs",
      ...["actors", "workers", "stream"].map(name => name + "_pb differing modeled file descriptor fields: []"), "PASS installed SDK and loaded-module provenance"].join("\n") };
  };
  const options = { command, runtimePin: { ...pins.runtime["linux-x64"], inventory_sha256: sha256(runtimeBytes), otp_archive_sha256: rt.otp_archive_sha256, files: 1, links: 0 },
    toolchain: { ...pins, generator_inventory_sha256: sha256(toolIndexBytes), generator_files: 1 }, toolIndexBytes, dependencyIndexBytes,
    dependencyPin: { packages: deps.packages, upstream_lock_sha256: deps.upstream_lock_sha256, dependency_inventory_sha256: sha256(dependencyIndexBytes), dependency_files: 1 } };
  return { ...f, root, calls, options, sdk, consumer, libs };
}

test("offline lifecycle stages the complete package and private runtime without inherited tool paths", t => {
  const f = fixture(t), result = qualify(f.args, f.options);
  assert.equal(f.calls.length, 3);
  assert.equal(f.calls[2].opts.env.ERL_LIBS, f.libs);
  assert.equal(f.calls[2].opts.cwd, f.consumer);
  assert.equal(f.calls[2].opts.env.ERL_FLAGS, "+S 1:1 +fnu");
  assert.equal(f.calls[2].argv.includes(join(f.sdk, "ebin")), true);
  assert.equal(readFileSync(join(f.sdk, "authority/stream/v1/stream.proto.bin"), "utf8"), "Rust descriptor stream");
  assert.equal(result.compiled_sdk_sha256["actors_pb.beam"], sha256("compiled actors_pb.erl"));
  assert.equal(result.unknown_descriptor_extensions_qualified, false);
  assert.equal(existsSync(join(f.args.output, "qualification.json")), true);
});

const mutations = {
  "staged consumer source": f => writeFileSync(join(f.consumer, "rpc_route_control.erl"), "changed"),
  "compiled consumer": f => writeFileSync(join(f.consumer, "ebin/rpc_route_control.beam"), "changed"),
  "compiled SDK": f => writeFileSync(join(f.sdk, "ebin/actors_pb.beam"), "changed"),
  "staged package metadata": f => writeFileSync(join(f.sdk, "src/acyclic_sdk_transport.app.src"), "changed"),
  "private dependency": f => writeFileSync(join(f.libs, "grpcbox-0.18.0/ebin/grpcbox.beam"), "changed"),
  "additional private application": f => { mkdirSync(join(f.libs, "injected/ebin"), { recursive: true }); writeFileSync(join(f.libs, "injected/ebin/code.beam"), "injected"); },
  "additional consumer code": f => writeFileSync(join(f.consumer, "injected.erl"), "injected"),
  "generation receipt bytes": f => writeFileSync(f.args.receipt, readFileSync(f.args.receipt, "utf8") + "\n"),
  "upstream generator module": f => writeFileSync(join(f.args["tool-home"], "tool.beam"), "changed"),
  "OTP runtime module": f => writeFileSync(join(f.args["runtime-root"], "OTP-29.1.1/bin/erl"), "changed"),
};
for (const [name, mutate] of Object.entries(mutations)) test(`reject ${name} mutation before recording installed success`, t => {
  const f = fixture(t, mutate);
  assert.throws(() => qualify(f.args, f.options), /changed|differs/);
  assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
});

test("require both loaded-module provenance checks", t => {
  const f = fixture(t), execute = f.options.command;
  f.options.command = (exe, argv, opts) => { const result = execute(exe, argv, opts); return exe.endsWith("erl") ? { ...result, stdout: result.stdout.replace("PASS installed SDK and loaded-module provenance", "missing") } : result; };
  assert.throws(() => qualify(f.args, f.options), /provenance controls incomplete/);
  assert.equal(existsSync(join(f.args.output, "qualification.json")), false);
});

test("reject output that overlaps original dependencies before executing commands", t => {
  const f = fixture(t); f.args.output = join(f.args.dependencies, "output");
  assert.throws(() => qualify(f.args, f.options), /overlaps protected/);
  assert.equal(f.calls.length, 0);
});
