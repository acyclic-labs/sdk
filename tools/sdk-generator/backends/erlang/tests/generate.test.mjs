import assert from "node:assert/strict";
import { existsSync, mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { sha256 } from "../../../shared/authority.mjs";
import { generate, pins, producerSources } from "../src/generate.mjs";

function fixture(t, fault) {
  const root = mkdtempSync(join(tmpdir(), "sdk-erlang-producer-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const put = (name, bytes) => { const p = join(root, name); mkdirSync(join(p, ".."), { recursive: true }); writeFileSync(p, bytes); return p; };
  put("source/LICENSE", "license"); put("source/NOTICE", "notice");
  const families = ["actors", "workers", "stream"].map(name => {
    const source = `${name}/v1/${name}.proto`, descriptor = source + ".bin";
    put("authority/" + source, "canonical " + name); put("authority/" + descriptor, "rust descriptor " + name);
    return { source, descriptor, source_sha256: sha256("canonical " + name), descriptor_sha256: sha256("rust descriptor " + name) };
  });
  put("authority/rust-authority.json", JSON.stringify({ schema: "acyclic.sdk.rust-authority.v1", authority: "rust", source_revision: "a".repeat(40), families }));
  put("runtime/OTP-29.1.1/bin/erl", "admitted runtime");
  const runtime = { schema: "acyclic.sdk.erlang-runtime-inventory.v1", otp_version: "29.1.1", otp_archive_sha256: "a".repeat(64),
    files_sha256: { "OTP-29.1.1/bin/erl": sha256("admitted runtime") }, links: {} };
  const runtimeBytes = Buffer.from(JSON.stringify(runtime)); put("runtime-index.json", runtimeBytes);
  put("tools/module.beam", "admitted tool");
  const toolIndexBytes = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.erlang-generation-tools.v1", gpb_version: pins.gpb_version,
    grpcbox_plugin_version: pins.grpcbox_plugin_version, rebar_version: pins.rebar_version, files_sha256: { "module.beam": sha256("admitted tool") } }));
  const toolchain = { ...pins, generator_inventory_sha256: sha256(toolIndexBytes), generator_files: 1,
    runtime: { [`${process.platform}-${process.arch}`]: { ...pins.runtime["linux-x64"], inventory_sha256: sha256(runtimeBytes), otp_archive_sha256: runtime.otp_archive_sha256, files: 1, links: 0 } } };
  const args = { "source-root": join(root, "source"), authority: join(root, "authority"), "runtime-root": join(root, "runtime"),
    "runtime-inventory": join(root, "runtime-index.json"), "tool-home": join(root, "tools"), output: join(root, "output") };
  const calls = [];
  const command = (exe, argv, opts) => {
    calls.push({ exe, argv, opts });
    if (!exe.endsWith("escript")) return { status: 0, stdout: "29\n" };
    const project = argv[2];
    for (const name of ["actors", "workers", "stream"]) {
      assert.equal(readFileSync(join(project, "proto", name + ".proto"), "utf8"), "canonical " + name);
      for (const file of [name + "_pb.erl", `acyclic_${name}_v_1_${name}_service_client.erl`, `acyclic_${name}_v_1_${name}_service_bhvr.erl`]) writeFileSync(join(project, "src", file), "generated " + file);
    }
    fault?.({ root, project, args });
    return { status: 0, stdout: "PASS pinned GPB/grpcbox generator\n" };
  };
  return { root, args, calls, options: { command, toolchain, toolIndexBytes } };
}

test("stage canonical inputs privately and preserve all original descriptor bytes", t => {
  const f = fixture(t), receipt = generate(f.args, f.options);
  assert.equal(f.calls.length, 2);
  assert.equal(f.calls[1].opts.env.HOME.includes("sdk-erlang-"), true);
  assert.equal(f.calls[1].opts.env.ERL_LIBS, undefined);
  assert.equal(readFileSync(join(f.args.output, "authority/workers/v1/workers.proto.bin"), "utf8"), "rust descriptor workers");
  assert.equal(receipt.output_sha256["authority/workers/v1/workers.proto.bin"], receipt.input_sha256["workers/v1/workers.proto.bin"]);
  assert.equal(Object.keys(receipt.source_sha256).length, producerSources.length);
  assert.equal(receipt.outputs.filter(name => name.endsWith(".erl")).length, 9);
  assert.equal(existsSync(f.calls[1].argv[2]), false);
});

for (const kind of ["tool", "runtime", "authority"]) test(`reject ${kind} mutation during native generation before publishing output`, t => {
  const f = fixture(t, ({ root }) => writeFileSync(join(root, kind === "tool" ? "tools/module.beam" : kind === "runtime" ? "runtime/OTP-29.1.1/bin/erl" : "authority/actors/v1/actors.proto"), "mutated"));
  assert.throws(() => generate(f.args, f.options), /differs|mismatch/);
  assert.equal(existsSync(f.args.output), false);
});

test("reject unexpected generated executable files", t => {
  const f = fixture(t, ({ project }) => writeFileSync(join(project, "src/injected.erl"), "injected"));
  assert.throws(() => generate(f.args, f.options), /module inventory/);
  assert.equal(existsSync(f.args.output), false);
});

test("reject output inside an admitted tool tree before any command runs", t => {
  const f = fixture(t); f.args.output = join(f.args["tool-home"], "output");
  assert.throws(() => generate(f.args, f.options), /overlaps protected/);
  assert.equal(f.calls.length, 0);
});
