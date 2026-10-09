import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, delimiter, dirname, join, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadAuthority, sha256, within } from "../../../shared/authority.mjs";
import { verifyRuntime } from "./runtime.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };
function inventory(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("generated output contains a link");
    if (entry.isDirectory()) return inventory(root, name);
    if (!entry.isFile()) throw new Error("generated output is not a regular file");
    return [name];
  }).sort();
}

// Command/toolchain injection supports tiny offline fixtures. CLI pins are fixed.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  const source = realpathSync(args["source-root"]), authority = realpathSync(args.authority);
  const protoc = realpathSync(args.protoc), plugin = realpathSync(args["elixir-plugin"]);
  const runtime = realpathSync(args["runtime-root"]), runtimeIndex = realpathSync(args["runtime-inventory"]);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const input of [source, authority, protoc, plugin, runtime, runtimeIndex, dirname(directory)]) {
    if (within(input, output) || within(output, input)) throw new Error("output overlaps protected input");
  }
  const { bytes: manifestBytes, manifest, inputs } = loadAuthority(authority);
  const inputHashes = Object.fromEntries([...inputs].map(([name, bytes]) => [name, sha256(bytes)]));
  const metadata = new Map(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  for (const name of ["mix.exs", "README.md"]) metadata.set(name, readFileSync(join(directory, "../templates/package", name)));
  const sourceNames = ["generate.mjs", "runtime.mjs", "../../../shared/authority.mjs", "../../../shared/protoc.json", "../toolchains/toolchain.json"];
  const sourceBytes = new Map(sourceNames.map(name => [name, readFileSync(join(directory, name))]));
  const host = `${process.platform}-${process.arch}`, runtimePin = toolchain.runtime?.[host];
  const compilerHash = sha256(readFileSync(protoc)), pluginHash = sha256(readFileSync(plugin));
  if (compilerHash !== toolchain.protoc?.[host]?.sha256) throw new Error("compiler executable differs from pin");
  if (pluginHash !== toolchain.elixir_plugin?.[host]?.sha256) throw new Error("Elixir plugin differs from pin");
  if (!runtimePin) throw new Error("runtime host is not admitted");
  const indexBytes = readFileSync(runtimeIndex);
  verifyRuntime(runtime, indexBytes, runtimePin);
  const temporary = mkdtempSync(join(tmpdir(), "sdk-elixir-descriptors-"));
  try {
    const home = join(temporary, "home"); mkdirSync(home);
    const env = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
      !/^(?:ERL_|ELIXIR_|ESCRIPT_|MIX_|HEX_|GIT_|LD_|DYLD_|XDG_|HOME$|PATH$|ROOTDIR$|BINDIR$|EMU$|PROGNAME$)/.test(key)));
    Object.assign(env, { HOME: home, PATH: [join(runtime, runtimePin.roots[1], "bin"), join(runtime, runtimePin.roots[0], "bin"), "/usr/bin", "/bin"].join(":"),
      ERL_FLAGS: "+S 1:1", ELIXIR_ERL_OPTIONS: "+fnu", LC_ALL: "C.UTF-8" });
    const run = (exe, argv) => {
      const result = command(exe, argv, { cwd: temporary, env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
      if (result.error || result.status !== 0) throw new Error(`Elixir generation command failed: ${result.stderr ?? ""}`);
      return result.stdout.trim();
    };
    if (run(protoc, ["--version"]) !== toolchain.protoc_version || run(plugin, ["--version"]) !== toolchain.protobuf_version) throw new Error("generation tool version differs");
    const otp = join(runtime, runtimePin.roots[0], "bin/erl");
    if (run(otp, ["-noshell", "-eval", 'io:format("~s~n", [erlang:system_info(otp_release)]), halt().']) !== runtimePin.otp_release) throw new Error("OTP release differs");
    const descriptors = new Map();
    for (const family of manifest.families) if (family.descriptor) descriptors.set(family.descriptor_sha256, inputs.get(family.descriptor));
    const paths = [...descriptors].sort(([a], [b]) => a.localeCompare(b)).map(([digest, bytes]) => {
      const target = join(temporary, digest + ".bin"); writeFileSync(target, bytes, { flag: "wx" }); return target;
    });
    const bindings = join(temporary, "bindings"); mkdirSync(bindings);
    const names = manifest.families.map(family => family.source).sort();
    run(protoc, [`--descriptor_set_in=${paths.join(delimiter)}`, `--plugin=protoc-gen-elixir=${plugin}`,
      `--elixir_out=plugins=grpc,gen_descriptors=true,gen_proto_source=true:${bindings}`, ...names]);
    const generated = inventory(bindings);
    const expected = names.map(name => name.slice(0, -6) + ".pb.ex");
    if (generated.length !== expected.length || generated.some(name => !name.endsWith(".pb.ex"))
      || new Set(generated.map(name => name.toLowerCase())).size !== generated.length
      || expected.some(name => generated.filter(file => file === name || file.endsWith("/" + name)).length !== 1)) throw new Error("incomplete or unexpected generated Elixir inventory");
    mkdirSync(output);
    const put = (name, bytes) => { const target = join(output, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: "wx" }); };
    for (const name of generated) put("lib/" + name, readFileSync(join(bindings, name)));
    for (const [name, bytes] of metadata) put(name, bytes);
    put("authority/rust-authority.json", manifestBytes);
    for (const [name, bytes] of inputs) put("authority/" + name, bytes);
    if (sha256(readFileSync(protoc)) !== compilerHash || sha256(readFileSync(plugin)) !== pluginHash || !readFileSync(runtimeIndex).equals(indexBytes)) throw new Error("generation tool inputs changed");
    for (const [name, bytes] of sourceBytes) if (!readFileSync(join(directory, name)).equals(bytes)) throw new Error("producer source changed");
    for (const name of ["mix.exs", "README.md"]) if (!readFileSync(join(directory, "../templates/package", name)).equals(metadata.get(name))) throw new Error("package template changed");
    const outputs = inventory(output);
    const receipt = { schema: "acyclic.sdk.elixir-producer-receipt.v1", authority: "rust", target: "elixir",
      source_revision: manifest.source_revision, authority_manifest_sha256: sha256(manifestBytes), input_sha256: inputHashes,
      protoc_version: toolchain.protoc_version, protobuf_version: toolchain.protobuf_version,
      tool_sha256: { protoc: compilerHash, elixir_plugin: pluginHash }, runtime_inventory_sha256: sha256(indexBytes),
      otp_version: runtimePin.otp_version, elixir_version: runtimePin.elixir_version,
      generator_sha256: sha256(sourceBytes.get("generate.mjs")), authority_reader_sha256: sha256(sourceBytes.get("../../../shared/authority.mjs")),
      source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])),
      template_sha256: Object.fromEntries(["mix.exs", "README.md"].map(name => [name, sha256(metadata.get(name))])),
      toolchain_sha256: sha256(JSON.stringify(toolchain)), node_version: process.version,
      outputs, output_sha256: Object.fromEntries(outputs.map(name => [name, sha256(readFileSync(join(output, name)))])) };
    put("generation-receipt.json", Buffer.from(JSON.stringify(receipt, null, 2) + "\n"));
    return receipt;
  } finally {
    if (realpathSync(temporary) !== temporary) throw new Error("temporary directory identity changed");
    rmSync(temporary, { recursive: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["source-root", "authority", "protoc", "elixir-plugin", "runtime-root", "runtime-inventory", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options });
  if (Object.keys(options).some(name => !values[name])) throw new Error("all Elixir generation arguments are required");
  console.log(JSON.stringify(generate(values), null, 2));
}
