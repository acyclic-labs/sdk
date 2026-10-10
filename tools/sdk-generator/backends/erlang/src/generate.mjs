import { spawnSync } from "node:child_process";
import { lstatSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, posix, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { loadAuthority, sha256, within } from "../../../shared/authority.mjs";
import { verifyRuntime, verifyTools } from "./inventory.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
export const pins = JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json")));
export const producerSources = ["generate.mjs", "inventory.mjs", "../../../shared/authority.mjs",
  "../toolchains/toolchain.json", "../toolchains/generator-files.json", "../templates/generation/generate.escript"];
export const packageTemplates = new Map([
  ["rebar.config", "rebar.config"], ["README.md", "README.md"],
  ["src/acyclic_sdk_transport.app.src", "acyclic_sdk_transport.app.src"],
]);

function files(root, prefix = "") {
  return readdirSync(join(root, prefix), { withFileTypes: true }).flatMap(entry => {
    const name = posix.join(prefix, entry.name);
    if (entry.isSymbolicLink()) throw new Error("generated output contains a link");
    if (entry.isDirectory()) return files(root, name);
    if (!entry.isFile()) throw new Error("generated output is not a regular file");
    return [name];
  }).sort();
}

// Native command injection permits small offline staging and integrity controls.
export function generate(args, { command = spawnSync, toolchain = pins,
  toolIndexBytes = readFileSync(join(directory, "../toolchains/generator-files.json")) } = {}) {
  const source = realpathSync(args["source-root"]), authorityRoot = realpathSync(args.authority);
  const runtime = realpathSync(args["runtime-root"]), runtimeIndex = realpathSync(args["runtime-inventory"]);
  const tools = realpathSync(args["tool-home"]);
  const output = join(realpathSync(dirname(resolve(args.output))), basename(resolve(args.output)));
  try { lstatSync(output); throw new Error("output must be absent"); }
  catch (error) { if (error.code !== "ENOENT") throw error; }
  for (const protectedInput of [source, authorityRoot, runtime, runtimeIndex, tools, dirname(directory)]) {
    if (within(protectedInput, output) || within(output, protectedInput)) throw new Error("output overlaps protected input");
  }
  const pin = toolchain.runtime?.[`${process.platform}-${process.arch}`];
  if (!pin) throw new Error("generation runtime host is not admitted");
  const authority = loadAuthority(authorityRoot);
  if (authority.manifest.families.some(family => !authority.descriptors.has(family.source))) throw new Error("every generated family requires Rust descriptors");
  const runtimeBytes = readFileSync(runtimeIndex);
  const sourceBytes = new Map(producerSources.map(name => [name, readFileSync(join(directory, name))]));
  const templates = new Map([...packageTemplates].map(([name, template]) => [name, readFileSync(join(directory, "../templates/package", template))]));
  const metadata = new Map(["LICENSE", "NOTICE"].map(name => [name, readFileSync(join(source, name))]));
  const guard = () => {
    if (!readFileSync(runtimeIndex).equals(runtimeBytes)) throw new Error("runtime inventory changed");
    verifyRuntime(runtime, runtimeBytes, pin); verifyTools(tools, toolIndexBytes, toolchain);
    for (const [name, bytes] of sourceBytes) if (!readFileSync(join(directory, name)).equals(bytes)) throw new Error("producer source changed");
    for (const [name, template] of packageTemplates) if (!readFileSync(join(directory, "../templates/package", template)).equals(templates.get(name))) throw new Error("package template changed");
    for (const [name, bytes] of metadata) if (!readFileSync(join(source, name)).equals(bytes)) throw new Error("package metadata changed");
    const current = loadAuthority(authorityRoot);
    if (!current.bytes.equals(authority.bytes)) throw new Error("authority manifest changed");
  };
  guard();
  const temporary = mkdtempSync(join(tmpdir(), "sdk-erlang-"));
  try {
    const home = join(temporary, "home"), project = join(temporary, "project");
    for (const target of [home, join(project, "proto"), join(project, "src")]) mkdirSync(target, { recursive: true });
    const names = new Set();
    for (const family of authority.manifest.families) {
      const name = basename(family.source);
      if (names.has(name.toLowerCase())) throw new Error("canonical schema basenames collide");
      names.add(name.toLowerCase());
      writeFileSync(join(project, "proto", name), authority.inputs.get(family.source), { flag: "wx" });
    }
    const driver = join(temporary, "generate.escript");
    writeFileSync(driver, sourceBytes.get("../templates/generation/generate.escript"), { flag: "wx" });
    const env = { HOME: home, PATH: [join(runtime, pin.roots[0], "bin"), "/usr/bin", "/bin"].join(":"),
      ERL_FLAGS: "+S 1:1 +fnu", LC_ALL: "C.UTF-8", REBAR_COLOR: "none" };
    const run = (executable, argv, cwd) => {
      const result = command(executable, argv, { cwd, env, encoding: "utf8", timeout: 180_000, maxBuffer: 8 * 1024 * 1024 });
      if (result.error || result.status !== 0) throw new Error("Erlang generation command failed: " + (result.stderr ?? ""));
      return result.stdout ?? "";
    };
    const otpRelease = run(join(runtime, pin.roots[0], "bin/erl"), ["-noshell", "-noinput", "-eval",
      'io:format("~s~n",[erlang:system_info(otp_release)]),halt(0).'], temporary).trim();
    if (otpRelease !== pin.otp_release) throw new Error("OTP release differs");
    const log = run(join(runtime, pin.roots[0], "bin/escript"), [driver, tools, project], project);
    if (!log.split(/\r?\n/).includes("PASS pinned GPB/grpcbox generator")) throw new Error("generation completion marker missing");
    const generated = files(join(project, "src"));
    if (generated.length !== authority.manifest.families.length * 3 || generated.some(name => name.includes("/") || !name.endsWith(".erl"))
      || new Set(generated.map(name => name.toLowerCase())).size !== generated.length
      || [...names].some(name => !generated.includes(name.slice(0, -6) + "_pb.erl"))
      || generated.filter(name => name.endsWith("_service_client.erl")).length !== names.size
      || generated.filter(name => name.endsWith("_service_bhvr.erl")).length !== names.size) throw new Error("incomplete or unexpected generated module inventory");
    guard();
    mkdirSync(output);
    const put = (name, bytes) => { const target = join(output, name); mkdirSync(dirname(target), { recursive: true }); writeFileSync(target, bytes, { flag: "wx" }); };
    for (const name of generated) put("src/" + name, readFileSync(join(project, "src", name)));
    for (const [name, bytes] of [...templates, ...metadata]) put(name, bytes);
    put("authority/rust-authority.json", authority.bytes);
    for (const [name, bytes] of authority.inputs) put("authority/" + name, bytes);
    guard();
    const outputs = files(output);
    const receipt = { schema: "acyclic.sdk.erlang-producer-receipt.v1", authority: "rust", target: "erlang",
      source_revision: authority.manifest.source_revision, authority_manifest_sha256: sha256(authority.bytes),
      input_sha256: Object.fromEntries([...authority.inputs].map(([name, bytes]) => [name, sha256(bytes)])),
      otp_version: pin.otp_version, runtime_inventory_sha256: sha256(runtimeBytes),
      gpb_version: toolchain.gpb_version, grpcbox_plugin_version: toolchain.grpcbox_plugin_version, rebar_version: toolchain.rebar_version,
      generator_inventory_sha256: sha256(toolIndexBytes), toolchain_sha256: sha256(JSON.stringify(toolchain)),
      source_sha256: Object.fromEntries([...sourceBytes].map(([name, bytes]) => [name, sha256(bytes)])),
      template_sha256: Object.fromEntries([...templates].map(([name, bytes]) => [name, sha256(bytes)])),
      generation_log_sha256: sha256(log), node_version: process.version,
      outputs, output_sha256: Object.fromEntries(outputs.map(name => [name, sha256(readFileSync(join(output, name)))])) };
    put("generation-receipt.json", Buffer.from(JSON.stringify(receipt, null, 2) + "\n"));
    return receipt;
  } finally {
    if (realpathSync(temporary) !== temporary) throw new Error("temporary directory identity changed");
    rmSync(temporary, { recursive: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const options = Object.fromEntries(["source-root", "authority", "runtime-root", "runtime-inventory", "tool-home", "output"].map(name => [name, { type: "string" }]));
  const { values } = parseArgs({ options: { ...options, help: { type: "boolean" } } });
  if (values.help) console.log("node generate.mjs " + Object.keys(options).map(name => "--" + name + " <path>").join(" "));
  else {
    if (Object.keys(options).some(name => !values[name])) throw new Error("all Erlang generation arguments are required");
    console.log(JSON.stringify(generate(values), null, 2));
  }
}
