import { cpSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { sha256 } from "../../../../shared/authority.mjs";
import { fixture as packageFixture } from "./package.mjs";

export function fixture(t) {
  const pkg = packageFixture(t), root = dirname(pkg.args.package), hostRoot = join(root, "host"), runtimeRoot = join(root, "runtime");
  const write = (name, bytes) => { mkdirSync(dirname(name), { recursive: true }); writeFileSync(name, bytes); };
  const hostRoots = ["include", "lib", "bin"].map(name => ({ root: join(hostRoot, name), files_sha256: {} }));
  for (const [index, name] of [[0, "header.hpp"], [1, "runtime.lib"]]) { const bytes = Buffer.from(name); write(join(hostRoots[index].root, name), bytes); hostRoots[index].files_sha256[name] = sha256(bytes); }
  const tools = {};
  for (const name of ["cl.exe", "link.exe", "lib.exe", "rc.exe", "mt.exe", "cmake.exe", "ninja.exe"]) {
    const bytes = Buffer.from("fixture " + name), path = join(hostRoots[2].root, name); write(path, bytes); tools[name] = { path, sha256: sha256(bytes) }; hostRoots[2].files_sha256[name] = sha256(bytes);
  }
  const hostBytes = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.cpp-host-inventory.v1", roots: hostRoots, tools, environment: { include: [hostRoots[0].root], lib: [hostRoots[1].root] } }));
  const runtimeFiles = { "include/runtime.hpp": sha256("runtime header") }; write(join(runtimeRoot, "include/runtime.hpp"), "runtime header");
  const runtimeBytes = Buffer.from(JSON.stringify({ schema: "acyclic.sdk.cpp-runtime-inventory.v1", files: runtimeFiles }));
  const hostIndex = join(root, "host.json"), runtimeIndex = join(root, "runtime.json"); write(hostIndex, hostBytes); write(runtimeIndex, runtimeBytes);
  const pins = { host_inventory_sha256: sha256(hostBytes), host_inventory_roots: 3, host_inventory_files: 9,
    runtime_inventory_sha256: sha256(runtimeBytes), runtime_files: 1, system_root: join(root, "windows"), versions: { cmake: "cmake fixture", ninja: "ninja fixture", cl: "fixture" } };
  const args = { ...pkg.args, "runtime-root": runtimeRoot, "runtime-inventory": runtimeIndex, "host-inventory": hostIndex, output: join(root, "qualified") };
  const calls = [], hooks = {}, negatives = { InvalidActorBytes: ["C2665", "set_code_sha256"], InvalidWorkerBytes: ["C2665", "set_javascript_module"], InvalidOptionalInteger: ["C2664", "set_if_tail"] };
  const command = (exe, argv, options) => {
    calls.push({ exe, argv, options });
    if (options.env.CL !== undefined || options.env.CXXFLAGS !== undefined || options.env.CMAKE_TOOLCHAIN_FILE !== undefined) throw new Error("ambient compiler injection");
    let phase, stdout = "", status = 0;
    if (exe === tools["cmake.exe"].path && argv[0] === "--version") { phase = "cmake-version"; stdout = "cmake fixture\n"; }
    else if (exe === tools["ninja.exe"].path) { phase = "ninja-version"; stdout = "ninja fixture\n"; }
    else if (exe === tools["cl.exe"].path) { phase = "compiler-version"; stdout = "Version fixture\n"; status = 2; }
    else if (argv[0] === "-S") {
      if (argv.filter(arg => /^-DCMAKE_(?:CXX_COMPILER|LINKER|AR|RC_COMPILER|MT|MAKE_PROGRAM|PREFIX_PATH|INSTALL_PREFIX)=/.test(arg)).some(arg => arg.includes("\\"))) throw new Error("CMake cache paths require forward slashes");
      const source = argv[1], build = argv[3], sdk = source.endsWith("sdk-source"); phase = sdk ? "configure-sdk" : "configure-consumer";
      const expected = sdk ? [...pkg.payload.keys()].filter(name => name.startsWith("src/")).map(name => join(source, name)) : ["rpc_consumer.cc", "PositiveTypes.cc", ...Object.keys(negatives).map(name => name + ".cc")].map(name => join(source, name));
      write(join(build, "compile_commands.json"), JSON.stringify(expected.map(file => ({ directory: build, file, command: "fixture compile" }))));
      const prefix = join(args.output, "runtime");
      let cache = Object.entries({ Protobuf: "protobuf", absl: "absl", gRPC: "grpc", utf8_range: "utf8_range" }).map(([name, dir]) => name + "_DIR:PATH=" + join(prefix, "lib/cmake", dir)).join("\n") + "\n";
      if (!sdk) cache += "AcyclicTransport_DIR:PATH=" + join(args.output, "sdk-install/lib/cmake/AcyclicTransport") + "\n";
      write(join(build, "CMakeCache.txt"), cache);
    } else if (argv[0] === "--install") {
      phase = "install-sdk"; write(join(args.output, "sdk-install/lib/acyclic_sdk_transport.lib"), "compiled SDK fixture");
      for (const [name, bytes] of pkg.payload) {
        const destination = name.startsWith("include/") ? "sdk-install/" + name : ["LICENSE", "NOTICE"].includes(name) || name.startsWith("authority/") ? "sdk-install/share/acyclic-sdk-transport/" + name : undefined;
        if (destination) write(join(args.output, destination), bytes);
      }
    } else if (argv[0] === "--build") {
      const target = argv.at(-1);
      if (negatives[target]) { phase = "negative-" + target; status = 1; stdout = "error " + negatives[target][0] + ": intended " + negatives[target][1] + " type mismatch\n"; }
      else if (argv[1].endsWith("consumer-build")) { phase = "build-consumer"; write(join(args.output, "consumer-build/RpcConsumer.exe"), "compiled consumer fixture"); }
      else phase = "build-sdk";
    } else if (exe.endsWith("RpcConsumer.exe")) { phase = "consumer"; stdout = "PASS C++ complete file descriptors, 25 RPC message pairs, bytes, unsigned bounds, optional zero and oneof controls\nPASS C++ 25 generated client/server loopback calls, Rust file descriptors, populated payloads and actual method paths/stream types\n"; }
    else throw new Error("unexpected command");
    const result = { status, stdout, stderr: "" }; hooks[phase]?.(result, options); return result;
  };
  return { args, pins, pkg, root, runtimeRoot, hostRoots, hostBytes, runtimeBytes, tools, hooks, calls, options: { command, toolchain: pins } };
}
