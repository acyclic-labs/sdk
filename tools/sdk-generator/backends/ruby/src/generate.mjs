import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generateProtocPackage } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in RubyGems tool pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  return generateProtocPackage(args, {
    command, toolchain, directory, backend: "ruby", language: "Ruby",
    pluginArgument: "grpc-ruby", pluginKey: "grpc_ruby",
    pluginMismatch: "grpc-ruby executable differs from published host pin",
    bindingsDirectory: "lib", authorityDirectory: "authority", extension: ".rb",
    compilerArguments: (output, plugin) => [`--ruby_out=${output}`, `--plugin=protoc-gen-grpc-ruby=${plugin}`, `--grpc-ruby_out=${output}`],
    packageMetadata: manifest => ({ "acyclic-sdk-transport.gemspec": Buffer.from(readFileSync(join(directory, "../templates/package/acyclic-sdk-transport.gemspec"), "utf8").replace("@RUST_SOURCE_REVISION@", manifest.source_revision)) }),
  });
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "grpc-ruby", "output"]);
