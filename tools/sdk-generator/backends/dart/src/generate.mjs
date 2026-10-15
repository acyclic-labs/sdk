import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generateProtocPackage } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in Dart plugin build pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  return generateProtocPackage(args, {
    command, toolchain, directory, backend: "dart", language: "Dart",
    pluginArgument: "dart-plugin", pluginKey: "dart_plugin",
    pluginMismatch: "dart-plugin executable differs from admitted host pin",
    bindingsDirectory: "lib", authorityDirectory: "authority", extension: ".dart",
    compilerArguments: (output, plugin) => [`--plugin=protoc-gen-dart=${plugin}`, `--dart_out=grpc:${output}`],
    packageMetadata: () => ({ "pubspec.yaml": readFileSync(join(directory, "../templates/package/pubspec.yaml")) }),
  });
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "dart-plugin", "output"]);
