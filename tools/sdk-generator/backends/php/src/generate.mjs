import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generateProtocPackage } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in PHP plugin build pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  return generateProtocPackage(args, {
    command, toolchain, directory, backend: "php", language: "PHP",
    pluginArgument: "php-plugin", pluginKey: "php_plugin",
    pluginMismatch: "php-plugin executable differs from admitted host pin",
    bindingsDirectory: "src", authorityDirectory: "authority", extension: ".php",
    compilerArguments: (output, plugin) => [`--plugin=protoc-gen-grpc=${plugin}`, `--php_out=${output}`, `--grpc_out=${output}`],
    packageMetadata: () => ({ "composer.json": readFileSync(join(directory, "../templates/package/composer.json")) }),
  });
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "php-plugin", "output"]);
