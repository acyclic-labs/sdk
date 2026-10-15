import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generateProtocPackage } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in NuGet package tool pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  return generateProtocPackage(args, {
    command, toolchain, directory, backend: "dotnet", language: "C#",
    pluginArgument: "grpc-csharp", pluginKey: "grpc_csharp",
    pluginMismatch: "grpc-csharp executable differs from published package host pin",
    bindingsDirectory: "generated", authorityDirectory: "", extension: ".cs",
    compilerArguments: (output, plugin) => [`--csharp_out=${output}`, `--plugin=protoc-gen-grpc=${plugin}`, `--grpc_out=${output}`],
    packageMetadata: () => Object.fromEntries(["Acyclic.Sdk.Transport.csproj", "packages.lock.json", "global.json", "README.md"].map(name => [name, readFileSync(join(directory, "../templates/package", name))])),
  });
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "grpc-csharp", "output"]);
