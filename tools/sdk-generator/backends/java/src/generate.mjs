import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { endianness } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { runCli, generateProtocPackage } from "../../../shared/authority.mjs";

const directory = dirname(fileURLToPath(import.meta.url));
const pinned = { ...JSON.parse(readFileSync(join(directory, "../toolchains/toolchain.json"), "utf8")),
  ...JSON.parse(readFileSync(join(directory, "../../../shared/protoc.json"), "utf8")) };

// The command and toolchain injection are for offline staging controls. The CLI
// always uses spawnSync and the checked-in Maven Central tool pins.
export function generate(args, { command = spawnSync, toolchain = pinned } = {}) {
  return generateProtocPackage(args, {
    command, toolchain, directory, backend: "java", language: "Java",
    pluginArgument: "grpc-java", pluginKey: "grpc_java",
    pluginMismatch: "grpc-java executable differs from published host pin",
    bindingsDirectory: "src/main/java", authorityDirectory: "src/main/resources/META-INF",
    licenseDirectory: "src/main/resources/META-INF", extension: ".java",
    admitHost: () => {
      if (process.arch === "ppc64" && endianness() !== "LE") throw new Error("grpc-java pin requires little endian ppc64");
    },
    compilerArguments: (output, plugin) => [`--java_out=${output}`, `--plugin=protoc-gen-grpc-java=${plugin}`, `--grpc-java_out=${output}`],
    packageMetadata: () => ({ "pom.xml": readFileSync(join(directory, "../templates/package/pom.xml")) }),
  });
}

runCli(import.meta.url, generate, ["source-root", "authority", "protoc", "grpc-java", "output"]);
