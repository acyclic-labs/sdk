import { execFileSync } from "node:child_process";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { nativeFamily } from "./native-family.mjs";
import { createNativeProducer } from "./build-native-family.mjs";
const producer = createNativeProducer(nativeFamily("stream"));
export const { sourceSnapshot, assertSourceSnapshot, capturedCompilerIdentity, ensureCargoTargetDirectory, deterministicRustflags, darwinAppleLdPaths, configureDarwinAppleLd, withDeterministicRustflags, linkerInputs, darwinRustObjcopyIdentity, createRustcInvocationCapture, napiGeneratorIdentity, normalizeBuildInputs, buildInputsReceipt, assertBuildInputs, assertMatchingBuildInputs, rustMetadata, assertExactInventory, assertOwnedDirectory, signDarwinAddon, assertBundle, prepareBuildOutput, publishBundle } = producer;
if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === "build") execFileSync(process.execPath, [fileURLToPath(new URL("./generate.mjs", import.meta.url)), "stream"], { stdio: "inherit" });
  await producer.main();
}
